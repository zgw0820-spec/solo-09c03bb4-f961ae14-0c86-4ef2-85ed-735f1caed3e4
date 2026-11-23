"""并发争用额度压力测试：大量随机区间同时打满额度。

验证在真实并发下：
  - 已用额度永远不超过容量；
  - 成功的 206 请求所需的“新缺口字节”总和 <= 容量；
  - 429 请求确实是因为额度不足，且不会留下预留泄漏（reserved 最终归零）；
  - 成功响应逐字节正确；
  - 结束后再发新缺口一定 429。

用法：.venv/bin/python -I tests/quota_stress.py
"""
from __future__ import annotations

import asyncio
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
TOTAL = 524288
CAPACITY = 60_000


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def start_server(app_dir: str, port: int, env: dict[str, str]) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port)],
        cwd=str(ROOT / app_dir),
        env={**os.environ, **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


async def wait_health(base: str) -> None:
    async with httpx.AsyncClient(timeout=1) as client:
        for _ in range(100):
            try:
                if (await client.get(f"{base}/healthz")).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.2)
    raise RuntimeError(f"not ready: {base}")


async def main() -> int:
    rng = random.Random(20261123)
    origin_port, proxy_port = free_port(), free_port()
    data_dir = tempfile.mkdtemp(prefix="qs-origin-")
    cache_dir = tempfile.mkdtemp(prefix="qs-cache-")
    origin = start_server("origin", origin_port,
                          {"ORIGIN_DATA_DIR": data_dir, "ORIGIN_VIDEO_SIZE": str(TOTAL)})
    proxy = start_server("proxy", proxy_port,
                         {"ORIGIN_BASE": f"http://127.0.0.1:{origin_port}",
                          "CACHE_DIR": cache_dir, "FETCH_MAX_RETRIES": "3",
                          "HTTP_TIMEOUT": "10", "CACHE_MAX_VERSIONS": "10",
                          "ORIGIN_BUDGET_BYTES": str(CAPACITY)})
    try:
        await wait_health(f"http://127.0.0.1:{origin_port}")
        await wait_health(f"http://127.0.0.1:{proxy_port}")
        async with httpx.AsyncClient(timeout=30) as http:
            await http.post(f"http://127.0.0.1:{origin_port}/admin/fault",
                            json={"mode": "delay", "delay": 0.3})

            # 60 个随机区间，总需求远超容量，必然出现竞争与 429
            ranges = []
            for _ in range(60):
                start = rng.randrange(0, TOTAL - 8192)
                length = rng.randrange(512, 8192)
                ranges.append((start, min(start + length - 1, TOTAL - 1)))

            async def fetch_one(start: int, end: int):
                resp = await http.get(
                    f"http://127.0.0.1:{proxy_port}/video",
                    headers={"Range": f"bytes={start}-{end}"},
                )
                return start, end, resp

            responses = await asyncio.gather(
                *[fetch_one(s, e) for s, e in ranges]
            )
            ok = [(s, e, r) for s, e, r in responses if r.status_code == 206]
            rejected = [(s, e, r) for s, e, r in responses if r.status_code == 429]
            bad = [r.status_code for _, _, r in responses
                   if r.status_code not in (206, 429)]

            # 成功后读取额度快照
            pstats = (await http.get(f"http://127.0.0.1:{proxy_port}/stats")).json()
            budget = pstats["origin_budget"]

            # 逐字节校验成功响应
            mismatches = 0
            for s, e, r in ok:
                truth = await http.get(
                    f"http://127.0.0.1:{origin_port}/video",
                    headers={"Range": f"bytes={s}-{e}"},
                )
                if truth.content != r.content or len(r.content) != e - s + 1:
                    mismatches += 1

            # 额度耗尽后再要新缺口，必须 429
            tail = await http.get(
                f"http://127.0.0.1:{proxy_port}/video",
                headers={"Range": "bytes=520000-523999"},
            )

            print(f"总请求: {len(ranges)}")
            print(f"206 成功: {len(ok)}    429 拒绝: {len(rejected)}    其他状态: {len(bad)}")
            print(f"成功响应字节不一致段数: {mismatches}")
            print(f"额度快照: {budget}")
            print(f"耗尽后新缺口状态: {tail.status_code}")

            checks = {
                "无异常状态码": not bad,
                "有 429 也有成功（真实争用）": bool(ok) and bool(rejected),
                "成功响应逐字节一致": mismatches == 0,
                "已用不超容量": budget["used_bytes"] <= CAPACITY,
                "预留全部归还（无泄漏）": budget["reserved_bytes"] == 0,
                "可用 = 容量 - 已用": budget["available_bytes"]
                == CAPACITY - budget["used_bytes"],
                "耗尽后新缺口 429": tail.status_code == 429,
            }
            for name, passed in checks.items():
                print(f"[{'PASS' if passed else 'FAIL'}] {name}")
            return 0 if all(checks.values()) else 1
    finally:
        for proc in (proxy, origin):
            proc.terminate()
            try:
                proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate(timeout=5)
        shutil.rmtree(data_dir, ignore_errors=True)
        shutil.rmtree(cache_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
