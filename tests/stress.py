"""并发压力自测：大量随机闭区间并发请求，逐字节校验缓存正确性。

与 selftest.py 共用启动方式，独立运行：
  python tests/stress.py
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
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]


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
    data_dir = tempfile.mkdtemp(prefix="stress-origin-")
    cache_dir = tempfile.mkdtemp(prefix="stress-cache-")
    origin = start_server("origin", origin_port,
                          {"ORIGIN_DATA_DIR": data_dir, "ORIGIN_VIDEO_SIZE": "524288"})
    proxy = start_server("proxy", proxy_port,
                         {"ORIGIN_BASE": f"http://127.0.0.1:{origin_port}",
                          "CACHE_DIR": cache_dir, "FETCH_MAX_RETRIES": "3",
                          "HTTP_TIMEOUT": "10", "CACHE_MAX_VERSIONS": "10"})
    total = 524288
    try:
        await wait_health(f"http://127.0.0.1:{origin_port}")
        await wait_health(f"http://127.0.0.1:{proxy_port}")
        async with httpx.AsyncClient(timeout=30) as http:
            # 给源站区间响应加延迟，放大并发窗口，便于验证非重叠缺口同时回源
            await http.post(f"http://127.0.0.1:{origin_port}/admin/fault",
                            json={"mode": "delay", "delay": 0.3})
            # 制造大量重叠的随机闭区间
            ranges = []
            for _ in range(40):
                start = rng.randrange(0, total - 4096)
                length = rng.randrange(128, 8192)
                ranges.append((start, min(start + length - 1, total - 1)))

            async def fetch_one(r: tuple[int, int]) -> tuple[int, int, bytes, int]:
                start, end = r
                resp = await http.get(
                    f"http://127.0.0.1:{proxy_port}/video",
                    headers={"Range": f"bytes={start}-{end}"},
                )
                assert resp.status_code == 206, (r, resp.status_code, resp.text)
                assert resp.headers["Content-Range"] == f"bytes {start}-{end}/{total}"
                return start, end, resp.content, resp.status_code

            t0 = time.monotonic()
            proxy_results = await asyncio.gather(*[fetch_one(r) for r in ranges])
            proxy_elapsed = time.monotonic() - t0

            # 直接向源站逐段取真值比对
            mismatches = 0
            for start, end, content, _ in proxy_results:
                truth = await http.get(
                    f"http://127.0.0.1:{origin_port}/video",
                    headers={"Range": f"bytes={start}-{end}"},
                )
                if truth.content != content or len(content) != end - start + 1:
                    mismatches += 1

            # 第二轮：全部应命中缓存，源站区间计数不再增加
            ostats_before = (await http.get(f"http://127.0.0.1:{origin_port}/stats")).json()["stats"]
            await asyncio.gather(*[fetch_one(r) for r in ranges])
            ostats_after = (await http.get(f"http://127.0.0.1:{origin_port}/stats")).json()["stats"]
            pstats = (await http.get(f"http://127.0.0.1:{proxy_port}/stats")).json()

            print(f"区间请求数: {len(ranges)}")
            print(f"第一轮并发耗时: {proxy_elapsed:.2f}s")
            print(f"字节不一致段数: {mismatches}")
            print(f"第二轮源站新增区间请求: "
                  f"{ostats_after['range_requests'] - ostats_before['range_requests']}")
            print(f"源站峰值并发区间数: {ostats_after['max_concurrent_range']}")
            print(f"代理统计: {pstats}")

            ok = (
                mismatches == 0
                and ostats_after["range_requests"] == ostats_before["range_requests"]
                and ostats_after["max_concurrent_range"] >= 2
            )
            print("STRESS PASS ✅" if ok else "STRESS FAIL ❌")
            return 0 if ok else 1
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
