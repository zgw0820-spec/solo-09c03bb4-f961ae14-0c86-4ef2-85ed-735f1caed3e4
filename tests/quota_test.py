"""回源额度专项自测：本进程运行期内有效的字节额度。

覆盖：
  G. 额度够时互不重叠的缺口并行回源，且已用只按实际读到的正文字节计
  H. 并发争用：多个请求同时抢同一份额度，成功者恰好用满、失败者 429 且未发出新 GET
  I. 已缓存 / 正在补取的字节不再占额度
  J. 截断：已读字节照扣、未用预留归还；重试必须重新预留，不足则 429
  K. 换片（409）不额外扣未读字节；强 ETag 校验仍生效

用法：.venv/bin/python -I tests/quota_test.py
"""
from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
TOTAL = 1048576
results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))


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


class Rig:
    """一组 origin + proxy 进程，支持按不同额度重启代理。"""

    def __init__(self) -> None:
        self.origin_port = free_port()
        self.proxy_port = free_port()
        self.data_dir = tempfile.mkdtemp(prefix="quota-origin-")
        self.cache_dir = tempfile.mkdtemp(prefix="quota-cache-")
        self.origin_proc: subprocess.Popen | None = None
        self.proxy_proc: subprocess.Popen | None = None

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.origin_port}"

    @property
    def proxy(self) -> str:
        return f"http://127.0.0.1:{self.proxy_port}"

    async def start_origin(self) -> None:
        self.origin_proc = start_server(
            "origin", self.origin_port,
            {"ORIGIN_DATA_DIR": self.data_dir, "ORIGIN_VIDEO_SIZE": str(TOTAL)},
        )
        await wait_health(self.origin)

    async def start_proxy(self, budget: int | None) -> None:
        if self.proxy_proc is not None:
            self.proxy_proc.terminate()
            self.proxy_proc.wait(timeout=10)
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        env = {
            "ORIGIN_BASE": self.origin,
            "CACHE_DIR": self.cache_dir,
            "FETCH_MAX_RETRIES": "3",
            "HTTP_TIMEOUT": "10",
            "CACHE_MAX_VERSIONS": "10",
        }
        if budget is not None:
            env["ORIGIN_BUDGET_BYTES"] = str(budget)
        self.proxy_proc = start_server("proxy", self.proxy_port, env)
        await wait_health(self.proxy)

    async def proxy_stats(self) -> dict:
        async with httpx.AsyncClient() as http:
            return (await http.get(f"{self.proxy}/stats")).json()

    async def origin_stats(self) -> dict:
        async with httpx.AsyncClient() as http:
            return (await http.get(f"{self.origin}/stats")).json()["stats"]

    def stop(self) -> None:
        for proc in (self.proxy_proc, self.origin_proc):
            if proc is not None:
                proc.terminate()
                try:
                    proc.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.communicate(timeout=5)
        shutil.rmtree(self.data_dir, ignore_errors=True)
        shutil.rmtree(self.cache_dir, ignore_errors=True)


async def reset_origin(http: httpx.AsyncClient, rig: Rig) -> None:
    await http.post(f"{rig.origin}/admin/fault", json={"mode": "none"})
    await http.post(f"{rig.origin}/admin/reset-stats")
    await http.post(f"{rig.origin}/admin/swap", json={"version": "v1"})


async def main() -> int:
    rig = Rig()
    try:
        await rig.start_origin()
        async with httpx.AsyncClient(timeout=30) as http:
            # ---------- G. 额度内非重叠缺口并行 + 精确记账 ----------
            print("== G. 额度内并行回源与按正文字节记账 ==")
            await rig.start_proxy(10_000)
            await reset_origin(http, rig)
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "delay", "delay": 0.4})
            ranges = ["bytes=4000-5999", "bytes=8000-8999", "bytes=9000-9999"]
            t0 = asyncio.get_running_loop().time()
            resps = await asyncio.gather(
                *[http.get(f"{rig.proxy}/video", headers={"Range": h}) for h in ranges]
            )
            elapsed = asyncio.get_running_loop().time() - t0
            ostats = await rig.origin_stats()
            pstats = await rig.proxy_stats()
            # 三段互不重叠：4000-5999（2000）、8000-8999（1000）、9000-9999（1000）
            gap_bytes = 2000 + 1000 + 1000
            check("G1 额度够时非重叠缺口并行回源（并发>=2，耗时≈单次延迟）",
                  all(r.status_code == 206 for r in resps)
                  and ostats["max_concurrent_range"] >= 2 and elapsed < 0.9,
                  f"status={[r.status_code for r in resps]} concurrency="
                  f"{ostats['max_concurrent_range']} elapsed={elapsed:.2f}s")
            check("G2 已用额度等于实际回源正文字节（并集，不重复）",
                  pstats["origin_budget"]["used_bytes"] == gap_bytes
                  and ostats["bytes_sent"] == gap_bytes,
                  f"used={pstats['origin_budget']['used_bytes']} bytes_sent={ostats['bytes_sent']}")
            check("G3 有回源结束时预留全部归还",
                  pstats["origin_budget"]["reserved_bytes"] == 0,
                  f"reserved={pstats['origin_budget']['reserved_bytes']}")
            check("G4 /stats 暴露已用/预留/可用三项",
                  pstats["origin_budget"]["available_bytes"]
                  == 10_000 - gap_bytes,
                  f"available={pstats['origin_budget']['available_bytes']}")
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "none"})

            # ---------- I. 命中 / 在途字节不再占额度 ----------
            print("\n== I. 已缓存与正在补取的字节不再占额度 ==")
            before = (await rig.proxy_stats())["origin_budget"]["used_bytes"]
            hit = await http.get(f"{rig.proxy}/video", headers={"Range": "bytes=4000-4999"})
            after = (await rig.proxy_stats())["origin_budget"]["used_bytes"]
            check("I1 全命中请求不新增已用额度",
                  hit.status_code == 206 and after == before,
                  f"used {before} -> {after}")

            # 在途：两请求同时要同一大段，只应有 1 段回源、额度只扣一次
            await rig.start_proxy(20_000)
            await reset_origin(http, rig)
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "delay", "delay": 0.5})
            pair = await asyncio.gather(
                *[http.get(f"{rig.proxy}/video", headers={"Range": "bytes=100000-109999"})
                  for _ in range(2)]
            )
            ostats = await rig.origin_stats()
            pstats = await rig.proxy_stats()
            check("I2 两个请求抢同一缺口：只回源一次、只占一次额度",
                  all(r.status_code == 206 for r in pair)
                  and ostats["range_requests"] == 1
                  and pstats["origin_budget"]["used_bytes"] == 10_000,
                  f"range_requests={ostats['range_requests']} "
                  f"used={pstats['origin_budget']['used_bytes']}")
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "none"})

            # ---------- H. 并发争用额度 ----------
            print("\n== H. 并发争用额度 ==")
            # 三个互不重叠的 4000 字节请求同时到达，总额度 10000：
            # 只能容纳两段，其余必须 429，且不得发出任何新 GET
            await rig.start_proxy(10_000)
            await reset_origin(http, rig)
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "delay", "delay": 0.4})
            contending = ["bytes=200000-203999", "bytes=300000-303999",
                          "bytes=400000-403999"]
            t0 = asyncio.get_running_loop().time()
            cresps = await asyncio.gather(
                *[http.get(f"{rig.proxy}/video", headers={"Range": h}) for h in contending]
            )
            elapsed = asyncio.get_running_loop().time() - t0
            codes = sorted(r.status_code for r in cresps)
            rejected = [r for r in cresps if r.status_code == 429]
            ostats = await rig.origin_stats()
            pstats = await rig.proxy_stats()
            check("H1 额度不足的请求返回 429，其余成功",
                  codes == [206, 206, 429],
                  f"status={[r.status_code for r in cresps]}")
            check("H2 429 回包说明所需与可用额度",
                  all("额度不足" in r.json()["message"] for r in rejected),
                  f"body={[r.json()['message'] for r in rejected]}")
            check("H3 429 请求没有发出任何新 GET（回源段数=成功请求数）",
                  ostats["range_requests"] == 2,
                  f"range_requests={ostats['range_requests']}")
            check("H4 争用下只用满额度、不多扣",
                  pstats["origin_budget"]["used_bytes"] == 8000,
                  f"used={pstats['origin_budget']['used_bytes']}")
            check("H5 并发争用没有串行化（耗时≈单次延迟）",
                  elapsed < 0.9, f"elapsed={elapsed:.2f}s")
            # 额度已用满：任何新字节都 429
            full = await http.get(f"{rig.proxy}/video", headers={"Range": "bytes=500000-503999"})
            check("H6 额度耗尽后新缺口一律 429",
                  full.status_code == 429,
                  f"status={full.status_code} used={pstats['origin_budget']['used_bytes']}")
            # 已缓存部分仍可命中，不受额度影响
            hit2 = await http.get(f"{rig.proxy}/video", headers={"Range": "bytes=200000-200999"})
            check("H7 额度耗尽不影响已缓存字节命中",
                  hit2.status_code == 206,
                  f"status={hit2.status_code}")
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "none"})

            # ---------- J. 截断：读到的字节照扣，重试再预留 ----------
            print("\n== J. 截断重试与额度 ==")
            # 每次截断少发 64 字节：1000 字节缺口实际读到 936
            await rig.start_proxy(5000)
            await reset_origin(http, rig)
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "truncate_once"})
            once = await http.get(f"{rig.proxy}/video", headers={"Range": "bytes=600000-600999"})
            d_once = await http.get(f"{rig.origin}/video", headers={"Range": "bytes=600000-600999"})
            ostats = await rig.origin_stats()
            pstats = await rig.proxy_stats()
            check("J1 截断一次后重试成功，字节完整",
                  once.status_code == 206 and once.content == d_once.content,
                  f"status={once.status_code} range_requests={ostats['range_requests']}")
            check("J2 截断读到 935 字节 + 重试读满 1000 字节，全部计入已用",
                  pstats["origin_budget"]["used_bytes"] == 1000 + (1000 - 65),
                  f"used={pstats['origin_budget']['used_bytes']} "
                  f"range_requests={ostats['range_requests']}")
            check("J3 重试结束预留归零",
                  pstats["origin_budget"]["reserved_bytes"] == 0,
                  f"reserved={pstats['origin_budget']['reserved_bytes']}")
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "none"})

            # 持续截断 + 额度只够一次半：第 1 次尝试扣 935，重试需再预留 1000，
            # 但可用只剩 565 -> 429。用于证明“重试须再预留，不够也回 429”。
            print("\n-- J4..J6 持续截断导致重试过程中额度耗尽 --")
            await rig.start_proxy(1500)
            await reset_origin(http, rig)
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "truncate"})
            fail = await http.get(f"{rig.proxy}/video", headers={"Range": "bytes=700000-700999"})
            pstats = await rig.proxy_stats()
            ostats = await rig.origin_stats()
            check("J4 重试期间额度不足时返回 429（而非 502）",
                  fail.status_code == 429,
                  f"status={fail.status_code} body={fail.text[:120]}")
            check("J5 已读的截断字节照扣（一次尝试 935 字节）",
                  pstats["origin_budget"]["used_bytes"] == 1000 - 65,
                  f"used={pstats['origin_budget']['used_bytes']}")
            check("J6 预留不足时没有再发第 2 次 GET",
                  ostats["range_requests"] == 1,
                  f"range_requests={ostats['range_requests']}")
            check("J6b 失败尝试未用额度已归还",
                  pstats["origin_budget"]["reserved_bytes"] == 0,
                  f"reserved={pstats['origin_budget']['reserved_bytes']}")
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "none"})
            await rig.start_proxy(5000)  # 新一次运行，额度重置
            await reset_origin(http, rig)
            pstats = await rig.proxy_stats()
            check("J7 额度只在单次运行内生效，重启即清零",
                  pstats["origin_budget"]["used_bytes"] == 0
                  and pstats["origin_budget"]["capacity_bytes"] == 5000,
                  f"budget={pstats['origin_budget']}")

            # ---------- K. 换片仍按强 ETag 校验 ----------
            print("\n== K. 强 ETag 校验保持 ==")
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "swap_during"})
            swap = await http.get(f"{rig.proxy}/video", headers={"Range": "bytes=800000-800999"})
            pstats = await rig.proxy_stats()
            check("K1 回源途中换片仍返回 409",
                  swap.status_code == 409 and "版本变化" in swap.json()["message"],
                  f"status={swap.status_code}")
            check("K2 换片失败不扣未读字节，且预留归还",
                  pstats["origin_budget"]["used_bytes"] == 0
                  and pstats["origin_budget"]["reserved_bytes"] == 0,
                  f"budget={pstats['origin_budget']}")
            await http.post(f"{rig.origin}/admin/fault", json={"mode": "none"})
            await http.post(f"{rig.origin}/admin/swap", json={"version": "v1"})
            ok = await http.get(f"{rig.proxy}/video", headers={"Range": "bytes=800000-800999"})
            direct = await http.get(f"{rig.origin}/video", headers={"Range": "bytes=800000-800999"})
            check("K3 故障恢复后按新版本成功且可校验 ETag",
                  ok.status_code == 206 and ok.content == direct.content
                  and ok.headers.get("ETag", "").startswith('"'),
                  f"status={ok.status_code} etag={ok.headers.get('ETag')}")
    finally:
        rig.stop()

    failed = [name for name, ok, _ in results if not ok]
    print(f"\n共 {len(results)} 项，通过 {len(results) - len(failed)} 项，失败 {len(failed)} 项")
    if failed:
        print("失败项:", failed)
        return 1
    print("额度专项全部通过 ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
