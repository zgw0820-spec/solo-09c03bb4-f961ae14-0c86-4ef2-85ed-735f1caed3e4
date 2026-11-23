"""端到端自测：启动本地源站 + 代理，验证 Range 缓存行为。

用法（在已安装 fastapi/uvicorn/httpx 的环境中）：
  python tests/selftest.py

覆盖场景：
  A. 基础取段 + 二次命中（不回源取字节）
  B. 同版本并发重叠缺口：重叠只回源一次，非重叠缺口同时回源
  C. 源站换片：新版本新 ETag 独立缓存，旧缓存字节不串用
  D. 回包截断：一次性故障自动重试成功；持续故障重试耗尽后报错
  E. 换片发生在回源途中（ETag 不匹配）：报 409，重试后成功
  F. 非法 Range（多段/开区间/后缀/越界）
"""
from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]

PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def wait_health(base: str) -> None:
    async with httpx.AsyncClient(timeout=1) as client:
        for _ in range(100):
            try:
                resp = await client.get(f"{base}/healthz")
                if resp.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.2)
    raise RuntimeError(f"service not ready: {base}")


def start_server(app_dir: str, port: int, env: dict[str, str]) -> subprocess.Popen:
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(ROOT / app_dir),
        env={**os.environ, **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc


async def reset(origin: str, proxy_stats: str) -> None:
    async with httpx.AsyncClient() as client:
        await client.post(f"{origin}/admin/fault", json={"mode": "none"})
        await client.post(f"{origin}/admin/reset-stats")
        await client.post(f"{origin}/admin/swap", json={"version": "v1"})
        # 代理 stats 目前通过重启清零，这里仅读基线
        await client.get(proxy_stats)


async def main() -> int:
    origin_port = free_port()
    proxy_port = free_port()
    data_dir = tempfile.mkdtemp(prefix="origin-data-")
    cache_dir = tempfile.mkdtemp(prefix="proxy-cache-")

    origin_proc = start_server(
        "origin", origin_port, {"ORIGIN_DATA_DIR": data_dir, "ORIGIN_VIDEO_SIZE": "1048576"}
    )
    proxy_proc = start_server(
        "proxy",
        proxy_port,
        {
            "ORIGIN_BASE": f"http://127.0.0.1:{origin_port}",
            "CACHE_DIR": cache_dir,
            "FETCH_MAX_RETRIES": "3",
            "HTTP_TIMEOUT": "10",
            "CACHE_MAX_VERSIONS": "10",
        },
    )

    origin = f"http://127.0.0.1:{origin_port}"
    proxy = f"http://127.0.0.1:{proxy_port}"
    try:
        await wait_health(origin)
        await wait_health(proxy)
        print(f"origin={origin} proxy={proxy}\n")
        async with httpx.AsyncClient(timeout=15) as http:
            # ---------- A. 基础取段 + 缓存命中 ----------
            print("== A. 基础取段与缓存命中 ==")
            await reset(origin, f"{proxy}/stats")
            r = await http.get(f"{proxy}/video", headers={"Range": "bytes=1000-1999"})
            check("A1 首取返回 206 + Content-Range",
                  r.status_code == 206
                  and r.headers.get("Content-Range") == "bytes 1000-1999/1048576"
                  and len(r.content) == 1000,
                  f"status={r.status_code} cr={r.headers.get('Content-Range')}")

            direct = await http.get(f"{origin}/video", headers={"Range": "bytes=1000-1999"})
            check("A2 代理字节与源站一致", r.content == direct.content)

            pstats_before = (await http.get(f"{proxy}/stats")).json()
            ostats_before = (await http.get(f"{origin}/stats")).json()["stats"]
            r2 = await http.get(f"{proxy}/video", headers={"Range": "bytes=1000-1999"})
            pstats_after = (await http.get(f"{proxy}/stats")).json()
            ostats_after = (await http.get(f"{origin}/stats")).json()["stats"]
            check("A3 二次请求全命中（源站不再发生 GET 取字节）",
                  r2.status_code == 206
                  and ostats_after["range_requests"] == ostats_before["range_requests"]
                  and pstats_after["cache_hit"] == pstats_before["cache_hit"] + 1,
                  f"origin range delta={ostats_after['range_requests'] - ostats_before['range_requests']}")

            # 区间合并：取包含已缓存段的更大区间
            r3 = await http.get(f"{proxy}/video", headers={"Range": "bytes=0-2999"})
            ostats3 = (await http.get(f"{origin}/stats")).json()["stats"]
            check("A4 大区间只补缺失部分（源站只回 0-999 与 2000-2999 两段）",
                  r3.status_code == 206 and ostats3["range_requests"] == ostats_after["range_requests"] + 2,
                  f"origin range total={ostats3['range_requests']}")
            direct3 = await http.get(f"{origin}/video", headers={"Range": "bytes=0-2999"})
            check("A5 合并区间字节完整且与源站一致", r3.content == direct3.content)

            # ---------- B. 并发重叠缺口去重 + 非重叠并行 ----------
            print("\n== B. 并发重叠去重 / 非重叠并行 ==")
            # 重启代理拿到干净 stats 与冷缓存
            proxy_proc.terminate()
            proxy_proc.wait(timeout=10)
            shutil.rmtree(cache_dir, ignore_errors=True)
            os.makedirs(cache_dir, exist_ok=True)
            proxy_proc = start_server(
                "proxy", proxy_port,
                {"ORIGIN_BASE": f"http://127.0.0.1:{origin_port}",
                 "CACHE_DIR": cache_dir, "FETCH_MAX_RETRIES": "3",
                 "HTTP_TIMEOUT": "10", "CACHE_MAX_VERSIONS": "10"})
            await wait_health(proxy)
            await http.post(f"{origin}/admin/reset-stats")
            # 让源站每个区间请求慢一点，便于观察并发
            await http.post(f"{origin}/admin/fault", json={"mode": "delay", "delay": 0.4})

            ranges = ["bytes=4000-5999", "bytes=5000-6999", "bytes=8000-9999"]
            t0 = time.monotonic()
            responses = await asyncio.gather(
                *[http.get(f"{proxy}/video", headers={"Range": h}) for h in ranges]
            )
            elapsed = time.monotonic() - t0
            ok_status = all(resp.status_code == 206 for resp in responses)
            # 先读统计，再做直连对比，避免对比请求污染源站计数
            ostats = (await http.get(f"{origin}/stats")).json()
            # 并集 4000-6999 + 8000-9999 = 5000 字节，分 3 个互不重叠回源段
            check("B1 重叠部分只回源一次（源站共 3 次区间请求，覆盖并集 5000 字节）",
                  ok_status
                  and ostats["stats"]["range_requests"] == 3
                  and ostats["stats"]["bytes_sent"] == 5000,
                  f"range_requests={ostats['stats']['range_requests']} bytes={ostats['stats']['bytes_sent']}")
            check("B2 非重叠缺口同时回源（max_concurrent>=2，总耗时接近单次延迟）",
                  ostats["stats"]["max_concurrent_range"] >= 2 and elapsed < 0.9,
                  f"max_concurrent={ostats['stats']['max_concurrent_range']} elapsed={elapsed:.2f}s")
            for resp, h in zip(responses, ranges):
                d = await http.get(f"{origin}/video", headers={"Range": h})
                check(f"B 内容一致 {h}", resp.content == d.content and resp.status_code == 206)
            await http.post(f"{origin}/admin/fault", json={"mode": "none"})

            # ---------- C. 换片 ----------
            print("\n== C. 源站换片 ==")
            # 当前代理进程内先缓存 v1 的一个冷段，建立 v1 版本缓存
            await http.post(f"{origin}/admin/swap", json={"version": "v1"})
            await http.post(f"{origin}/admin/reset-stats")
            r_v1 = await http.get(f"{proxy}/video", headers={"Range": "bytes=60000-60999"})
            etag_v1 = (await http.get(f"{origin}/stats")).json()["etag"]
            swap = await http.post(f"{origin}/admin/swap", json={"version": "v2"})
            etag_v2 = swap.json()["etag"]
            check("C1 换片后 ETag 变化且为强 ETag",
                  r_v1.status_code == 206 and etag_v1 != etag_v2 and len(etag_v2) == 64)
            r_v2 = await http.get(f"{proxy}/video", headers={"Range": "bytes=60000-60999"})
            check("C2 新版本独立回源，响应 ETag 为新版本且返回 206",
                  r_v2.status_code == 206
                  and r_v2.headers.get("ETag", "").strip('"') == etag_v2,
                  f"status={r_v2.status_code}")
            check("C3 新旧版本同偏移内容确实不同（证明未串用旧缓存）",
                  r_v2.content != r_v1.content)
            # 换回 v1：当前进程的 v1 版本缓存仍在，应命中。
            # 基线在任何直连对比之前取
            await http.post(f"{origin}/admin/swap", json={"version": "v1"})
            ostats_c = (await http.get(f"{origin}/stats")).json()["stats"]
            r_back = await http.get(f"{proxy}/video", headers={"Range": "bytes=60000-60999"})
            ostats_c2 = (await http.get(f"{origin}/stats")).json()["stats"]
            check("C4 切回旧版本直接命中旧缓存（无新增区间回源）",
                  r_back.content == r_v1.content
                  and ostats_c2["range_requests"] == ostats_c["range_requests"],
                  f"delta={ostats_c2['range_requests'] - ostats_c['range_requests']}")
            # 直连对比放到计数检查之后，避免污染源站统计
            d_v2 = await http.get(f"{origin}/video", headers={"Range": "bytes=60000-60999"})
            # 此时源站已切回 v1，d_v2 取到的是 v1 内容，因此与 v2 代理响应不同
            check("C5 源站当前为 v1，与缓存的 v2 内容确实不同",
                  r_v2.content != d_v2.content and d_v2.content == r_back.content)
            direct_back = await http.get(f"{origin}/video", headers={"Range": "bytes=60000-60999"})
            check("C6 回切后代理字节与源站 v1 一致", r_back.content == direct_back.content)

            # ---------- D. 回包截断与重试 ----------
            print("\n== D. 回包截断与失败重试 ==")
            await http.post(f"{origin}/admin/swap", json={"version": "v1"})
            await http.post(f"{origin}/admin/reset-stats")
            # 一次性截断：代理应在缺口级别重试后成功
            await http.post(f"{origin}/admin/fault", json={"mode": "truncate_once"})
            r_t = await http.get(f"{proxy}/video", headers={"Range": "bytes=20000-20999"})
            d_t = await http.get(f"{origin}/video", headers={"Range": "bytes=20000-20999"})
            ostats_t = (await http.get(f"{origin}/stats")).json()["stats"]
            check("D1 一次性截断后自动重试成功，字节完整",
                  r_t.status_code == 206 and r_t.content == d_t.content,
                  f"status={r_t.status_code} origin_range_requests={ostats_t['range_requests']}")
            check("D2 截断请求确实发生过重试（源站区间请求 >= 2）",
                  ostats_t["range_requests"] >= 2,
                  f"range_requests={ostats_t['range_requests']}")
            # 持续截断：重试耗尽 -> 502
            await http.post(f"{origin}/admin/fault", json={"mode": "truncate"})
            r_fail = await http.get(f"{proxy}/video", headers={"Range": "bytes=30000-30999"})
            check("D3 持续截断且重试耗尽时返回 502 与明确错误信息",
                  r_fail.status_code == 502 and "截断" in r_fail.json()["message"],
                  f"status={r_fail.status_code} body={r_fail.text[:100]}")
            await http.post(f"{origin}/admin/fault", json={"mode": "none"})
            # 故障恢复后重试成功
            r_recover = await http.get(f"{proxy}/video", headers={"Range": "bytes=30000-30999"})
            d_recover = await http.get(f"{origin}/video", headers={"Range": "bytes=30000-30999"})
            check("D4 故障恢复后同区间请求成功",
                  r_recover.status_code == 206 and r_recover.content == d_recover.content)

            # ---------- E. 回源途中换片 ----------
            print("\n== E. 回源途中换片（ETag 不匹配） ==")
            await http.post(f"{origin}/admin/swap", json={"version": "v1"})
            await http.post(f"{origin}/admin/fault", json={"mode": "swap_during"})
            r_swap = await http.get(f"{proxy}/video", headers={"Range": "bytes=40000-40999"})
            check("E1 回源 ETag 与当前版本不一致时报 409（不做无意义重试）",
                  r_swap.status_code == 409 and "版本变化" in r_swap.json()["message"],
                  f"status={r_swap.status_code} body={r_swap.text[:120]}")
            # 此时源站已停在 v2；重新请求应走新版本并成功
            r_new = await http.get(f"{proxy}/video", headers={"Range": "bytes=40000-40999"})
            d_new = await http.get(f"{origin}/video", headers={"Range": "bytes=40000-40999"})
            check("E2 换片后新请求按新版本补齐并成功",
                  r_new.status_code == 206 and r_new.content == d_new.content,
                  f"status={r_new.status_code}")

            # ---------- F. 非法 Range ----------
            print("\n== F. 非法 Range 校验 ==")
            bad = [
                ("bytes=0-99,200-299", 400, "多段"),
                ("bytes=100-", 400, "开区间"),
                ("bytes=-500", 400, "后缀形式"),
                ("bytes=99-10", 400, "逆序"),
                ("bytes=999999999-1000000000", 416, "越界"),
            ]
            for header, expect, label in bad:
                resp = await http.get(f"{proxy}/video", headers={"Range": header})
                check(f"F {label} {header} -> {expect}",
                      resp.status_code == expect, f"actual={resp.status_code}")
            resp = await http.get(f"{proxy}/video")
            check("F 缺失 Range 头 -> 400", resp.status_code == 400)

            pstats = (await http.get(f"{proxy}/stats")).json()
            print("\nproxy stats:", pstats)

    finally:
        for proc in (proxy_proc, origin_proc):
            proc.terminate()
            try:
                out = proc.communicate(timeout=10)[0]
            except subprocess.TimeoutExpired:
                proc.kill()
                out = proc.communicate(timeout=5)[0]
            if proc is origin_proc:
                log_path = ROOT / "tests" / "origin.log"
            else:
                log_path = ROOT / "tests" / "proxy.log"
            log_path.write_text(out or "")
        shutil.rmtree(data_dir, ignore_errors=True)
        shutil.rmtree(cache_dir, ignore_errors=True)

    failed = [name for name, ok, _ in results if not ok]
    print(f"\n共 {len(results)} 项，通过 {len(results) - len(failed)} 项，失败 {len(failed)} 项")
    if failed:
        print("失败项:", failed)
        return 1
    print("全部通过 ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
