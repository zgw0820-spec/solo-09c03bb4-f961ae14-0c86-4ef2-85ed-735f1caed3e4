"""可换片的本地视频源站（仅用于本地联调/自测）。

- /video 支持单段 Range，回 206 + 强 ETag + Content-Range；
- ETag 由当前版本内容的 sha256 生成，换片后必然变化；
- /admin/swap 原子切换到另一版本（或自定义版本）；
- /admin/fault 注入故障：
    truncate        回包截断（持续，直到 fault=none）
    truncate_once   下一次区间响应截断一次
    swap_during     下一次区间响应在发送前换片（ETag 与 Content-Range 不匹配）
    delay           区间响应延迟 N 秒（默认 0.5s）
    none            清除故障
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import time
from typing import Dict, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

DATA_DIR = os.environ.get("ORIGIN_DATA_DIR", "/data")
DEFAULT_SIZE = int(os.environ.get("ORIGIN_VIDEO_SIZE", str(1024 * 1024)))

app = FastAPI(title="swappable-video-origin", version="1.0.0")

# 当前版本：版本名 -> (字节内容, 强 ETag)
_state: Dict[str, object] = {
    "version": "",
    "content": b"",
    "etag": "",
}
_state_lock = asyncio.Lock()
_stats = {
    "requests_total": 0,
    "range_requests": 0,
    "bytes_sent": 0,
    "swaps": 0,
    "max_concurrent_range": 0,
    "current_concurrent": 0,
}
_fault = {"mode": "none", "delay": 0.5}


def _make_content(version: str, size: int) -> bytes:
    """确定性生成内容：不同版本内容不同，同版本每次生成一致。"""
    seed = hashlib.sha256(f"video::{version}".encode()).digest()
    chunks = []
    block = seed
    produced = 0
    while produced < size:
        block = hashlib.sha256(block + version.encode()).digest()
        take = min(len(block), size - produced)
        chunks.append(block[:take])
        produced += take
    return b"".join(chunks)[:size]


def _register(version: str, content: bytes) -> None:
    etag = hashlib.sha256(content).hexdigest()
    _state["version"] = version
    _state["content"] = content
    _state["etag"] = etag


@app.on_event("startup")
async def _startup() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    v1_path = os.path.join(DATA_DIR, "v1.bin")
    v2_path = os.path.join(DATA_DIR, "v2.bin")
    if not os.path.exists(v1_path):
        with open(v1_path, "wb") as fh:
            fh.write(_make_content("v1", DEFAULT_SIZE))
    if not os.path.exists(v2_path):
        with open(v2_path, "wb") as fh:
            fh.write(_make_content("v2", DEFAULT_SIZE))
    _register("v1", open(v1_path, "rb").read())
    # 同时落一份 ETag 方便人工核对
    with open(os.path.join(DATA_DIR, "current.etag"), "w") as fh:
        fh.write(str(_state["etag"]))


def _current() -> Tuple[str, bytes, str]:
    return str(_state["version"]), _state["content"], str(_state["etag"])  # type: ignore[return-value]


@app.head("/video")
async def video_head() -> Response:
    """HEAD：返回强 ETag / Content-Length，供代理做版本校验。"""
    version, content, etag = _current()
    return Response(
        status_code=200,
        headers={
            "ETag": f'"{etag}"',
            "Accept-Ranges": "bytes",
            "Content-Type": "video/mp4",
            "Content-Length": str(len(content)),
        },
    )


@app.get("/video")
async def video(request: Request) -> Response:
    _stats["requests_total"] += 1
    version, content, etag = _current()
    total = len(content)
    headers = {
        "ETag": f'"{etag}"',
        "Accept-Ranges": "bytes",
        "Content-Type": "video/mp4",
    }
    range_header = request.headers.get("Range")
    if not range_header:
        return Response(content=content, status_code=200, headers=headers)

    # 仅支持 bytes=start-end
    try:
        spec = range_header.strip().removeprefix("bytes=")
        start_s, end_s = spec.split("-", 1)
        start, end = int(start_s), int(end_s)
    except (ValueError, AttributeError):
        return JSONResponse(
            status_code=400,
            content={"error": "bad_range", "message": "源站自测仅支持 bytes=start-end"},
        )
    if end < start or start >= total:
        return JSONResponse(
            status_code=416,
            content={"error": "range_not_satisfiable", "total": total},
        )
    end = min(end, total - 1)

    _stats["range_requests"] += 1
    _stats["current_concurrent"] += 1
    _stats["max_concurrent_range"] = max(
        _stats["max_concurrent_range"], _stats["current_concurrent"]
    )
    try:
        mode = _fault["mode"]
        if mode == "delay":
            await asyncio.sleep(float(_fault["delay"]))
        if mode == "swap_during":
            _fault["mode"] = "none"
            await do_swap("v2" if version == "v1" else "v1")
            # 响应头换成新 ETag，但 Content-Range/内容仍是旧版本上下文：
            # 代理必须识别出 ETag 与当前版本不一致
            headers["ETag"] = f'"{_state["etag"]}"'
        if mode == "truncate_once":
            _fault["mode"] = "none"
            payload = content[start : max(start, end - 64)]  # 少发若干字节
        elif mode == "truncate":
            payload = content[start : max(start, end - 64)]
        else:
            payload = content[start : end + 1]

        resp_headers = dict(headers)
        resp_headers["Content-Range"] = f"bytes {start}-{end}/{total}"
        resp_headers["Content-Length"] = str(len(payload))
        _stats["bytes_sent"] += len(payload)
        return Response(content=payload, status_code=206, headers=resp_headers)
    finally:
        _stats["current_concurrent"] -= 1


@app.get("/stats")
async def stats() -> JSONResponse:
    version, _, etag = _current()
    return JSONResponse(
        {
            "stats": _stats,
            "version": version,
            "etag": etag,
            "fault": dict(_fault),
        }
    )


@app.post("/admin/reset-stats")
async def reset_stats() -> JSONResponse:
    current = _stats["current_concurrent"]
    _stats.clear()
    _stats.update(
        {
            "requests_total": 0,
            "range_requests": 0,
            "bytes_sent": 0,
            "swaps": 0,
            "max_concurrent_range": 0,
            "current_concurrent": current,
        }
    )
    return JSONResponse({"ok": True})


async def do_swap(version: str) -> JSONResponse:
    async with _state_lock:
        path = os.path.join(DATA_DIR, f"{version}.bin")
        if not os.path.exists(path):
            return JSONResponse(
                status_code=404,
                content={"error": "unknown_version", "version": version},
            )
        with open(path, "rb") as fh:
            content = fh.read()
        _register(version, content)
        with open(os.path.join(DATA_DIR, "current.etag"), "w") as fh:
            fh.write(str(_state["etag"]))
        _stats["swaps"] += 1
    return JSONResponse(
        {"ok": True, "version": str(_state["version"]), "etag": str(_state["etag"])}
    )


@app.post("/admin/swap")
async def swap(request: Request) -> JSONResponse:
    body: Dict[str, str] = {}
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        pass
    version = body.get("version") or ("v2" if str(_state["version"]) == "v1" else "v1")
    return await do_swap(version)


@app.post("/admin/fault")
async def fault(request: Request) -> JSONResponse:
    body: Dict[str, str] = {}
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        pass
    mode = body.get("mode", "none")
    if mode not in {"none", "truncate", "truncate_once", "swap_during", "delay"}:
        return JSONResponse(status_code=400, content={"error": "unknown_mode"})
    _fault["mode"] = mode
    if "delay" in body:
        _fault["delay"] = float(body["delay"])  # type: ignore[arg-type]
    return JSONResponse({"ok": True, "fault": dict(_fault)})


@app.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok", "version": str(_state["version"])})
