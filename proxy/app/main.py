"""单视频 Range 缓存代理。

规则：
- 只接受单段闭区间 Range: bytes=start-end（拒绝多段、开区间、后缀形式）；
- 每个请求先 HEAD 源站拿强 ETag 与总长度，弱 ETag 直接报错；
- 仅补齐当前 ETag 版本缺失的字节，同版本并发缺口重叠部分只回源一次；
- 所需字节齐全且来自同一版本才回 206 + Content-Range；
- 回源时发现换片（ETag 不一致）或回包截断则报错，缺口可重试。
"""
from __future__ import annotations

import logging
import os
import re
from typing import Optional, Tuple

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from .cache import CacheStore, UpstreamError, VersionChanged
from .intervals import missing

logger = logging.getLogger("proxy")

ORIGIN_BASE = os.environ.get("ORIGIN_BASE", "http://origin:8080").rstrip("/")
VIDEO_PATH = os.environ.get("ORIGIN_VIDEO_PATH", "/video")
CACHE_DIR = os.environ.get("CACHE_DIR", "/data/cache")
CACHE_MAX_VERSIONS = int(os.environ.get("CACHE_MAX_VERSIONS", "3"))
FETCH_MAX_RETRIES = int(os.environ.get("FETCH_MAX_RETRIES", "3"))
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "10"))

_RANGE_RE = re.compile(r"^bytes=(\d+)-(\d+)$")
_CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+|\*)$")

app = FastAPI(title="video-range-cache-proxy", version="1.0.0")
store = CacheStore(CACHE_DIR, max_versions=CACHE_MAX_VERSIONS)
client: Optional[httpx.AsyncClient] = None

_stats = {
    "requests_total": 0,
    "cache_hit": 0,
    "origin_head": 0,
    "origin_get": 0,
    "bytes_fetched": 0,
    "bytes_served": 0,
    "errors": 0,
}


class BadRange(ValueError):
    """Range 头不合法。"""


@app.on_event("startup")
async def _startup() -> None:
    global client
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(HTTP_TIMEOUT),
        follow_redirects=True,
        headers={"User-Agent": "range-cache-proxy/1.0"},
    )
    logger.info("proxy started, origin=%s cache=%s", ORIGIN_BASE, CACHE_DIR)


@app.on_event("shutdown")
async def _shutdown() -> None:
    if client is not None:
        await client.aclose()
    await store.aclose()


def _error(status: int, code: str, message: str) -> JSONResponse:
    _stats["errors"] += 1
    return JSONResponse(status_code=status, content={"error": code, "message": message})


def parse_range(header: str) -> Tuple[int, int]:
    """只接受单段闭区间 bytes=start-end，其余一律拒绝。"""
    match = _RANGE_RE.match(header.strip())
    if not match:
        raise BadRange(
            "仅支持单段闭区间 Range: bytes=start-end；"
            "不支持多段、开区间（bytes=start-）和后缀形式（bytes=-n）"
        )
    start, end = int(match.group(1)), int(match.group(2))
    if end < start:
        raise BadRange("Range 结束字节不能小于起始字节")
    return start, end


async def head_origin() -> Tuple[str, int]:
    """HEAD 源站，返回（强 ETag 去引号值，总长度）。"""
    assert client is not None
    _stats["origin_head"] += 1
    try:
        resp = await client.head(f"{ORIGIN_BASE}{VIDEO_PATH}")
    except httpx.HTTPError as exc:
        raise UpstreamError(f"HEAD 源站失败: {exc!r}") from exc
    if resp.status_code != 200:
        raise UpstreamError(f"源站 HEAD 状态异常: {resp.status_code}")
    etag_header = resp.headers.get("ETag", "")
    if not etag_header:
        raise UpstreamError("源站未返回 ETag，无法做版本校验")
    if etag_header.startswith(("W/", "w/")):
        raise UpstreamError("源站返回弱 ETag，代理只接受强 ETag")
    etag = etag_header.strip('"')
    length_header = resp.headers.get("Content-Length")
    if length_header is None:
        raise UpstreamError("源站未返回 Content-Length")
    return etag, int(length_header)


def make_fetcher(total: int):
    """构造绑定当前版本总长度的回源函数。"""

    async def fetch_gap(etag: str, start: int, end: int) -> bytes:
        """回源取一个闭区间缺口，逐字段校验版本与长度，任何不符即抛错。"""
        assert client is not None
        _stats["origin_get"] += 1
        url = f"{ORIGIN_BASE}{VIDEO_PATH}"
        try:
            async with client.stream(
                "GET", url, headers={"Range": f"bytes={start}-{end}"}
            ) as resp:
                if resp.status_code != 206:
                    body = await resp.aread()
                    raise UpstreamError(
                        "源站未按 206 响应区间请求，"
                        f"status={resp.status_code} body={body[:120]!r}"
                    )
                etag_header = resp.headers.get("ETag", "")
                if etag_header.startswith(("W/", "w/")) or not etag_header:
                    raise UpstreamError(f"源站 ETag 异常: {etag_header!r}")
                if etag_header.strip('"') != etag:
                    # 源站在两次请求之间换片
                    raise VersionChanged(
                        f"源站版本变化：期望 {etag!r}，实际 {etag_header!r}"
                    )
                cr = resp.headers.get("Content-Range", "")
                match = _CONTENT_RANGE_RE.match(cr)
                if not match:
                    raise UpstreamError(f"源站 Content-Range 无法解析: {cr!r}")
                r_start, r_end, r_total = match.groups()
                if int(r_start) != start or int(r_end) != end:
                    raise UpstreamError(
                        f"源站回包区间不符：期望 {start}-{end}，实收 {r_start}-{r_end}"
                    )
                if r_total != "*" and int(r_total) != total:
                    raise VersionChanged(
                        f"源站总长度变化：期望 {total}，实际 {r_total}（疑似换片）"
                    )
                data = await resp.aread()
        except VersionChanged:
            raise
        except UpstreamError:
            raise
        except httpx.HTTPError as exc:
            # 连接中断、响应未读完等都视为回包/网络失败，交由上层重试
            raise UpstreamError(f"回源请求失败: {exc!r}") from exc

        expected = end - start + 1
        if len(data) != expected:
            raise UpstreamError(f"源站回包截断：期望 {expected} 字节，实收 {len(data)}")
        _stats["bytes_fetched"] += len(data)
        return data

    return fetch_gap


@app.get("/video")
async def video(request: Request) -> Response:
    _stats["requests_total"] += 1
    range_header = request.headers.get("Range")
    if not range_header:
        return _error(400, "range_required", "必须携带单段闭区间 Range 头")
    try:
        start, end = parse_range(range_header)
    except BadRange as exc:
        return _error(400, "bad_range", str(exc))

    try:
        etag, total = await head_origin()
        if start >= total or end >= total:
            return _error(
                416,
                "range_not_satisfiable",
                f"请求区间 {start}-{end} 超出资源总长度 {total}",
            )
        entry = await store.get(etag, total)
        if not missing(entry.covered, start, end):
            _stats["cache_hit"] += 1
        await entry.ensure(start, end, make_fetcher(total), FETCH_MAX_RETRIES)
        data = entry.read(start, end)
    except VersionChanged as exc:
        logger.warning("version changed: %s", exc)
        return _error(409, "version_changed", str(exc))
    except UpstreamError as exc:
        logger.warning("upstream error: %s", exc)
        return _error(502, "upstream_error", str(exc))

    if len(data) != end - start + 1:
        return _error(502, "truncated_cache", "缓存读取长度异常")

    _stats["bytes_served"] += len(data)
    return Response(
        content=data,
        status_code=206,
        headers={
            "Content-Type": "video/mp4",
            "Content-Length": str(len(data)),
            "Content-Range": f"bytes {start}-{end}/{total}",
            "ETag": f'"{etag}"',
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-store",
        },
    )


@app.get("/stats")
async def stats() -> JSONResponse:
    return JSONResponse(_stats)


@app.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok"})
