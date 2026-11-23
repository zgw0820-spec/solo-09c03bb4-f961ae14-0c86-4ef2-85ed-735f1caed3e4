"""按强 ETag 分版本的字节缓存。

每个 ETag 版本对应一个稀疏缓存文件和一份"已覆盖闭区间"列表：
- 同一版本的并发请求按缺口（gap）去重：重叠部分共享同一个回源任务，
  互不重叠的缺口各自建立独立任务，可同时回源；
- 缺口补齐后合并进已覆盖区间，后续相同字节直接命中缓存；
- 换片后源站 ETag 变化，会得到一个全新的 VersionCache，旧版本字节不会串用。
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
from collections import OrderedDict
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from .intervals import merge, missing, subtract_gap

logger = logging.getLogger(__name__)

Range = Tuple[int, int]
# 回源函数：(etag, start, end) -> 该闭区间的完整字节；失败抛异常
Fetcher = Callable[[str, int, int], Awaitable[bytes]]

_SAFE_ETAG = re.compile(r"[^A-Za-z0-9_.-]")


def _safe_name(etag: str) -> str:
    return _SAFE_ETAG.sub("_", etag)[:128]


class VersionCache:
    """单个 ETag 版本的缓存。"""

    def __init__(self, etag: str, total_length: int, path: str) -> None:
        self.etag = etag
        self.total_length = total_length
        self.path = path
        # 稀疏文件，按 total_length 预占大小；实际磁盘块随写入增长
        self.fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        os.ftruncate(self.fd, total_length)
        self.covered: List[Range] = []
        # 规划锁：计算缺口、登记/复用 in-flight 任务、更新 covered 时持有，
        # 保证去重与状态切换原子性
        self.plan_lock = asyncio.Lock()
        self.in_flight: Dict[Range, "asyncio.Future[None]"] = {}

    async def ensure(
        self, start: int, end: int, fetcher: Fetcher, max_retries: int
    ) -> None:
        """保证 [start, end] 在本版本缓存中齐全，必要时回源补齐。

        去重规则（全程持有 plan_lock 做原子规划）：
        - 缺口已被某个运行中的任务完全覆盖 -> 直接等待该任务；
        - 与运行中的任务部分重叠 -> 重叠段等待原任务，剩余段另起任务，
          因此重叠字节只会回源一次，互不重叠的缺口可同时回源；
        - 没有任何运行中任务覆盖 -> 新建回源任务。
        """
        tasks: List["asyncio.Future[None]"] = []
        async with self.plan_lock:
            # 清理已完成但还没被 finally 移除的条目
            for done_gap in [g for g, f in self.in_flight.items() if f.done()]:
                self.in_flight.pop(done_gap, None)
            gaps = missing(self.covered, start, end)
            for gap in gaps:
                # 规划期间其他分片可能已完成并更新 covered：
                # 每个缺口处理前都按最新 covered 重新裁剪，防止等待不再写入的分片
                gap = self._trim_to_uncovered(gap)
                if gap is None:
                    continue
                # 任务可能在本轮规划中刚完成：剔除后用最新 covered 重算
                inflight = self._find_inflight(gap)
                if inflight is not None and inflight[1].done():
                    self.in_flight.pop(inflight[0], None)
                    inflight = None
                    gap = self._trim_to_uncovered(gap)
                    if gap is None:
                        continue
                if inflight is None:
                    future = asyncio.get_running_loop().create_future()
                    self.in_flight[gap] = future
                    tasks.append(future)
                    asyncio.create_task(
                        self._run_fetch(gap, fetcher, max_retries, future)
                    )
                    continue

                inflight_gap, reusable_future = inflight
                if inflight_gap[0] <= gap[0] and inflight_gap[1] >= gap[1]:
                    # 运行中任务完全覆盖本缺口
                    if reusable_future not in tasks:
                        tasks.append(reusable_future)
                    continue

                # 部分重叠：重叠部分等待原任务，剩余未覆盖部分新建任务
                if reusable_future not in tasks:
                    tasks.append(reusable_future)
                for piece in subtract_gap(gap, inflight_gap):
                    piece = self._trim_to_uncovered(piece)
                    if piece is None:
                        continue
                    # piece 已是减去重叠后的精确缺口：只有完全包含它的在途任务
                    # 才能满足，部分重叠不算（否则会漏掉夹缝中的缺口）
                    cover = self._covering_inflight(piece)
                    if cover is not None and not cover[1].done():
                        if cover[1] not in tasks:
                            tasks.append(cover[1])
                        continue
                    if cover is not None:
                        self.in_flight.pop(cover[0], None)
                        piece = self._trim_to_uncovered(piece)
                        if piece is None:
                            continue
                    piece_future = asyncio.get_running_loop().create_future()
                    self.in_flight[piece] = piece_future
                    tasks.append(piece_future)
                    asyncio.create_task(
                        self._run_fetch(piece, fetcher, max_retries, piece_future)
                    )
        # 规划完成后再等待，避免持有锁期间串行化
        for future in tasks:
            await future

    def _find_inflight(
        self, gap: Range
    ) -> Optional[Tuple[Range, "asyncio.Future[None]"]]:
        """返回任意与 gap 有重叠的在途任务。"""
        for inflight_gap, future in self.in_flight.items():
            g_start, g_end = gap
            i_start, i_end = inflight_gap
            if i_start <= g_end and i_end >= g_start:
                return inflight_gap, future
        return None

    def _covering_inflight(
        self, gap: Range
    ) -> Optional[Tuple[Range, "asyncio.Future[None]"]]:
        """返回完全包含 gap 的在途任务（部分重叠不算）。"""
        for inflight_gap, future in self.in_flight.items():
            if inflight_gap[0] <= gap[0] and inflight_gap[1] >= gap[1]:
                return inflight_gap, future
        return None

    def _trim_to_uncovered(self, gap: Range) -> Optional[Range]:
        """按最新 covered 裁剪，返回缺口仍未覆盖的部分。"""
        remain = missing(self.covered, gap[0], gap[1])
        if not remain:
            return None
        return remain[0]

    async def _run_fetch(
        self,
        gap: Range,
        fetcher: Fetcher,
        max_retries: int,
        future: "asyncio.Future[None]",
    ) -> None:
        start, end = gap
        try:
            data: Optional[bytes] = None
            last_exc: Optional[BaseException] = None
            for attempt in range(1, max_retries + 1):
                try:
                    data = await fetcher(self.etag, start, end)
                    break
                except VersionChanged:
                    # 源站已换片，重试没有意义，直接向上抛出
                    raise
                except Exception as exc:  # noqa: BLE001 - 回源失败统一重试
                    last_exc = exc
                    logger.warning(
                        "gap fetch failed etag=%s range=%s attempt=%d/%d: %r",
                        self.etag,
                        f"{start}-{end}",
                        attempt,
                        max_retries,
                        exc,
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(min(0.2 * attempt, 1.0))
            if data is None:
                raise UpstreamError(
                    f"回源补齐缺口 {start}-{end} 失败（已重试 {max_retries} 次）: "
                    f"{last_exc!r}"
                )
            if len(data) != end - start + 1:
                raise UpstreamError(
                    f"源站回包截断：期望 {end - start + 1} 字节，实收 {len(data)}"
                )
            # pread/pwrite 按偏移原子定位，多任务并发写互不干扰
            view = memoryview(data)
            written = 0
            while written < len(data):
                written += os.pwrite(self.fd, view[written:], start + written)
            # 与 in_flight 清理、future 完成放在同一把 plan_lock 临界区内：
            # 先登记 covered、移除条目，再唤醒等待者，
            # 保证被唤醒的请求重新规划时不会对已完成分片重复回源
            async with self.plan_lock:
                # 顺手合并其他已完成但尚未收尾的分片，避免交界处重复取段
                done_gaps = [
                    g for g, f in self.in_flight.items()
                    if f is not future and f.done() and f.exception() is None
                ]
                self.covered = merge(self.covered + [gap] + done_gaps)
                for done_gap in done_gaps:
                    self.in_flight.pop(done_gap, None)
                self.in_flight.pop(gap, None)
                if not future.done():
                    future.set_result(None)
        except BaseException as exc:
            async with self.plan_lock:
                self.in_flight.pop(gap, None)
                if not future.done():
                    future.set_exception(exc)

    def read(self, start: int, end: int) -> bytes:
        return os.pread(self.fd, end - start + 1, start)

    def close(self) -> None:
        try:
            os.close(self.fd)
        except OSError:
            pass


class CacheStore:
    """管理所有 ETag 版本的 VersionCache（简单 LRU，超容量淘汰最旧版本）。"""

    def __init__(self, root: str, max_versions: int = 3) -> None:
        self.root = root
        self.max_versions = max_versions
        os.makedirs(root, exist_ok=True)
        self._entries: "OrderedDict[str, VersionCache]" = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, etag: str, total_length: int) -> VersionCache:
        async with self._lock:
            entry = self._entries.get(etag)
            if entry is not None:
                self._entries.move_to_end(etag)
                return entry
            version_dir = os.path.join(self.root, _safe_name(etag))
            os.makedirs(version_dir, exist_ok=True)
            entry = VersionCache(
                etag=etag,
                total_length=total_length,
                path=os.path.join(version_dir, "data.bin"),
            )
            self._entries[etag] = entry
            while len(self._entries) > self.max_versions:
                old_etag, old_entry = self._entries.popitem(last=False)
                old_entry.close()
                # 同步删除磁盘版本目录，避免同名版本重建时读到残留稀疏文件
                shutil.rmtree(os.path.dirname(old_entry.path), ignore_errors=True)
                logger.info("淘汰旧版本缓存 etag=%s", old_etag)
            return entry

    async def aclose(self) -> None:
        async with self._lock:
            for entry in self._entries.values():
                entry.close()
            self._entries.clear()


class UpstreamError(RuntimeError):
    """源站不可用、回包截断、协议错误等。"""


class VersionChanged(RuntimeError):
    """回源响应的 ETag 与当前版本不一致（源站换片）。"""
