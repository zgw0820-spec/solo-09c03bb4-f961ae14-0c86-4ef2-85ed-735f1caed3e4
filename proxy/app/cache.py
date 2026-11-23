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
from .quota import FetchQuota, QuotaExceeded, Reservation

logger = logging.getLogger(__name__)

Range = Tuple[int, int]
# 回源函数：(etag, start, end, reservation) -> 该闭区间的完整字节；失败抛异常。
# 实现必须在每次尝试结束（成功或失败）后自行结算 reservation。
Fetcher = Callable[[str, int, int, Reservation], Awaitable[bytes]]

_SAFE_ETAG = re.compile(r"[^A-Za-z0-9_.-]")


def _safe_name(etag: str) -> str:
    return _SAFE_ETAG.sub("_", etag)[:128]


class VersionCache:
    """单个 ETag 版本的缓存。"""

    def __init__(
        self,
        etag: str,
        total_length: int,
        path: str,
        quota: Optional[FetchQuota] = None,
    ) -> None:
        self.etag = etag
        self.total_length = total_length
        self.path = path
        self.quota = quota if quota is not None else FetchQuota(None)
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
        - 先把请求区间按“已覆盖 - 在途任务 - 本轮已规划段”精确扣减；
        - 剩余缺口若已有在途任务 -> 等待该任务，不重复回源；
        - 剩余缺口若已被本轮规划段覆盖 -> 合并进该段，不重复回源；
        - 否则为新缺口建立任务，互不重叠的缺口各自并行回源。

        额度规则：
        - 只对“本次新建回源任务”的缺口计费；已缓存或已在补取团队里的字节
          不再占额度；
        - 所有新缺口在规划阶段一次性预留（reserve_many），预留不足时
          整体失败并抛出 QuotaExceeded，此时**一个 GET 都还没发出**；
        - 互不重叠的新缺口在预留成功后同时起任务，可并行回源。
        """
        async with self.plan_lock:
            # 清理已完成但还没被 finally 移除的条目
            for done_gap in [g for g, f in self.in_flight.items() if f.done()]:
                self.in_flight.pop(done_gap, None)

            waits: List["asyncio.Future[None]"] = []

            def wait_for(future: "asyncio.Future[None]") -> None:
                if future not in waits:
                    waits.append(future)

            pending: List[Range] = missing(self.covered, start, end)
            # 扣掉已有在途任务覆盖的字节：这部分已在补取，不重复回源、不占额度；
            # 只有真正与本次缺口有重叠的在途任务才需要等待
            for inflight_gap, future in list(self.in_flight.items()):
                if not pending:
                    break
                if not any(
                    candidate[0] <= inflight_gap[1] and candidate[1] >= inflight_gap[0]
                    for candidate in pending
                ):
                    continue
                wait_for(future)
                pending = [
                    rest
                    for candidate in pending
                    for rest in subtract_gap(candidate, inflight_gap)
                ]
            # 扣减后的 pending 段彼此不重叠；covered 在本轮规划期间不变
            # （更新也在同一把锁内），因此它们就是本次要新建任务的新缺口
            planned = pending

            # 一次性预留所有新缺口的额度；不足则抛 QuotaExceeded，
            # 此时尚未创建任务，也就没有发出任何新 GET
            reservations: List[Reservation] = []
            if planned:
                reservations = await self.quota.reserve_many(
                    [gap_end - gap_start + 1 for gap_start, gap_end in planned]
                )

            for (gap, reservation) in zip(planned, reservations):
                future = asyncio.get_running_loop().create_future()
                self.in_flight[gap] = future
                waits.append(future)
                asyncio.create_task(
                    self._run_fetch(gap, fetcher, max_retries, future, reservation)
                )
        # 规划完成后再等待，避免持有锁期间串行化。
        # 等齐本次登记的所有任务再抛第一个异常，避免遗漏某个 future 的异常。
        first_exc: Optional[BaseException] = None
        for future in waits:
            try:
                await future
            except BaseException as exc:  # noqa: BLE001 - 汇总后统一抛出
                if first_exc is None:
                    first_exc = exc
        if first_exc is not None:
            raise first_exc

    async def _run_fetch(
        self,
        gap: Range,
        fetcher: Fetcher,
        max_retries: int,
        future: "asyncio.Future[None]",
        reservation: Reservation,
    ) -> None:
        start, end = gap
        amount = end - start + 1
        try:
            data: Optional[bytes] = None
            last_exc: Optional[BaseException] = None
            for attempt in range(1, max_retries + 1):
                if attempt > 1:
                    # 重试是对源站的一次新读取，必须重新预留额度；
                    # 预留不到时 QuotaExceeded 直接向上抛，由 HTTP 层回 429
                    reservation = await self.quota.reserve(amount)
                try:
                    data = await fetcher(self.etag, start, end, reservation)
                    break
                except VersionChanged:
                    # 源站已换片，重试没有意义，直接向上抛出
                    raise
                except QuotaExceeded:
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
            if len(data) != amount:
                raise UpstreamError(
                    f"源站回包截断：期望 {amount} 字节，实收 {len(data)}"
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

    def __init__(
        self, root: str, max_versions: int = 3, quota: Optional[FetchQuota] = None
    ) -> None:
        self.root = root
        self.max_versions = max_versions
        # 额度账本按进程共享：所有 ETag 版本共用同一份回源额度
        self.quota = quota if quota is not None else FetchQuota(None)
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
                quota=self.quota,
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
