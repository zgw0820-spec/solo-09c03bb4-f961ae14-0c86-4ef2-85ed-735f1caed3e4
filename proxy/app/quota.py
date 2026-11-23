"""本次进程运行期内的回源字节额度（内存态，不落盘、不跨实例）。

额度按“预留 -> 结算”两阶段记账：

- 发起回源前，按缺口长度一次性预留（reserve）；
- 每次回源尝试结束后结算（settle）：把实际从源站读到的 206 正文字节计入
  ``used``，其余预留量归还；
- 截断、协议错误、连接中断等失败尝试同样要结算，已经读到的字节照扣；
- 重试需要重新预留；预留不到就抛 ``QuotaExceeded``，由 HTTP 层回 429。

``capacity is None`` 表示未配置额度（不限量），此时仍然统计 used/reserved，
但 ``available`` 为 ``None``。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import List, Optional


class QuotaExceeded(RuntimeError):
    """预留额度不足。携带“本次需要”和“当前可用”两个字节数，便于回包说明。"""

    def __init__(self, needed: int, available: int) -> None:
        self.needed = needed
        self.available = available
        super().__init__(
            f"回源额度不足：本次需要 {needed} 字节，当前可用 {available} 字节"
        )


@dataclass
class Reservation:
    """一次回源尝试的额度预留凭证。"""

    amount: int
    settled: bool = False


class FetchQuota:
    """固定容量的回源额度账本，仅在当前进程内有效。"""

    def __init__(self, capacity: Optional[int] = None) -> None:
        if capacity is not None and capacity < 0:
            raise ValueError("回源额度不能为负数")
        self.capacity = capacity
        self.used = 0
        self.reserved = 0
        self._lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        return self.capacity is not None

    def _available_locked(self) -> Optional[int]:
        if self.capacity is None:
            return None
        return max(0, self.capacity - self.used - self.reserved)

    async def reserve_many(self, amounts: List[int]) -> List[Reservation]:
        """一次性预留一组缺口的总额度，全成功或全失败。

        用于保证一次 /video 请求不会先发出部分 GET、再因为额度不足半途而废。
        """
        if any(amount < 0 for amount in amounts):
            raise ValueError("预留字节数不能为负数")
        total = sum(amounts)
        async with self._lock:
            if (
                self.capacity is not None
                and self.used + self.reserved + total > self.capacity
            ):
                available = self.capacity - self.used - self.reserved
                raise QuotaExceeded(total, max(0, available))
            self.reserved += total
            return [Reservation(amount) for amount in amounts]

    async def reserve(self, amount: int) -> Reservation:
        """预留单次重试所需的额度。"""
        reservations = await self.reserve_many([amount])
        return reservations[0]

    async def settle(self, reservation: Reservation, bytes_read: int) -> None:
        """结算一次回源尝试：实际读到的字节计入 used，未用部分归还。"""
        if bytes_read < 0:
            bytes_read = 0
        async with self._lock:
            if reservation.settled:
                return
            reservation.settled = True
            self.reserved -= reservation.amount
            # 正常情况下 bytes_read <= reservation.amount；源站异常多发时按实收计，
            # 保证“已用”忠实反映从源站读到的正文字节数。
            self.used += bytes_read

    async def snapshot(self) -> dict:
        """供 /stats 使用：已用、预留、可用（未配置额度时三项仍统计，可用为 null）。"""
        async with self._lock:
            return {
                "enabled": self.enabled,
                "capacity_bytes": self.capacity,
                "used_bytes": self.used,
                "reserved_bytes": self.reserved,
                "available_bytes": self._available_locked(),
            }
