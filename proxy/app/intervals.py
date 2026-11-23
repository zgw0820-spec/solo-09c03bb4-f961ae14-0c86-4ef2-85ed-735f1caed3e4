"""闭区间（含端点）字节范围工具。

所有区间均为 [start, end] 的闭区间，单位为字节偏移，inclusive。
"""
from __future__ import annotations

from typing import List, Tuple

Range = Tuple[int, int]


def merge(intervals: List[Range]) -> List[Range]:
    """把若干闭区间合并成互不相交、按起点排序的列表；相邻区间也合并。"""
    if not intervals:
        return []
    ordered = sorted(intervals)
    result: List[Range] = [ordered[0]]
    for start, end in ordered[1:]:
        last_start, last_end = result[-1]
        if start <= last_end + 1:
            result[-1] = (last_start, max(last_end, end))
        else:
            result.append((start, end))
    return result


def missing(covered: List[Range], need_start: int, need_end: int) -> List[Range]:
    """返回 [need_start, need_end] 中未被 covered 覆盖的缺口（闭区间）。"""
    gaps: List[Range] = []
    cursor = need_start
    for start, end in covered:
        if end < cursor:
            continue
        if start > cursor:
            gaps.append((cursor, min(start - 1, need_end)))
        cursor = max(cursor, end + 1)
        if cursor > need_end:
            break
    if cursor <= need_end:
        gaps.append((cursor, need_end))
    return gaps


def subtract_gap(interval: Range, gap: Range) -> List[Range]:
    """从 interval 中挖掉 gap，返回剩余部分（可能 0/1/2 段）。"""
    start, end = interval
    g_start, g_end = gap
    pieces: List[Range] = []
    if start <= g_start - 1:
        pieces.append((start, min(end, g_start - 1)))
    if g_end + 1 <= end:
        pieces.append((max(start, g_end + 1), end))
    return pieces
