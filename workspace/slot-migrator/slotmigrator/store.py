"""存储原语：CAS 乐观并发存储与追加事件日志。

协调器的所有状态推进都走 ``compare_and_set(expected_revision)``：如果在我们
reconcile 期间有别的执行者（旧消费者、另一次重试、另一个控制器）动过状态，
CAS 失败，本轮放弃、下轮重读——天然避免“两个协调者同时切换”这类双水位问题。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterator


class CASConflict(RuntimeError):
    """状态 revision 与预期不符，调用方应放弃本轮并重读。"""


@dataclass(frozen=True)
class Event:
    seq: int
    ts: float
    kind: str
    ref: str
    detail: dict[str, Any]


class CASStore:
    """按 key 保存 ``(revision, value)`` 的线程安全 CAS 存储。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: dict[str, tuple[int, Any]] = {}

    def get(self, key: str) -> tuple[int, Any]:
        with self._lock:
            return self._data.get(key, (0, None))

    def get_or_init(self, key: str, init: Callable[[], Any]) -> tuple[int, Any]:
        with self._lock:
            if key not in self._data:
                self._data[key] = (1, init())
            return self._data[key]

    def put(self, key: str, value: Any) -> int:
        with self._lock:
            rev = self._data.get(key, (0, None))[0] + 1
            self._data[key] = (rev, value)
            return rev

    def cas(self, key: str, expected_revision: int, value: Any) -> int:
        """仅当当前 revision == expected_revision 时写入。"""
        with self._lock:
            cur = self._data.get(key, (0, None))[0]
            if cur != expected_revision:
                raise CASConflict(f"{key}: expected rev {expected_revision}, got {cur}")
            self._data[key] = (cur + 1, value)
            return cur + 1

    def keys(self) -> list[str]:
        with self._lock:
            return sorted(self._data)

    def items(self) -> list[tuple[str, int, Any]]:
        with self._lock:
            return [(k, rev, v) for k, (rev, v) in sorted(self._data.items())]


class EventLog:
    """仅追加的审计事件流（对账页面与切换证明的事实来源之一）。"""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._lock = threading.Lock()
        self._events: list[Event] = []
        self._clock = clock

    def append(self, kind: str, ref: str, **detail: Any) -> Event:
        with self._lock:
            ev = Event(seq=len(self._events) + 1, ts=self._clock(),
                       kind=kind, ref=ref, detail=dict(detail))
            self._events.append(ev)
            return ev

    def since(self, seq: int = 0) -> list[Event]:
        with self._lock:
            return [e for e in self._events if e.seq > seq]

    def all(self) -> list[Event]:
        with self._lock:
            return list(self._events)

    def __iter__(self) -> Iterator[Event]:  # pragma: no cover
        return iter(self.all())
