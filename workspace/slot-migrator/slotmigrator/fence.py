"""切换栅栏：任何时刻最多只有一个有效消费水位。

栅栏状态机（针对单个源槽）::

    OPEN_OLD  --seal-->  SEALED  --open_new-->  OPEN_NEW

* ``OPEN_OLD``：只接受旧消费者（携带旧 epoch）的确认。
* ``SEALED``：**任何**消费者的确认都被拒绝（切换窗口，保证无主期可验证）。
* ``OPEN_NEW``：只接受新消费者（携带新 epoch）的确认；旧消费者迟到确认必被拒绝。

每次 ``seal`` / ``open_new`` 都让 ``revision``（CAS 版本）和 ``epoch``（fencing
token）单调递增。消费者 ack 时必须同时提交自己的 epoch，控制器/集群端比对当前
栅栏：旧 epoch 拒绝 —— 这正是“旧消费者迟到确认不得推进水位”的强制点。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum

from .store import CASStore


class FenceSide(str, Enum):
    OLD = "old"
    NEW = "new"


class FenceState(str, Enum):
    OPEN_OLD = "open_old"
    SEALED = "sealed"
    OPEN_NEW = "open_new"


class FenceError(Exception):
    """确认被栅栏拒绝（迟到消费者 / 切换窗口 / 错误槽）。"""


@dataclass
class Fence:
    key: str                         # f"{source_cluster}/{source_slot}"
    plan_id: str                     # 当前持有该槽的计划（禁止并发计划）
    state: FenceState = FenceState.OPEN_OLD
    epoch: int = 1
    old_slot: str = ""
    new_slot: str = ""
    target_cluster: str = ""
    revision: int = 0
    rejected_acks: list[dict] = field(default_factory=list)

    def required_side(self) -> FenceSide | None:
        if self.state is FenceState.OPEN_OLD:
            return FenceSide.OLD
        if self.state is FenceState.OPEN_NEW:
            return FenceSide.NEW
        return None


class FenceStore:
    """栅栏注册表；对每个槽的状态迁移做 CAS 保护。"""

    def __init__(self, store: CASStore | None = None) -> None:
        self._store = store or CASStore()
        self._lock = threading.Lock()

    @staticmethod
    def key(source_cluster: str, source_slot: str) -> str:
        return f"{source_cluster}/{source_slot}"

    def register(
        self,
        source_cluster: str,
        source_slot: str,
        plan_id: str,
        new_slot: str,
        target_cluster: str,
    ) -> Fence:
        """注册计划对源槽的所有权。同一槽只允许一个活动计划。"""
        key = self.key(source_cluster, source_slot)
        with self._lock:
            rev, cur = self._store.get(key)
            if cur is not None and cur.plan_id != plan_id and cur.state is not FenceState.OPEN_NEW:
                # 已存在尚未完成（或仍在旧侧开放）的其他计划 -> 拒绝并发计划
                raise FenceError(
                    f"slot {key} already owned by plan {cur.plan_id} ({cur.state.value})"
                )
            fence = Fence(
                key=key,
                plan_id=plan_id,
                old_slot=source_slot,
                new_slot=new_slot,
                target_cluster=target_cluster,
            )
            new_rev = self._store.put(key, fence)
            fence.revision = new_rev
            return fence

    def get(self, source_cluster: str, source_slot: str) -> Fence | None:
        _, f = self._store.get(self.key(source_cluster, source_slot))
        return f

    def seal(self, fence: Fence) -> Fence:
        """OPEN_OLD -> SEALED，epoch 递增（作废所有飞行中的旧 ack）。"""
        key = fence.key
        rev, cur = self._store.get(key)
        if cur is None or cur.plan_id != fence.plan_id:
            raise FenceError(f"fence {key} ownership lost")
        if cur.state is FenceState.SEALED:
            return cur
        if cur.state is not FenceState.OPEN_OLD:
            raise FenceError(f"cannot seal fence in state {cur.state.value}")
        cur.state = FenceState.SEALED
        cur.epoch += 1
        new_rev = self._store.cas(key, rev, cur)
        cur.revision = new_rev
        return cur

    def open_new(self, fence: Fence) -> Fence:
        """SEALED -> OPEN_NEW，epoch 再次递增（新消费者 fencing token）。"""
        key = fence.key
        rev, cur = self._store.get(key)
        if cur is None or cur.plan_id != fence.plan_id:
            raise FenceError(f"fence {key} ownership lost")
        if cur.state is FenceState.OPEN_NEW:
            return cur
        if cur.state is not FenceState.SEALED:
            raise FenceError(f"cannot open_new from state {cur.state.value}")
        cur.state = FenceState.OPEN_NEW
        cur.epoch += 1
        new_rev = self._store.cas(key, rev, cur)
        cur.revision = new_rev
        return cur

    def reopen_old(self, fence: Fence) -> Fence:
        """切换中止时回退 SEALED -> OPEN_OLD（epoch 不回收，继续单调）。"""
        key = fence.key
        rev, cur = self._store.get(key)
        if cur is None:
            raise FenceError(f"fence {key} gone")
        if cur.state is FenceState.OPEN_OLD:
            return cur
        if cur.state is not FenceState.SEALED:
            raise FenceError(f"cannot reopen_old from {cur.state.value}")
        cur.state = FenceState.OPEN_OLD
        cur.epoch += 1
        new_rev = self._store.cas(key, rev, cur)
        cur.revision = new_rev
        return cur

    def ack(
        self,
        source_cluster: str,
        source_slot: str,
        side: FenceSide,
        epoch: int,
        consumer: str,
        lsn: int,
        plan_id: str | None = None,
    ) -> None:
        """消费者推进水位的**唯一入口**。

        被拒的迟到 ack 会被记录（供对账页面展示“旧消费者迟到确认”确实被挡下），
        并抛出 :class:`FenceError` —— 调用方绝不能推进任何水位。
        """
        key = self.key(source_cluster, source_slot)
        rev, cur = self._store.get(key)
        if cur is None:
            raise FenceError(f"no fence for {key}")
        reason: str | None = None
        required = cur.required_side()
        if required is None:
            reason = "fence sealed (cutover in flight)"
        elif side is not required:
            reason = f"wrong side: {side.value}, current side is {required.value}"
        elif epoch != cur.epoch:
            reason = f"stale epoch {epoch} != current {cur.epoch} ({cur.state.value})"
        elif plan_id is not None and plan_id != cur.plan_id:
            reason = f"plan {plan_id} does not own slot (owner={cur.plan_id})"
        if reason is not None:
            cur.rejected_acks.append(
                {"consumer": consumer, "side": side.value, "epoch": epoch,
                 "current_epoch": cur.epoch, "state": cur.state.value,
                 "lsn": lsn, "reason": reason}
            )
            new_rev = self._store.cas(key, rev, cur)
            cur.revision = new_rev
            raise FenceError(f"ack rejected: {reason}")
