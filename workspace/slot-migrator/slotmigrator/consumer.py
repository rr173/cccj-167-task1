"""逻辑复制槽消费者（旧：源槽；新：目标槽）。

消费者每个 tick：读取新变更 -> 执行业务处理 -> 尝试推进水位。
推进水位**必须**携带自己的 side 与当前 fencing epoch，由 :class:`FenceStore`
裁决：

* 切换窗口（SEALED）：所有 ack 被拒绝 -> 消费者阻塞，不丢事件也不推进；
* 切换后旧消费者迟到 ack：epoch 已过期 -> 被拒绝并计入 rejected_acks；
* 新消费者只在 OPEN_NEW 后以新 epoch 推进目标槽。

ack 失败的条目保留在本地缓冲区，下轮重新处理，因此“被拒绝”绝不造成丢事件。
"""
from __future__ import annotations

import dataclasses
import threading
from dataclasses import dataclass, field
from typing import Callable

from .fence import FenceError, FenceSide, FenceStore
from .memcluster import MemCluster
from .types import LSN, TableRef, WALEntry


@dataclass
class ConsumerStats:
    processed: int = 0
    acks: int = 0
    rejected_acks: int = 0
    last_ack_lsn: LSN = 0
    last_reject_reason: str | None = None


@dataclass
class ConsumerHandle:
    consumer_id: str
    side: FenceSide
    cluster_name: str
    slot: str
    stats: ConsumerStats = field(default_factory=ConsumerStats)


class Consumer:
    def __init__(
        self,
        consumer_id: str,
        side: FenceSide,
        cluster: MemCluster,
        slot: str,
        tables: list[TableRef],
        source_cluster_name: str,
        source_slot: str,
        fences: FenceStore,
        plan_id: str,
        epoch: int,
        handler: Callable[[WALEntry], None] | None = None,
        batch: int = 16,
    ) -> None:
        self.consumer_id = consumer_id
        self.side = side
        self.cluster = cluster
        self.slot = slot
        self.tables = tables
        self.source_cluster_name = source_cluster_name
        self.source_slot = source_slot
        self.fences = fences
        self.plan_id = plan_id
        self.epoch = epoch
        self.handler = handler or (lambda e: None)
        self.batch = batch
        self.stats = ConsumerStats()
        self._buffer: list[WALEntry] = []
        self._lock = threading.Lock()
        self.alive = True

    def handle(self) -> ConsumerHandle:
        return ConsumerHandle(
            consumer_id=self.consumer_id, side=self.side,
            cluster_name=self.cluster.name, slot=self.slot, stats=self.stats,
        )

    def tick(self) -> int:
        """处理一轮，返回成功 ack 的条目数。"""
        if not self.alive:
            return 0
        with self._lock:
            wm = self.cluster.slot_watermark(self.slot)
            if not self._buffer:
                self._buffer = self.cluster.read_changes(wm, self.tables, self.batch)
            if not self._buffer:
                return 0
            upto = 0
            for e in self._buffer:
                self.handler(e)
                self.stats.processed += 1
                upto = e.lsn
            # 业务处理完成，尝试推进唯一水位（栅栏裁决）
            try:
                self.fences.ack(
                    source_cluster=self.source_cluster_name,
                    source_slot=self.source_slot,
                    side=self.side,
                    epoch=self.epoch,
                    consumer=self.consumer_id,
                    lsn=upto,
                    plan_id=self.plan_id,
                )
            except FenceError as ex:
                self.stats.rejected_acks += 1
                self.stats.last_reject_reason = str(ex)
                return 0
            self.cluster.advance_slot(self.slot, upto)
            n = len(self._buffer)
            self.stats.acks += 1
            self.stats.last_ack_lsn = upto
            self._buffer = []
            return n

    def run_until(self, predicate: Callable[[ConsumerStats], bool],
                  max_ticks: int = 100_000) -> None:
        for _ in range(max_ticks):
            self.tick()
            if predicate(self.stats):
                return
        raise TimeoutError("consumer run_until 超时")  # pragma: no cover

    def stop(self) -> None:
        self.alive = False

    def describe(self) -> dict:
        return {
            "consumer_id": self.consumer_id,
            "side": self.side.value,
            "cluster": self.cluster.name,
            "slot": self.slot,
            "epoch": self.epoch,
            "alive": self.alive,
            "processed": self.stats.processed,
            "acks": self.stats.acks,
            "rejected_acks": self.stats.rejected_acks,
            "last_ack_lsn": self.stats.last_ack_lsn,
        }
