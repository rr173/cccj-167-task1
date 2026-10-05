"""跨集群逻辑复制槽迁移控制器 (slotmigrator)。

核心思想
========
* 迁移计划 (Plan) 是一份**冻结**记录：源位点、目标订阅、表集合、模式版本
  全部在创建时确定，计划指纹随状态推进并写进切换证明，任何漂移都会被检测。
* 协调器 (Controller) 是声明式 reconcile 循环：每个阶段的动作都幂等，
  崩溃/重试只会重放已完成的步骤，不会重复推进消费水位。
* 切换由栅栏 (Fence) 保护：旧消费者 -> sealed -> 新消费者，任何时刻最多只有
  一个有效的消费水位；旧消费者迟到的确认会携带过期 epoch，被原子拒绝。
* 目标必须同时满足「位点追平」与「校验摘要一致」两道闸，才允许切换；
  切换完成后生成可独立验证的切换证明 (CutoverProof)。
"""

from .types import (
    Change,
    ChangeKind,
    ChecksumReport,
    CutoverFinal,
    GateResult,
    LSN,
    Plan,
    PlanSpec,
    PlanStatus,
    Phase,
    RowImage,
    SchemaVersion,
    Snapshot,
    TableRef,
    TableSnapshot,
    WALEntry,
)
from .digest import canonical_json, sha256_hex, lsn_str
from .store import CASStore, CASConflict, EventLog
from .fence import Fence, FenceSide, FenceStore, FenceError
from .memcluster import MemCluster, PumpFault
from .controller import Controller, GATES, GateFailure, PlanRejected
from .consumer import Consumer, ConsumerHandle
from .harness import Harness
from .proof import CutoverProof, verify_proof

__all__ = [
    "Change",
    "ChangeKind",
    "ChecksumReport",
    "CutoverFinal",
    "GateResult",
    "LSN",
    "Plan",
    "PlanSpec",
    "PlanStatus",
    "Phase",
    "RowImage",
    "SchemaVersion",
    "Snapshot",
    "TableRef",
    "TableSnapshot",
    "WALEntry",
    "canonical_json",
    "sha256_hex",
    "lsn_str",
    "CASStore",
    "CASConflict",
    "EventLog",
    "Fence",
    "FenceSide",
    "FenceStore",
    "FenceError",
    "MemCluster",
    "PumpFault",
    "Controller",
    "GATES",
    "GateFailure",
    "PlanRejected",
    "Consumer",
    "ConsumerHandle",
    "Harness",
    "CutoverProof",
    "verify_proof",
]
