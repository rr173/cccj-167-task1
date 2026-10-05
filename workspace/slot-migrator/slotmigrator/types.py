"""领域类型：位点、WAL、快照、冻结计划、阶段与对账结果。

LSN 用普通整数表示（与 PostgreSQL 一样是单调递增的日志序号），展示时格式化为
``0/000000xx`` 风格。所有计划关键记录都是 ``frozen=True`` 的 dataclass，语义上
不可变；可变的只有 :class:`PlanStatus`，它通过 CAS 存储做乐观并发更新。
"""
from __future__ import annotations

import dataclasses
import enum
from dataclasses import dataclass, field
from typing import Any, Mapping

LSN = int
"""Log Sequence Number（逻辑日志序号），全局单调递增。"""


class ChangeKind(str, enum.Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    DDL = "ddl"


@dataclass(frozen=True)
class TableRef:
    """逻辑表坐标：schema 名 + 表名。"""

    schema: str
    table: str

    def __str__(self) -> str:  # pragma: no cover - 纯展示
        return f"{self.schema}.{self.table}"

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.table}"


@dataclass(frozen=True)
class SchemaVersion:
    """模式版本：每执行一次 DDL，版本号 +1，同时维护完整模式摘要。

    ``digest`` 对表结构（列名、列类型、主键）做规范哈希，用于跨集群一致判断。
    """

    version: int
    digest: str

    @staticmethod
    def compute(tables: Mapping[TableRef, "TableSchema"]) -> "SchemaVersion":
        from .digest import digest_tables_schema

        if tables:
            v = max(s.version for s in tables.values())
        else:
            v = 0
        return SchemaVersion(version=v, digest=digest_tables_schema(tables))


@dataclass(frozen=True)
class TableSchema:
    """表结构定义。``version`` 是集群内全局 DDL 版本号。"""

    ref: TableRef
    columns: tuple[tuple[str, str], ...]
    pk: str
    version: int

    def digest_payload(self) -> dict[str, Any]:
        return {
            "table": self.ref.fqn,
            "columns": [list(c) for c in self.columns],
            "pk": self.pk,
            "version": self.version,
        }


@dataclass(frozen=True)
class Change:
    """一行变更的逻辑镜像。

    * INSERT/UPDATE：``after`` 为新行；UPDATE 另外带 ``before``。
    * DELETE：``before`` 为被删行。
    * DDL：``ddl`` 为 DDL 描述，``schema_version`` 为新模式版本。
    """

    kind: ChangeKind
    after: tuple[Any, ...] | None = None
    before: tuple[Any, ...] | None = None
    ddl: str | None = None
    schema_version: int | None = None


@dataclass(frozen=True)
class WALEntry:
    """一条 WAL 变更记录（消费复制槽看到的最小单位）。

    ``lsn`` 是条目所在集群的本地 LSN；``origin_lsn`` 非 0 时表示该条目是从
    远端集群回放的，值为远端原始 LSN（对应 PostgreSQL 的 replication origin
    元数据）。跨集群增量摘要按 origin_lsn 对齐比较。
    """

    lsn: LSN
    ref: TableRef
    change: Change
    origin_lsn: LSN = 0

    def to_payload(self, columns: tuple[tuple[str, str], ...] | None) -> dict[str, Any]:
        """转成用于校验摘要的规范负载。列名由调用方按当时模式传入。"""
        c = self.change
        payload: dict[str, Any] = {
            "lsn": self.origin_lsn or self.lsn,
            "table": self.ref.fqn,
            "op": c.kind.value,
        }
        if c.after is not None:
            payload["after"] = _row_payload(c.after, columns)
        if c.before is not None:
            payload["before"] = _row_payload(c.before, columns)
        if c.ddl is not None:
            payload["ddl"] = c.ddl
        if c.schema_version is not None:
            payload["schema_version"] = c.schema_version
        return payload


def _row_payload(row: tuple[Any, ...], columns: tuple[tuple[str, str], ...] | None) -> dict[str, Any]:
    if columns is not None and len(columns) == len(row):
        return {name: _norm(v) for (name, _typ), v in zip(columns, row)}
    return {f"c{i}": _norm(v) for i, v in enumerate(row)}


def _norm(v: Any) -> Any:
    # dataclass 的 asdict 会处理嵌套；这里只处理基本类型
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return str(v)


@dataclass(frozen=True)
class RowImage:
    pk: str
    row: tuple[Any, ...]


@dataclass(frozen=True)
class TableSnapshot:
    """某张表在一致快照点上的全部行（按主键排序）。"""

    ref: TableRef
    columns: tuple[tuple[str, str], ...]
    rows: tuple[RowImage, ...]


@dataclass(frozen=True)
class Snapshot:
    """一次 ``export_snapshot`` 的原子产物：快照位点 + 所有冻结表的行镜像。"""

    snap_lsn: LSN
    tables: tuple[TableSnapshot, ...]
    schema_version: SchemaVersion


class Phase(str, enum.Enum):
    """迁移计划状态机（见 :data:`TRANSITIONS`）。

    PENDING -> BACKFILLING -> CATCHING_UP -> READY -> SEALING -> SWITCHED
    任意非终态检测到不可恢复分歧 -> FAILED
    """

    PENDING = "Pending"
    BACKFILLING = "Backfilling"
    CATCHING_UP = "CatchingUp"
    READY = "Ready"
    SEALING = "Sealing"
    SWITCHED = "Switched"
    FAILED = "Failed"

    @property
    def terminal(self) -> bool:
        return self in (Phase.SWITCHED, Phase.FAILED)


# 合法状态迁移；reconcile 只会沿这些边推进
TRANSITIONS: dict[Phase, frozenset[Phase]] = {
    Phase.PENDING: frozenset({Phase.BACKFILLING, Phase.FAILED}),
    Phase.BACKFILLING: frozenset({Phase.CATCHING_UP, Phase.FAILED}),
    Phase.CATCHING_UP: frozenset({Phase.READY, Phase.FAILED}),
    Phase.READY: frozenset({Phase.SEALING, Phase.FAILED}),
    Phase.SEALING: frozenset({Phase.SWITCHED, Phase.READY, Phase.FAILED}),
    Phase.SWITCHED: frozenset(),
    Phase.FAILED: frozenset({Phase.PENDING}),  # 失败计划允许在修复后重试
}


@dataclass(frozen=True)
class PlanSpec:
    """**冻结**的迁移计划规格——创建后永不改变。"""

    plan_id: str
    source_cluster: str
    source_slot: str          # 源端旧消费者复制槽
    target_cluster: str
    subscription: str         # 目标侧订阅（目标 -> 源 的复制连接）
    target_slot: str          # 切换后消费者使用的新槽
    tables: tuple[TableRef, ...]
    schema_version: SchemaVersion
    # 计划创建原子动作的产物：从该位点开始流式传输；快照与它对齐
    start_lsn: LSN
    # 计划创建时旧消费者水位，仅用于对账展示与审计
    consumer_watermark_at_plan: LSN

    def fingerprint_payload(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "source_cluster": self.source_cluster,
            "source_slot": self.source_slot,
            "target_cluster": self.target_cluster,
            "subscription": self.subscription,
            "target_slot": self.target_slot,
            "tables": sorted(t.fqn for t in self.tables),
            "schema_version": dataclasses.asdict(self.schema_version),
            "start_lsn": self.start_lsn,
            "consumer_watermark_at_plan": self.consumer_watermark_at_plan,
        }


@dataclass(frozen=True)
class ChecksumReport:
    """一次校验摘要比对结果（快照行镜像 或 增量 WAL 段）。"""

    kind: str                 # "snapshot" | "journal"
    from_lsn: LSN
    to_lsn: LSN
    source_digest: str
    target_digest: str
    match: bool
    detail: str = ""


@dataclass(frozen=True)
class GateResult:
    """切换闸门检查结果（位点对账页面的单项）。"""

    gate: str
    ok: bool
    detail: str
    observed: Any = None
    expected: Any = None


# 切换闸门标识
GATE_SCHEMA = "G1_schema_identical"
GATE_PUMP_APPLIED = "G2_target_applied"
GATE_LSN_CAUGHT_UP = "G3_lsn_caught_up"
GATE_OLD_WM_PAST_START = "G4_old_watermark_past_snapshot"
GATE_JOURNAL_DIGEST = "G5_incremental_digest"
GATE_FULL_TABLE_DIGEST = "G6_full_table_digest"


@dataclass(frozen=True)
class CutoverFinal:
    """切换瞬间冻结的最终对账事实，直接嵌入切换证明。"""

    old_watermark_src_lsn: LSN        # 旧消费者在源上的最后确认位点
    target_remote_lsn: LSN            # 目标回放源 WAL 到达的远端位点
    target_local_lsn: LSN             # 映射到目标本地 WAL 的位点（新槽起点）
    source_insert_lsn: LSN            # 源集群当前最新位点
    snapshot_digest: str
    full_table_digest: str
    journal_digest_from_start: str
    schema_version: SchemaVersion
    plan_fingerprint: str
    fence_revision_before: int
    fence_revision_after: int
    sealed_epoch: int


@dataclass
class PlanStatus:
    """可变计划状态。每次 reconcile 产出新的 revision（CAS 保护）。"""

    phase: Phase = Phase.PENDING
    revision: int = 0
    snap_lsn: LSN | None = None
    snapshot_digest: str | None = None
    full_table_digest: str | None = None
    backfill_done: bool = False
    backfill_check: ChecksumReport | None = None
    pump_started: bool = False
    last_applied_remote_lsn: LSN | None = None
    last_observed_insert_lsn: LSN | None = None
    # “连续两轮零增量”静止计数：防止采样瞬间追平导致误判 READY
    quiescent_ticks: int = 0
    gates: tuple[GateResult, ...] = ()
    last_error: str | None = None
    attempts: dict[str, int] = field(default_factory=dict)  # 各步骤重试次数
    final: CutoverFinal | None = None
    proof_id: str | None = None
    switched_at: float | None = None

    def bump(self, **changes: Any) -> "PlanStatus":
        # 逐字段复制（不能用 dataclasses.asdict：它会把嵌套 dataclass
        # 如 CutoverFinal/ChecksumReport 递归转成 dict，破坏类型）
        data = {f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
        data.update(changes)
        data["revision"] = self.revision + 1
        data["gates"] = tuple(data["gates"])
        data["attempts"] = dict(data["attempts"])
        return PlanStatus(**data)


@dataclass(frozen=True)
class Plan:
    spec: PlanSpec
    status: PlanStatus

    def with_status(self, status: PlanStatus) -> "Plan":
        return Plan(self.spec, status)
