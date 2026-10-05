"""内存数据库集群：同时扮演源集群与目标集群。

虽然是模拟件，它刻意保留了真实 PostgreSQL 逻辑复制里与正确性相关的全部语义：

* 全局单调 WAL（LSN），复制槽保存 ``confirmed_flush_lsn``，且**只允许单调前进**；
* 复制原点（replication origin）记录“目标已回放到哪个远端 LSN”，并维护
  远端 LSN -> 目标本地 LSN 的稠密映射，切换时用它做位点翻译；
* 快照在单个临界区内原子导出（快照位点 + 行镜像一致）；
* pump 至少一次投递，apply 用确定性 upsert/幂等删除重放，崩溃重试不会重复推进；
* 可注入故障（fetch 失败、batch 中途崩溃、backfill 失败）和静默行损坏。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Iterable

from .digest import journal_digest, snapshot_digest
from .types import (
    Change,
    ChangeKind,
    LSN,
    RowImage,
    SchemaVersion,
    Snapshot,
    TableRef,
    TableSchema,
    TableSnapshot,
    WALEntry,
)


class LSNRegression(Exception):
    """拒绝把复制槽水位向回推进（双水位/重复确认的最后防线）。"""


class PumpFault(Exception):
    """pump 被注入的瞬时故障；操作可能在任意条目后中断，重放必须幂等。"""


@dataclass
class Slot:
    name: str
    confirmed_lsn: LSN = 0
    kind: str = "consumer"          # consumer | subscription


@dataclass
class Origin:
    """目标侧复制原点：远端位点 + 远端->本地 LSN 映射。"""

    subscription: str
    source_cluster: str
    applied_remote_lsn: LSN
    last_local_lsn: LSN = 0
    remote_to_local: dict[LSN, LSN] = field(default_factory=dict)


@dataclass
class PumpProgress:
    fetched: int
    applied: int
    skipped_snapshot: int
    applied_remote_lsn: LSN
    inserted_local_to: LSN


@dataclass
class _Table:
    schema: TableSchema
    rows: dict[str, tuple] = field(default_factory=dict)


class MemCluster:
    def __init__(self, name: str) -> None:
        self.name = name
        self._lock = threading.RLock()
        self._tables: dict[TableRef, _Table] = {}
        self._wal: list[WALEntry] = []          # index == lsn - 1
        self._slots: dict[str, Slot] = {}
        self._origins: dict[str, Origin] = {}
        self._backfill_done: dict[str, set[str]] = {}  # sub -> tables
        self._ddl_version = 0
        # 故障注入（一次性，计数归零即恢复，模拟“重试后成功”）
        self.faults: dict[str, int] = {}

    # ---------------------------------------------------------------- 基础

    @property
    def lock(self) -> threading.RLock:
        """跨集群操作时按固定顺序（先源后目标）获取锁。"""
        return self._lock

    def create_table(self, schema: str, table: str,
                     columns: list[tuple[str, str]], pk: str) -> TableSchema:
        ref = TableRef(schema, table)
        with self._lock:
            self._ddl_version += 1
            ts = TableSchema(ref=ref, columns=tuple(columns), pk=pk,
                             version=self._ddl_version)
            self._tables[ref] = _Table(schema=ts)
            return ts

    def schema_version(self) -> SchemaVersion:
        with self._lock:
            return SchemaVersion.compute({r: t.schema for r, t in self._tables.items()})

    def table_schema(self, ref: TableRef) -> TableSchema:
        with self._lock:
            return self._tables[ref].schema

    def current_lsn(self) -> LSN:
        with self._lock:
            return len(self._wal)

    def _pk_index(self, schema: TableSchema) -> int:
        return [n for n, _ in schema.columns].index(schema.pk)

    def _row_key(self, schema: TableSchema, row: tuple) -> str:
        return str(row[self._pk_index(schema)])

    # ---------------------------------------------------------------- 写入

    def insert(self, ref: TableRef, row: tuple) -> LSN:
        with self._lock:
            t = self._tables[ref]
            key = self._row_key(t.schema, row)
            t.rows[key] = tuple(row)
            return self._append(ref, Change(kind=ChangeKind.INSERT, after=tuple(row)))

    def update(self, ref: TableRef, pk: str, changes: dict[str, object]) -> LSN:
        with self._lock:
            t = self._tables[ref]
            old = t.rows[str(pk)]
            names = [n for n, _ in t.schema.columns]
            new = list(old)
            for k, v in changes.items():
                new[names.index(k)] = v
            new_t = tuple(new)
            t.rows[str(pk)] = new_t
            return self._append(
                ref, Change(kind=ChangeKind.UPDATE, before=old, after=new_t)
            )

    def delete(self, ref: TableRef, pk: str) -> LSN:
        with self._lock:
            t = self._tables[ref]
            old = t.rows.pop(str(pk))
            return self._append(ref, Change(kind=ChangeKind.DELETE, before=old))

    def add_column(self, ref: TableRef, name: str, typ: str, default: object) -> LSN:
        """模式变更：全局 DDL 版本 +1，老行用默认值补齐。"""
        with self._lock:
            self._ddl_version += 1
            t = self._tables[ref]
            old = t.schema
            t.schema = TableSchema(
                ref=ref,
                columns=old.columns + ((name, typ),),
                pk=old.pk,
                version=self._ddl_version,
            )
            t.rows = {k: v + (default,) for k, v in t.rows.items()}
            return self._append(
                ref,
                Change(kind=ChangeKind.DDL, ddl=f"ALTER TABLE {ref.fqn} ADD {name} {typ}",
                       schema_version=self._ddl_version),
            )

    def _append(self, ref: TableRef, change: Change) -> LSN:
        lsn = len(self._wal) + 1
        self._wal.append(WALEntry(lsn=lsn, ref=ref, change=change))
        return lsn

    # ---------------------------------------------------------------- 复制槽

    def create_slot(self, name: str, restart_lsn: LSN = 0, kind: str = "consumer") -> Slot:
        with self._lock:
            if name in self._slots:
                return self._slots[name]
            slot = Slot(name=name, confirmed_lsn=restart_lsn, kind=kind)
            self._slots[name] = slot
            return slot

    def slot_watermark(self, name: str) -> LSN:
        with self._lock:
            return self._slots[name].confirmed_lsn

    def advance_slot(self, name: str, lsn: LSN) -> None:
        """推进复制槽水位。回退一律拒绝——存储层的单调性保证。"""
        with self._lock:
            slot = self._slots[name]
            if lsn < slot.confirmed_lsn:
                raise LSNRegression(
                    f"slot {name}: {slot.confirmed_lsn} -> {lsn} 回退被拒绝"
                )
            slot.confirmed_lsn = lsn

    def read_changes(self, from_lsn: LSN, tables: Iterable[TableRef],
                     limit: int | None = None) -> list[WALEntry]:
        """读取给定表集合上 ``lsn > from_lsn`` 的数据变更（DDL 控制条目不返回）。"""
        wanted = {t.fqn for t in tables}
        with self._lock:
            out: list[WALEntry] = []
            for e in self._wal[from_lsn:]:
                if e.change.kind is ChangeKind.DDL:
                    continue
                if e.ref.fqn in wanted:
                    out.append(e)
                    if limit is not None and len(out) >= limit:
                        break
            return out

    # ---------------------------------------------------------------- 快照

    def export_snapshot(self, tables: Iterable[TableRef]) -> Snapshot:
        """原子导出：在同一临界区取当前 LSN 与全部行镜像。"""
        with self._lock:
            snap_lsn = len(self._wal)
            images: list[TableSnapshot] = []
            for ref in tables:
                t = self._tables[ref]
                rows = tuple(
                    RowImage(pk=k, row=t.rows[k])
                    for k in sorted(t.rows)
                )
                images.append(
                    TableSnapshot(ref=ref, columns=t.schema.columns, rows=rows)
                )
            return Snapshot(
                snap_lsn=snap_lsn,
                tables=tuple(images),
                schema_version=self.schema_version_locked(),
            )

    def schema_version_locked(self) -> SchemaVersion:
        return SchemaVersion.compute({r: t.schema for r, t in self._tables.items()})

    def snapshot_digest(self, tables: Iterable[TableRef]) -> str:
        with self._lock:
            snap = self.export_snapshot(tables)
            return snapshot_digest(snap.tables)

    def build_table_snapshot(self, ref: TableRef) -> TableSnapshot:
        with self._lock:
            t = self._tables[ref]
            rows = tuple(RowImage(pk=k, row=t.rows[k]) for k in sorted(t.rows))
            return TableSnapshot(ref=ref, columns=t.schema.columns, rows=rows)

    # ---------------------------------------------------------------- 目标端

    def ensure_table(self, schema: TableSchema) -> None:
        with self._lock:
            if schema.ref not in self._tables:
                self._tables[schema.ref] = _Table(schema=schema)

    def init_origin(self, subscription: str, source_cluster: str,
                    from_lsn: LSN) -> Origin:
        with self._lock:
            if subscription in self._origins:
                return self._origins[subscription]
            o = Origin(subscription=subscription, source_cluster=source_cluster,
                       applied_remote_lsn=from_lsn)
            self._origins[subscription] = o
            self._backfill_done.setdefault(subscription, set())
            return o

    def origin(self, subscription: str) -> Origin:
        with self._lock:
            return self._origins[subscription]

    def map_remote_to_local(self, subscription: str, remote_lsn: LSN) -> LSN:
        """把源位点翻译为目标本地位点。

        若该 LSN 本身不是被迁移表上的数据条目（例如落在两表的间隙 LSN 上），
        取不超过它的最近映射，模拟 PostgreSQL ``pg_replication_origin_advance``
        的位点语义；超过已回放范围则报错（目标还没追平，绝不允许切换）。
        """
        with self._lock:
            o = self._origins[subscription]
            if remote_lsn in o.remote_to_local:
                return o.remote_to_local[remote_lsn]
            if remote_lsn < o.applied_remote_lsn:
                mapped = [v for k, v in o.remote_to_local.items() if k <= remote_lsn]
                if mapped:
                    return max(mapped)
            raise KeyError(
                f"remote LSN {remote_lsn} 尚未在订阅 {subscription} 回放映射 "
                f"(applied={o.applied_remote_lsn})"
            )

    def backfill_table(self, subscription: str, ts: TableSnapshot) -> None:
        """幂等回填：按主键覆盖（重放同一批数据结果相同），不产生业务 WAL。

        调用前必须已用源端 :class:`TableSchema` 在目标建好表（列/主键一致）。
        """
        done = self._backfill_done.setdefault(subscription, set())
        if ts.ref.fqn in done:
            return
        if self._consume_fault("backfill_fault"):
            raise PumpFault("backfill 中途崩溃（注入）")
        with self._lock:
            t = self._tables[ts.ref]
            for img in ts.rows:
                t.rows[img.pk] = tuple(img.row)
            done.add(ts.ref.fqn)

    def mark_backfill(self, subscription: str, ref: TableRef) -> None:
        self._backfill_done.setdefault(subscription, set()).add(ref.fqn)

    def backfill_completed(self, subscription: str, tables: Iterable[TableRef]) -> bool:
        done = self._backfill_done.get(subscription, set())
        return all(t.fqn in done for t in tables)

    def pump_once(
        self,
        subscription: str,
        source: "MemCluster",
        tables: Iterable[TableRef],
        max_entries: int = 256,
    ) -> PumpProgress:
        """从源拉取一批变更并逐条原子回放。

        每条数据变更在**同一个目标临界区**内同时落本地 WAL 与远端->本地映射，
        因此“批末崩溃后重试”看到该条目已有映射就直接跳过，不会写第二条本地
        WAL（保证源/目标 WAL 段逐条目对齐，摘要可逐字节比对）。
        批次水位（applied_remote_lsn、源订阅槽）只在批末提交一次。
        """
        o = self._origins[subscription]
        if self._consume_fault("pump_fetch_fault"):
            raise PumpFault("拉取变更失败（注入）")

        # 固定加锁顺序：先源后目标
        with source.lock:
            entries = source.read_changes(o.applied_remote_lsn, tables, max_entries)
            with self._lock:
                return self._pump_apply(subscription, source, entries)

    def _pump_apply(self, subscription: str, source: "MemCluster",
                    entries: list[WALEntry]) -> PumpProgress:
        o = self._origins[subscription]
        applied = 0
        replayed = 0
        will_crash = self._consume_fault("pump_apply_fault")
        for e in entries:
            if e.lsn in o.remote_to_local:
                replayed += 1                      # 崩溃重试：条目已原子落盘
                continue
            local = self._apply_entry(e, o)
            o.remote_to_local[e.lsn] = local       # 与本地 WAL 同临界区提交
            applied += 1
            if will_crash:
                # 模拟批末水位提交前崩溃：已落盘的条目映射保留，水位不前进
                raise PumpFault(f"批处理在 LSN {e.lsn} 后崩溃（注入）")
        if entries:
            o.applied_remote_lsn = entries[-1].lsn
            # 目标订阅推进源端复制槽（源端保留策略），只进不退
            source.advance_slot(o.subscription, entries[-1].lsn)
        return PumpProgress(
            fetched=len(entries),
            applied=applied,
            skipped_snapshot=replayed,
            applied_remote_lsn=o.applied_remote_lsn,
            inserted_local_to=o.last_local_lsn,
        )

    def _apply_entry(self, e: WALEntry, o: Origin) -> LSN:
        """幂等回放单条变更，返回目标本地 LSN；保留远端 origin LSN 元数据。"""
        t = self._tables[e.ref]
        c = e.change
        if c.kind is ChangeKind.INSERT:
            t.rows[self._row_key(t.schema, c.after)] = tuple(c.after)
        elif c.kind is ChangeKind.UPDATE:
            t.rows[self._row_key(t.schema, c.after)] = tuple(c.after)
        elif c.kind is ChangeKind.DELETE:
            t.rows.pop(self._row_key(t.schema, c.before), None)
        local = len(self._wal) + 1
        self._wal.append(WALEntry(lsn=local, ref=e.ref, change=c,
                                  origin_lsn=e.lsn))
        o.last_local_lsn = local
        return local

    def pump_drain(self, subscription: str, source: "MemCluster",
                   tables: Iterable[TableRef]) -> PumpProgress:
        """连续 pump 直到没有新条目。"""
        total = PumpProgress(0, 0, 0, self._origins[subscription].applied_remote_lsn, 0)
        while True:
            p = self.pump_once(subscription, source, tables)
            total.fetched += p.fetched
            total.applied += p.applied
            total.applied_remote_lsn = p.applied_remote_lsn
            total.inserted_local_to = p.inserted_local_to
            if p.fetched == 0:
                return total

    def drain_while_holding_source(self, subscription: str,
                                   source: "MemCluster",
                                   tables: Iterable[TableRef]) -> PumpProgress:
        """切换窗口专用：全程持有源锁排空并取最终位点。

        这对应真实系统里“在可重复读快照/复制槽保护下 drain 到一致点”，
        保证返回的源当前位点 >= 已回放远端位点，且其间没有漏读的新写入。
        """
        with source.lock:
            total = PumpProgress(0, 0, 0,
                                 self._origins[subscription].applied_remote_lsn, 0)
            while True:
                p = self.pump_once_locked(subscription, source, tables)
                total.fetched += p.fetched
                total.applied += p.applied
                total.applied_remote_lsn = p.applied_remote_lsn
                total.inserted_local_to = p.inserted_local_to
                if p.fetched == 0:
                    return total

    def pump_once_locked(self, subscription: str, source: "MemCluster",
                         tables: Iterable[TableRef],
                         max_entries: int = 4096) -> PumpProgress:
        """调用方已持有源锁时的 pump（避免 drain 与取位点之间的窗口）。"""
        o = self._origins[subscription]
        if self._consume_fault("pump_fetch_fault"):
            raise PumpFault("拉取变更失败（注入）")
        entries = source.read_changes(o.applied_remote_lsn, tables, max_entries)
        with self._lock:
            return self._pump_apply(subscription, source, entries)

    def read_local_changes(self, from_lsn: LSN, tables: Iterable[TableRef],
                           limit: int | None = None) -> list[WALEntry]:
        wanted = {t.fqn for t in tables}
        with self._lock:
            return [
                e for e in self._wal[from_lsn:]
                if e.ref.fqn in wanted and e.change.kind is not ChangeKind.DDL
            ][: limit if limit is not None else None]

    def create_consumer_slot(self, name: str, start_local_lsn: LSN) -> Slot:
        with self._lock:
            return self.create_slot(name, start_local_lsn, kind="consumer")

    # ---------------------------------------------------------------- 校验

    def target_journal_digest(self, tables: Iterable[TableRef],
                              from_local_lsn: LSN, to_local_lsn: LSN,
                              logical_bounds: tuple[LSN, LSN] | None = None) -> str:
        """目标本地 WAL 段摘要（左开右闭切片）。

        ``logical_bounds`` 为 (源 start_lsn, 源 end_lsn)，保证外层摘要位点与
        源端字节一致（目标条目的 origin_lsn 已提供逐条目对齐）。
        """
        with self._lock:
            entries = [
                e for e in self._wal[from_local_lsn:to_local_lsn]
                if e.change.kind is not ChangeKind.DDL
            ]
            schemas = {r: self._tables[r].schema.columns for r in tables}
            return journal_digest(entries, schemas, bounds=logical_bounds)

    def source_journal_digest_for(self, tables: Iterable[TableRef],
                                  from_lsn: LSN, to_lsn: LSN) -> str:
        """源端同范围摘要。两端的 entry 负载逐字节一致才能匹配。"""
        with self._lock:
            wanted = {t.fqn for t in tables}
            entries = [
                e for e in self._wal[from_lsn:to_lsn]
                if e.ref.fqn in wanted and e.change.kind is not ChangeKind.DDL
            ]
            schemas = {r: self._tables[r].schema.columns for r in tables}
            return journal_digest(entries, schemas,
                                  bounds=(entries[0].lsn, entries[-1].lsn)) \
                if entries else journal_digest(entries, schemas)

    # ---------------------------------------------------------------- 测试

    def corrupt_row(self, ref: TableRef, pk: str, column: str, value: object) -> None:
        """测试辅助：静默篡改目标行（快照/增量摘要必须能发现）。"""
        with self._lock:
            t = self._tables[ref]
            names = [n for n, _ in t.schema.columns]
            row = list(t.rows[str(pk)])
            row[names.index(column)] = value
            t.rows[str(pk)] = tuple(row)

    def _consume_fault(self, name: str) -> bool:
        n = self.faults.get(name, 0)
        if n > 0:
            self.faults[name] = n - 1
            return True
        return False
