"""迁移协调器：声明式 reconcile 循环 + 原子切换。

设计要点
========
1. **计划冻结原子化** (:meth:`Controller.create_plan`)：在“源锁 -> 目标锁”的固定
   加锁顺序内，一次拿到快照位点 L、行镜像、模式版本、旧消费者水位，同时创建
   目标订阅槽与复制原点。之后规格永不改变。
2. **每个阶段动作幂等**：backfill 按表标记、pump 按 LSN 去重、栅栏迁移按状态
   判重、新槽按名复用。崩溃后重试只会收敛，不会重复推进水位。
3. **两道闸**：位点闸（目标远端位点到达旧水位）与摘要闸（快照 + 增量 WAL 段
   源/目标摘要逐字节一致）。切换在 SEALED 窗口内**重新验证**全部闸门。
4. **CAS 推进**：状态写入带 revision；与并发活动冲突则放弃本轮。
"""
from __future__ import annotations

import dataclasses
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from .digest import digest_payload, lsn_str, snapshot_digest
from .fence import Fence, FenceError, FenceStore
from .memcluster import MemCluster, PumpFault
from .proof import build_proof
from .store import CASStore, EventLog
from .types import (
    ChecksumReport,
    CutoverFinal,
    GATE_FULL_TABLE_DIGEST,
    GATE_JOURNAL_DIGEST,
    GATE_LSN_CAUGHT_UP,
    GATE_OLD_WM_PAST_START,
    GATE_PUMP_APPLIED,
    GATE_SCHEMA,
    GateResult,
    Phase,
    Plan,
    PlanSpec,
    PlanStatus,
    Snapshot,
    TableRef,
    TRANSITIONS,
)

GATES = (
    GATE_SCHEMA,
    GATE_PUMP_APPLIED,
    GATE_LSN_CAUGHT_UP,
    GATE_OLD_WM_PAST_START,
    GATE_JOURNAL_DIGEST,
    GATE_FULL_TABLE_DIGEST,
)


class GateFailure(RuntimeError):
    """切换窗口内闸门复验失败；协调器回退到 READY 并重新打开旧侧栅栏。"""


class PlanRejected(RuntimeError):
    pass


@dataclass
class _PlanRuntime:
    plan: Plan
    snapshot: Snapshot
    revision: int


class Controller:
    def __init__(
        self,
        store: CASStore | None = None,
        fences: FenceStore | None = None,
        events: EventLog | None = None,
        clock: Callable[[], float] = time.time,
        proof_key: bytes = b"slotmigrator-demo-proof-key",
    ) -> None:
        self.store = store or CASStore()
        self.fences = fences or FenceStore(self.store)
        self.events = events or EventLog(clock)
        self.clock = clock
        self.proof_key = proof_key
        self.clusters: dict[str, MemCluster] = {}
        self._runtimes: dict[str, _PlanRuntime] = {}
        self._lock = threading.RLock()
        # 切换成功回调（harness 用来创建新消费者）
        self.on_cutover: Callable[[Plan], None] | None = None

    def register_cluster(self, c: MemCluster) -> None:
        self.clusters[c.name] = c

    def cluster(self, name: str) -> MemCluster:
        return self.clusters[name]

    # ------------------------------------------------------------ 计划冻结

    def create_plan(
        self,
        *,
        plan_id: str | None = None,
        source_cluster: str,
        source_slot: str,
        target_cluster: str,
        subscription: str | None = None,
        target_slot: str | None = None,
        tables: list[TableRef],
    ) -> Plan:
        """原子冻结迁移计划。

        固定加锁顺序（先源后目标），临界区内完成：旧水位读取、一致快照导出、
        模式版本读取、源订阅槽创建、目标原点初始化与模式同步。
        """
        plan_id = plan_id or f"plan-{uuid.uuid4().hex[:12]}"
        sub = subscription or f"sub_{plan_id}"
        new_slot = target_slot or f"{source_slot}_new"
        src = self.clusters[source_cluster]
        dst = self.clusters[target_cluster]

        with self._lock:
            if self._plan_key(plan_id) in {k for k in self.store.keys()}:
                raise PlanRejected(f"plan {plan_id} already exists")
            with src.lock:
                if source_slot not in src._slots:  # noqa: SLF001 - 同包模拟件
                    raise PlanRejected(f"源复制槽 {source_slot} 不存在")
                wm = src.slot_watermark(source_slot)
                snapshot = src.export_snapshot(tables)
                schema = snapshot.schema_version
                start_lsn = snapshot.snap_lsn
                # 目标在源侧的订阅槽（与旧消费者槽并存，互不推进对方水位）
                src.create_slot(sub, restart_lsn=start_lsn, kind="subscription")
                with dst.lock:
                    # 模式同步：按冻结版本在目标建表
                    for ts in snapshot.tables:
                        sc = src.table_schema(ts.ref)
                        dst.ensure_table(sc)
                    dst.init_origin(sub, source_cluster, start_lsn)

            # 注册槽所有权：同一源槽禁止两个活动计划（并发计划直接拒绝）
            try:
                self.fences.register(
                    source_cluster, source_slot, plan_id, new_slot, target_cluster
                )
            except FenceError as ex:
                raise PlanRejected(str(ex)) from ex

            spec = PlanSpec(
                plan_id=plan_id,
                source_cluster=source_cluster,
                source_slot=source_slot,
                target_cluster=target_cluster,
                subscription=sub,
                target_slot=new_slot,
                tables=tuple(sorted(tables, key=lambda t: t.fqn)),
                schema_version=schema,
                start_lsn=start_lsn,
                consumer_watermark_at_plan=wm,
            )
            status = PlanStatus(snap_lsn=start_lsn)
            plan = Plan(spec=spec, status=status)
            rev = self.store.put(self._plan_key(plan_id), plan)
            self._runtimes[plan_id] = _PlanRuntime(plan=plan, snapshot=snapshot,
                                                   revision=rev)
            self.events.append(
                "plan_frozen", plan_id,
                start_lsn=start_lsn, start_lsn_text=lsn_str(start_lsn),
                old_watermark=wm, tables=[t.fqn for t in spec.tables],
                schema_version=schema.version, subscription=sub,
                fingerprint=plan_fingerprint(spec),
            )
            return plan

    # ------------------------------------------------------------ reconcile

    def get_plan(self, plan_id: str) -> Plan:
        _, plan = self.store.get(self._plan_key(plan_id))
        if plan is None:
            raise KeyError(plan_id)
        return plan  # type: ignore[return-value]

    def list_plans(self) -> list[Plan]:
        return [v for k, _, v in self.store.items() if k.startswith("plan/")]

    def tick(self, plan_id: str) -> Plan:
        """推进一个计划一整轮（幂等）。"""
        with self._lock:
            rev, plan = self.store.get(self._plan_key(plan_id))
            if plan is None:
                raise KeyError(plan_id)
            rt = self._runtimes[plan_id]
            phase = plan.status.phase
            try:
                if phase is Phase.PENDING:
                    plan = self._advance(plan, rev, Phase.BACKFILLING)
                    rev += 1
                if plan.status.phase is Phase.BACKFILLING:
                    plan, rev = self._tick_backfill(plan, rev, rt.snapshot)
                if plan.status.phase is Phase.CATCHING_UP:
                    plan, rev = self._tick_catchup(plan, rev)
                if plan.status.phase is Phase.READY:
                    plan, rev = self._tick_catchup(plan, rev)
                if plan.status.phase is Phase.SEALING:
                    # 上次 switch 在中途崩溃 -> 安全重放切换
                    plan, rev = self._continue_cutover(plan, rev)
                if plan.status.phase is Phase.SWITCHED:
                    # 切换后订阅仍持续把源增量回放到目标（新消费者的来源）
                    spec = plan.spec
                    dst = self.clusters[spec.target_cluster]
                    src = self.clusters[spec.source_cluster]
                    p = dst.pump_drain(spec.subscription, src, list(spec.tables))
                    plan = Plan(spec, plan.status.bump(
                        last_applied_remote_lsn=p.applied_remote_lsn,
                        last_observed_insert_lsn=src.current_lsn(),
                    ))
                    self.store.cas(self._plan_key(plan_id), rev, plan)
                    rev += 1
            except (PumpFault,) as ex:
                st = self._bump(plan, rev, attempts_inc=("fault",),
                                last_error=f"瞬时故障，等待重试: {ex}")
                plan = Plan(plan.spec, st)
                rev += 1
                self.events.append("fault_retry", plan_id, error=str(ex))
            rt.plan = plan
            rt.revision = rev
            return plan

    def reap(self, max_rounds: int = 1) -> list[Plan]:
        """推进所有非终态计划（真实控制器里是 watch 队列）。"""
        out = []
        for _ in range(max_rounds):
            plans = [p for p in self.list_plans() if not p.status.phase.terminal]
            if not plans:
                break
            for p in plans:
                out.append(self.tick(p.spec.plan_id))
        return out

    def run_until(self, plan_id: str, target: Phase,
                  max_ticks: int = 5000,
                  between: Callable[[], None] | None = None) -> Plan:
        """测试/演示辅助：reconcile 直到进入目标阶段（READY 满足即可）。"""
        for _ in range(max_ticks):
            plan = self.tick(plan_id)
            if between:
                between()
            if plan.status.phase is target:
                return plan
            if plan.status.phase is Phase.FAILED:
                raise RuntimeError(f"计划失败: {plan.status.last_error}")
        raise TimeoutError(f"等待 {target.value} 超时")  # pragma: no cover

    # ------------------------------------------------------------ 阶段实现

    def _tick_backfill(self, plan: Plan, rev: int, snapshot: Snapshot):
        spec = plan.spec
        src = self.clusters[spec.source_cluster]
        dst = self.clusters[spec.target_cluster]
        st = plan.status

        # 模式漂移检查（源端继续写入期间若发生 DDL，冻结计划立即失败）
        ok, why = self._schema_ok(spec)
        if not ok:
            return self._fail(plan, rev, why)

        for ts in snapshot.tables:
            dst.backfill_table(spec.subscription, ts)
            st = self._bump(plan, rev, attempts_inc=("backfill",))
            plan = Plan(spec, st)
            rev += 1

        # 快照摘要比对：冻结快照 vs 目标回填后的当前行镜像
        source_digest = snapshot_digest(snapshot.tables)
        target_tables = [dst.build_table_snapshot(t.ref) for t in snapshot.tables]
        target_digest = snapshot_digest(target_tables)
        report = ChecksumReport(
            kind="snapshot", from_lsn=0, to_lsn=snapshot.snap_lsn,
            source_digest=source_digest, target_digest=target_digest,
            match=source_digest == target_digest,
            detail="一致快照行镜像回填比对",
        )
        if not report.match:
            return self._fail(
                plan, rev,
                f"快照校验摘要不一致: src={source_digest[:12]} "
                f"tgt={target_digest[:12]}（目标数据损坏或回填错误）",
                backfill_check=report,
            )
        self.events.append(
            "checksum_ok", spec.plan_id, report=report.kind,
            digest=source_digest, snap_lsn=snapshot.snap_lsn,
        )
        plan = self._advance(
            plan, rev, Phase.CATCHING_UP,
            snapshot_digest=source_digest, backfill_done=True,
            backfill_check=report,
        )
        return plan, rev + 1

    def _tick_catchup(self, plan: Plan, rev: int):
        spec = plan.spec
        src = self.clusters[spec.source_cluster]
        dst = self.clusters[spec.target_cluster]

        ok, why = self._schema_ok(spec)
        if not ok:
            return self._fail(plan, rev, why)

        # 增量传输（幂等：已应用条目按 origin 映射去重）
        p = dst.pump_drain(spec.subscription, src, list(spec.tables))

        inserted = src.current_lsn()
        wm = src.slot_watermark(spec.source_slot)
        origin = dst.origin(spec.subscription)
        applied = origin.applied_remote_lsn

        gates = self._evaluate_gates(plan, applied=applied, inserted=inserted,
                                     wm=wm)
        # 静止判据：目标必须连续两轮在泵排空后仍保持零滞后，
        # 防止“采样瞬间碰巧追平”的误判（闸门 G3 内部消费此计数）
        lag = inserted - applied
        qt = plan.status.quiescent_ticks
        qt = qt + 1 if lag == 0 else 0
        all_green = all(g.ok for g in gates)
        st = plan.status.bump(
            gates=gates,
            pump_started=True,
            quiescent_ticks=qt,
            last_applied_remote_lsn=applied,
            last_observed_insert_lsn=inserted,
            last_error=None if all_green else "等待闸门变绿: "
            + ", ".join(g.gate for g in gates if not g.ok),
        )
        plan = Plan(spec, st)
        self.store.cas(self._plan_key(spec.plan_id), rev, plan)
        rev += 1

        if all_green and plan.status.phase is Phase.CATCHING_UP:
            plan = self._advance(plan, rev, Phase.READY)
            rev += 1
            self.events.append("ready", spec.plan_id, applied=applied,
                               old_watermark=wm)
        return plan, rev

    def _evaluate_gates(self, plan: Plan, *, applied: int, inserted: int,
                        wm: int, require_quiescence: bool = True,
                        full_digest: bool = False,
                        ) -> tuple[GateResult, ...]:
        spec = plan.spec
        src = self.clusters[spec.source_cluster]
        dst = self.clusters[spec.target_cluster]
        sv = src.schema_version()
        g1 = GateResult(
            gate=GATE_SCHEMA, ok=sv == spec.schema_version,
            detail="源模式与冻结计划一致" if sv == spec.schema_version
            else f"模式漂移: {sv.version}/{sv.digest[:8]} != "
                 f"{spec.schema_version.version}/{spec.schema_version.digest[:8]}",
            observed=sv.version, expected=spec.schema_version.version,
        )
        g2 = GateResult(
            gate=GATE_PUMP_APPLIED, ok=applied >= wm,
            detail=f"目标已回放 {lsn_str(applied)}，旧消费者水位 {lsn_str(wm)}",
            observed=applied, expected=wm,
        )
        caught = applied >= inserted
        g3_ok = caught and (plan.status.quiescent_ticks >= 2
                            or not require_quiescence)
        g3 = GateResult(
            gate=GATE_LSN_CAUGHT_UP, ok=g3_ok,
            detail=(f"目标远端位点 {lsn_str(applied)} vs 源当前 {lsn_str(inserted)}"
                    + ("（切换窗口内排空复检）" if not require_quiescence
                       else f"（静止轮次 {plan.status.quiescent_ticks}/2）")
                    if caught else f"目标落后 {inserted - applied} 条"),
            observed=applied, expected=inserted,
        )
        g4 = GateResult(
            gate=GATE_OLD_WM_PAST_START, ok=wm >= spec.start_lsn,
            detail=(f"旧消费者已越过快照位点: wm {lsn_str(wm)} "
                    f">= start {lsn_str(spec.start_lsn)}"),
            observed=wm, expected=spec.start_lsn,
        )
        # 只摘要“目标确定已回放”的范围 (start_lsn, min(applied,inserted)]，
        # 避免引用目标尚未映射的位点
        g5 = self._journal_gate(plan, upto=min(applied, inserted))
        gates = [g1, g2, g3, g4, g5]
        if full_digest:
            # 切换窗口内的最终物理对账：全表行镜像摘要必须逐字节一致。
            # 这能抓住“回填后静默损坏”这类不会反映在 WAL 链上的偏差。
            gates.append(self._full_table_gate(plan))
        return tuple(gates)

    def _full_table_gate(self, plan: Plan) -> GateResult:
        spec = plan.spec
        src = self.clusters[spec.source_cluster]
        dst = self.clusters[spec.target_cluster]
        with src.lock:
            src_d = src.snapshot_digest(spec.tables)
            with dst.lock:
                tgt_d = dst.snapshot_digest(spec.tables)
        ok = src_d == tgt_d
        return GateResult(
            gate=GATE_FULL_TABLE_DIGEST, ok=ok,
            detail=("切换窗口全表行镜像摘要一致" if ok
                    else "全表行镜像摘要不一致（目标存在静默损坏/错应用）"),
            observed=tgt_d[:16], expected=src_d[:16],
        )

    def _journal_gate(self, plan: Plan, *, upto: int) -> GateResult:
        spec = plan.spec
        if upto <= spec.start_lsn:
            return GateResult(
                gate=GATE_JOURNAL_DIGEST, ok=True,
                detail="暂无快照位点之后的增量段需要校验",
                observed=upto, expected=spec.start_lsn,
            )
        src = self.clusters[spec.source_cluster]
        dst = self.clusters[spec.target_cluster]
        # upto 可能落在非迁移表的间隙 LSN，回退到最近的已映射条目 LSN
        origin = dst.origin(spec.subscription)
        mapped = sorted(k for k in origin.remote_to_local if spec.start_lsn < k <= upto)
        if not mapped:
            return GateResult(
                gate=GATE_JOURNAL_DIGEST, ok=True,
                detail="快照位点之后尚无迁移表条目",
                observed=upto, expected=spec.start_lsn,
            )
        upto = mapped[-1]
        entries_src = [e for e in src.read_changes(spec.start_lsn, spec.tables)
                       if e.lsn <= upto]
        src_d = src.source_journal_digest_for(spec.tables, spec.start_lsn, upto)
        local_to = origin.remote_to_local[upto]
        tgt_d = dst.target_journal_digest(
            spec.tables, 0, local_to,
            logical_bounds=(entries_src[0].lsn, upto) if entries_src else None,
        )
        return GateResult(
            gate=GATE_JOURNAL_DIGEST, ok=src_d == tgt_d,
            detail=("增量 WAL 段摘要一致 " if src_d == tgt_d
                    else "增量 WAL 段摘要不一致（丢事件/重复应用/错应用） ")
            + f"[{lsn_str(spec.start_lsn)}..{lsn_str(upto)}]",
            observed=tgt_d[:16], expected=src_d[:16],
        )

    # ------------------------------------------------------------ 原子切换

    def switch(self, plan_id: str) -> Plan:
        """用户发起切换：READY -> SEALING（密封栅栏）-> SWITCHED。

        对 SWITCHED 幂等：直接返回完成态（重放/重试不会重复迁移栅栏）。
        """
        with self._lock:
            rev, plan = self.store.get(self._plan_key(plan_id))
            if plan.status.phase is Phase.SWITCHED:
                return plan
            if plan.status.phase not in (Phase.READY, Phase.SEALING):
                raise GateFailure(
                    f"计划处于 {plan.status.phase.value}，不允许切换"
                )
            return self._continue_cutover(plan, rev)

    def _continue_cutover(self, plan: Plan, rev: int) -> Plan:
        """切换流程可安全重放：每一步对栅栏/槽状态判重。"""
        spec = plan.spec
        dst = self.clusters[spec.target_cluster]
        fence = self.fences.get(spec.source_cluster, spec.source_slot)
        assert fence is not None

        # 1) 密封旧侧（幂等）
        if fence.state.value == "open_old":
            rev_before = fence.revision
            fence = self.fences.seal(fence)
            self.events.append(
                "fence_sealed", spec.plan_id, epoch=fence.epoch,
                revision=fence.revision,
            )
            plan = self._advance(plan, rev, Phase.SEALING)
            rev += 1
        else:
            # 重入：rev_before = 密封前 revision（seal/open_new 各使 revision+1）
            rev_before = max(0, fence.revision - 2)

        # 2) SEALED 窗口内：追平、闸门复验、位点翻译、建新槽、打开新侧
        if fence.state.value == "sealed":
            gates = self._sealed_verify(plan, pump=True)
            src = self.clusters[spec.source_cluster]
            wm = src.slot_watermark(spec.source_slot)
            local_lsn = dst.map_remote_to_local(spec.subscription, wm)
            existing = dst._slots.get(spec.target_slot)  # noqa: SLF001
            if existing is None:
                dst.create_consumer_slot(spec.target_slot, local_lsn)
            elif existing.confirmed_lsn != local_lsn:
                raise GateFailure(
                    f"目标槽 {spec.target_slot} 已存在且起点不一致"
                )
            sealed_epoch = fence.epoch
            fence = self.fences.open_new(fence)
            self.events.append(
                "fence_open_new", spec.plan_id, epoch=fence.epoch,
                revision=fence.revision, target_slot=spec.target_slot,
                target_local_lsn=local_lsn,
            )
            return self._finalize_switch(plan, rev, fence, gates,
                                         local_lsn, sealed_epoch, rev_before)

        # 3) 崩溃窗口恢复：栅栏已 OPEN_NEW，状态/证明还没落盘 -> 幂等收尾
        if fence.state.value == "open_new" and plan.status.phase is Phase.SEALING:
            gates = self._sealed_verify(plan, pump=True)
            src = self.clusters[spec.source_cluster]
            wm = src.slot_watermark(spec.source_slot)
            local_lsn = dst.map_remote_to_local(spec.subscription, wm)
            existing = dst._slots.get(spec.target_slot)  # noqa: SLF001
            if existing is None or existing.confirmed_lsn != local_lsn:
                raise GateFailure("OPEN_NEW 后恢复失败：目标槽起点缺失/不一致")
            self.events.append("cutover_recovered", spec.plan_id,
                               fence_revision=fence.revision)
            return self._finalize_switch(plan, rev, fence, gates,
                                         local_lsn, fence.epoch - 1, rev_before)

        # 已 OPEN_NEW 且已完成：直接返回完成态
        return self.get_plan(spec.plan_id)

    def _sealed_verify(self, plan: Plan, *, pump: bool) -> tuple[GateResult, ...]:
        """SEALED 窗口内的闸门复验。任何一项失败都回退旧侧并拒绝切换。"""
        spec = plan.spec
        src = self.clusters[spec.source_cluster]
        dst = self.clusters[spec.target_cluster]
        fence = self.fences.get(spec.source_cluster, spec.source_slot)
        if pump:
            # 全程持源锁排空：消除“排空后、取位点前”又有新写入的窗口
            dst.drain_while_holding_source(spec.subscription, src, list(spec.tables))
        wm = src.slot_watermark(spec.source_slot)       # 旧水位已冻结
        inserted = src.current_lsn()
        applied = dst.origin(spec.subscription).applied_remote_lsn
        gates = self._evaluate_gates(plan, applied=applied,
                                     inserted=inserted, wm=wm,
                                     require_quiescence=False,
                                     full_digest=True)
        failed = [g for g in gates if not g.ok]
        if failed:
            self.fences.reopen_old(fence)
            rev_now, cur = self.store.get(self._plan_key(spec.plan_id))
            cur = cur.with_status(cur.status.bump(
                phase=Phase.READY, gates=gates,
                last_error="切换复验失败，已回退: "
                + ", ".join(g.gate for g in failed),
            ))
            self.store.cas(self._plan_key(spec.plan_id), rev_now, cur)
            self.events.append(
                "cutover_aborted", spec.plan_id,
                failed=[g.gate for g in failed],
            )
            raise GateFailure(
                "切换闸门复验失败: " + ", ".join(g.gate for g in failed)
            )
        return gates

    def _finalize_switch(self, plan: Plan, rev: int, fence: Fence,
                         gates: tuple[GateResult, ...], local_lsn: int,
                         sealed_epoch: int, rev_before: int) -> Plan:
        spec = plan.spec
        src = self.clusters[spec.source_cluster]
        dst = self.clusters[spec.target_cluster]
        wm = src.slot_watermark(spec.source_slot)
        inserted = src.current_lsn()
        applied = dst.origin(spec.subscription).applied_remote_lsn

        # 最终增量摘要：start_lsn .. 旧水位，源/目标逐字节比对。
        # 旧水位一定是旧消费者确认过的迁移表条目 LSN（消费者只 ack 读到的条目）。
        origin = dst.origin(spec.subscription)
        src_entries = [e for e in src.read_changes(spec.start_lsn, spec.tables)
                       if e.lsn <= wm]
        journal_src = src.source_journal_digest_for(
            spec.tables, spec.start_lsn, wm
        )
        local_to = origin.remote_to_local.get(wm)
        if local_to is None:  # 防御性：理论上 G2 已保证映射存在
            raise GateFailure(f"旧水位 {wm} 在目标无映射，拒绝切换")
        bounds = (src_entries[0].lsn, wm) if src_entries else None
        journal_tgt = dst.target_journal_digest(
            spec.tables, 0, local_to, logical_bounds=bounds
        )
        if journal_src != journal_tgt:
            self.fences.reopen_old(fence)
            raise GateFailure("最终增量摘要不一致，已回退到旧槽")

        fp = plan_fingerprint(spec)
        # 全表摘要取密封复验时刻的 G6 结果（复验与 finalize 间源可能继续写入，
        # 但旧水位已冻结、G6 证明的就是该边界的物理一致）
        g6 = next(g for g in gates if g.gate == GATE_FULL_TABLE_DIGEST)
        final = CutoverFinal(
            old_watermark_src_lsn=wm,
            target_remote_lsn=applied,
            target_local_lsn=local_lsn,
            source_insert_lsn=inserted,
            snapshot_digest=plan.status.snapshot_digest or "",
            full_table_digest=g6.expected,
            journal_digest_from_start=journal_src,
            schema_version=spec.schema_version,
            plan_fingerprint=fp,
            fence_revision_before=rev_before,
            fence_revision_after=fence.revision,
            sealed_epoch=sealed_epoch,
        )
        np = plan.with_status(plan.status.bump(
            phase=Phase.SWITCHED,
            gates=gates,
            final=final,
            proof_id=f"proof-{spec.plan_id}",
            switched_at=self.clock(),
        ))
        self.store.cas(self._plan_key(spec.plan_id), rev, np)
        proof = build_proof(np, list(gates), self.events.all(), self.proof_key)
        self.store.put(f"proof/{spec.plan_id}", proof)
        self.events.append(
            "cutover_switched", spec.plan_id,
            old_watermark=wm, target_remote=applied,
            target_local=local_lsn, fingerprint=fp,
        )
        if self.on_cutover:
            self.on_cutover(np)
        return np


    # ------------------------------------------------------------ 杂项

    def proof_for(self, plan_id: str):
        _, proof = self.store.get(f"proof/{plan_id}")
        return proof

    def reconcile_view(self, plan_id: str) -> dict[str, Any]:
        """位点对账页面数据：计划、闸门、双端位点、栅栏、被拒迟到 ack。"""
        plan = self.get_plan(plan_id)
        spec, st = plan.spec, plan.status
        src = self.clusters[spec.source_cluster]
        dst = self.clusters[spec.target_cluster]
        fence = self.fences.get(spec.source_cluster, spec.source_slot)
        origin = dst._origins.get(spec.subscription)  # noqa: SLF001
        wm = src.slot_watermark(spec.source_slot)
        applied = origin.applied_remote_lsn if origin else None
        inserted = src.current_lsn()
        new_slot = dst._slots.get(spec.target_slot)  # noqa: SLF001
        return {
            "plan_id": spec.plan_id,
            "phase": st.phase.value,
            "revision": st.revision,
            "frozen": {
                "source_cluster": spec.source_cluster,
                "target_cluster": spec.target_cluster,
                "start_lsn": spec.start_lsn,
                "start_lsn_text": lsn_str(spec.start_lsn),
                "tables": [t.fqn for t in spec.tables],
                "schema_version": spec.schema_version.version,
                "schema_digest": spec.schema_version.digest,
                "subscription": spec.subscription,
                "source_slot": spec.source_slot,
                "target_slot": spec.target_slot,
                "fingerprint": plan_fingerprint(spec),
                "consumer_watermark_at_plan": spec.consumer_watermark_at_plan,
            },
            "positions": {
                "source_insert_lsn": inserted,
                "source_insert_lsn_text": lsn_str(inserted),
                "old_slot_watermark": wm,
                "old_slot_watermark_text": lsn_str(wm),
                "subscription_slot_watermark":
                    src.slot_watermark(spec.subscription),
                "target_applied_remote_lsn": applied,
                "target_applied_remote_lsn_text":
                    lsn_str(applied) if applied is not None else None,
                "new_target_slot_watermark":
                    new_slot.confirmed_lsn if new_slot else None,
                "lag_entries": None if applied is None else inserted - applied,
                "old_consumer_behind": max(0, inserted - wm),
            },
            "gates": [dataclasses.asdict(g) for g in st.gates],
            "snapshot_digest": st.snapshot_digest,
            "fence": None if fence is None else {
                "state": fence.state.value,
                "epoch": fence.epoch,
                "revision": fence.revision,
                "rejected_acks": list(fence.rejected_acks),
            },
            "final": dataclasses.asdict(st.final) if st.final else None,
            "proof_id": st.proof_id,
            "last_error": st.last_error,
            "attempts": dict(st.attempts),
        }

    def _schema_ok(self, spec: PlanSpec) -> tuple[bool, str]:
        sv = self.clusters[spec.source_cluster].schema_version()
        if sv != spec.schema_version:
            return False, (
                f"模式漂移：源已到 v{sv.version}({sv.digest[:12]})，"
                f"计划冻结 v{spec.schema_version.version}"
                f"({spec.schema_version.digest[:12]})；冻结计划终止，请重建计划"
            )
        return True, ""

    def _fail(self, plan: Plan, rev: int, reason: str, **changes: Any):
        self.events.append("plan_failed", plan.spec.plan_id, reason=reason)
        np = plan.with_status(plan.status.bump(phase=Phase.FAILED,
                                              last_error=reason, **changes))
        self.store.cas(self._plan_key(plan.spec.plan_id), rev, np)
        return np, rev + 1

    def _advance(self, plan: Plan, rev: int, phase: Phase, **changes: Any) -> Plan:
        allowed = TRANSITIONS[plan.status.phase]
        if phase not in allowed:
            raise RuntimeError(
                f"非法状态迁移 {plan.status.phase.value} -> {phase.value}"
            )
        np = plan.with_status(plan.status.bump(phase=phase, **changes))
        self.store.cas(self._plan_key(plan.spec.plan_id), rev, np)
        self.events.append("phase", plan.spec.plan_id, phase=phase.value)
        return np

    def _bump(self, plan: Plan, rev: int, *, attempts_inc: tuple[str, ...] = (),
              **changes: Any) -> PlanStatus:
        attempts = dict(plan.status.attempts)
        for k in attempts_inc:
            attempts[k] = attempts.get(k, 0) + 1
        np = plan.with_status(
            plan.status.bump(attempts=attempts, **changes)
        )
        self.store.cas(self._plan_key(plan.spec.plan_id), rev, np)
        return np.status

    @staticmethod
    def _plan_key(plan_id: str) -> str:
        return f"plan/{plan_id}"


def plan_fingerprint(spec: PlanSpec) -> str:
    return digest_payload(spec.fingerprint_payload())
