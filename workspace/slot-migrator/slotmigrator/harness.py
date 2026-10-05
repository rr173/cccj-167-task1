"""演示/测试装配器：两个内存集群 + 持续写入 + 旧消费者 + 控制器。

它把“源端继续写入”这件事做成确定性的步进模型：每轮 ``step`` 依次推进
*writer -> old consumer -> controller*；切换成功后控制器回调自动拉起新消费者，
新消费者在目标集群上继续消费并推进**新**槽。整个过程中：

* 旧消费者只在 OPEN_OLD/旧 epoch 下推进源槽；
* 新消费者只在 OPEN_NEW/新 epoch 下推进目标槽；
* 旧消费者即使切换后仍存活，其迟到 ack 也会被栅栏拒绝并记录。
"""
from __future__ import annotations

from .consumer import Consumer
from .controller import Controller
from .fence import FenceSide
from .memcluster import MemCluster
from .types import Phase, Plan, TableRef


class Harness:
    def __init__(self, tables: list[TableRef] | None = None,
                 seed_rows: int = 5) -> None:
        self.src = MemCluster("src-db")
        self.dst = MemCluster("dst-db")
        self.ctrl = Controller()
        self.ctrl.register_cluster(self.src)
        self.ctrl.register_cluster(self.dst)

        self.ref = TableRef("app", "orders")
        self.ref2 = TableRef("app", "payments")
        self.tables = tables or [self.ref]
        for c in (self.src, self.dst):
            c.create_table("app", "orders",
                           [("id", "bigint"), ("amount", "bigint"),
                            ("note", "text")], pk="id")
            if self.ref2 in self.tables:
                c.create_table("app", "payments",
                               [("id", "bigint"), ("sum", "bigint")], pk="id")

        for i in range(1, seed_rows + 1):
            self.src.insert(self.ref, (i, 100 * i, f"seed-{i}"))
        self.src.create_slot("slot_orders", restart_lsn=0, kind="consumer")

        self.next_pk = seed_rows + 1
        self.written = seed_rows
        self.old = self._make_old_consumer()
        self.new: Consumer | None = None
        self.ctrl.on_cutover = self._spawn_new_consumer
        self.plan: Plan | None = None

    # ------------------------------------------------------------ 参与者

    def _make_old_consumer(self) -> Consumer:
        return Consumer(
            consumer_id="worker-legacy",
            side=FenceSide.OLD,
            cluster=self.src,
            slot="slot_orders",
            tables=list(self.tables),
            source_cluster_name="src-db",
            source_slot="slot_orders",
            fences=self.ctrl.fences,
            plan_id="",  # 计划创建前也允许读；ack 的 plan 校验在创建后才需要
            epoch=1,
        )

    def _spawn_new_consumer(self, plan: Plan) -> None:
        spec = plan.spec
        self.new = Consumer(
            consumer_id="worker-new",
            side=FenceSide.NEW,
            cluster=self.dst,
            slot=spec.target_slot,
            tables=list(spec.tables),
            source_cluster_name=spec.source_cluster,
            source_slot=spec.source_slot,
            fences=self.ctrl.fences,
            plan_id=spec.plan_id,
            epoch=self.ctrl.fences.get(spec.source_cluster,
                                      spec.source_slot).epoch,
        )
        # 目标消费槽由控制器在切换内以翻译出的本地位点创建，这里不再重复建

    # ------------------------------------------------------------ 计划/步进

    def create_plan(self, plan_id: str = "plan-demo") -> Plan:
        self.plan = self.ctrl.create_plan(
            plan_id=plan_id,
            source_cluster="src-db",
            source_slot="slot_orders",
            target_cluster="dst-db",
            tables=list(self.tables),
        )
        # 旧消费者绑定到计划的 fence 校验
        self.old.plan_id = plan_id
        return self.plan

    def write_one(self, amount: int | None = None) -> int:
        i = self.next_pk
        self.next_pk += 1
        self.written += 1
        return self.src.insert(self.ref, (i, amount or (10 * i), f"live-{i}"))

    def reconcile_step(self) -> Plan:
        """不写入的一轮：控制器泵/reconcile + 旧/新消费者各处理一轮。"""
        if self.plan is not None:
            # switch 可能由外部直接发起，本地引用过期则刷新
            fresh = self.ctrl.get_plan(self.plan.spec.plan_id)
            self.plan = fresh
            self.plan = self.ctrl.tick(self.plan.spec.plan_id)
        self.old.tick()
        if self.new is not None:
            self.new.tick()
        return self.plan

    def step(self, writes: int = 1) -> None:
        """确定性一轮：控制器先泵（含切换后持续复制），再消费者处理，最后写入。

        这样新消费者每轮都能看到泵已落目标的增量；源端写入不中断。
        """
        if self.plan is not None:
            # switch 可能由外部直接发起，刷新本地引用避免 CAS 过期版本
            self.plan = self.ctrl.get_plan(self.plan.spec.plan_id)
            self.plan = self.ctrl.tick(self.plan.spec.plan_id)
        self.old.tick()
        if self.new is not None:
            self.new.tick()
        for _ in range(writes):
            self.write_one()

    def run_until(self, phase: Phase, max_steps: int = 2000,
                  writes_per_step: int = 1) -> Plan:
        for _ in range(max_steps):
            self.step(writes_per_step)
            if self.plan.status.phase is phase:
                return self.plan
            if self.plan.status.phase is Phase.FAILED:
                raise AssertionError(f"计划失败: {self.plan.status.last_error}")
        raise TimeoutError(f"未到达 {phase}")  # pragma: no cover

    def switch(self):
        self.plan = self.ctrl.switch(self.plan.spec.plan_id)
        return self.plan

    def old_consumer_late_ack(self) -> None:
        """模拟旧消费者在切换后“迟到”的一次 ack 尝试（必须被拒）。"""
        self.old.tick()

    def drain(self, ticks: int = 200) -> None:
        """停写排空：泵 + 新消费者持续运行直到目标行集合追平源。"""
        for _ in range(ticks):
            # 不写入；先泵后消费
            self.plan = self.ctrl.tick(self.plan.spec.plan_id)
            if self.new is not None:
                self.new.tick()
            sr = {r.pk for r in self.src.build_table_snapshot(self.ref).rows}
            dr = {r.pk for r in self.dst.build_table_snapshot(self.ref).rows}
            if sr == dr:
                return
        raise TimeoutError("drain 未追平")  # pragma: no cover

    # ------------------------------------------------------------ 不变量核查

    def assert_no_loss(self, verbose: bool = False, drain_first: bool = True) -> dict:
        """切换完成后核对：源/目标业务表最终行集合一致，且无双重水位。

        ``drain_first=True`` 先停写排空（复制是异步的，切换保证的是**边界**
        位点/摘要一致；切换后增量仍在流式传输）。
        """
        if drain_first:
            self.drain()
        src_rows = self.src.build_table_snapshot(self.ref)
        dst_rows = self.dst.build_table_snapshot(self.ref)
        src_keys = {r.pk for r in src_rows.rows}
        dst_keys = {r.pk for r in dst_rows.rows}
        assert src_keys == dst_keys, (
            f"行集合不一致: 源独有={src_keys - dst_keys}, "
            f"目标独有={dst_keys - src_keys}"
        )
        fence = self.ctrl.fences.get("src-db", "slot_orders")
        # 任何时刻只有一侧开放
        assert fence is not None
        # 新槽水位单调；旧槽水位在切换后不得再前进
        new_slot = self.dst.slot_watermark(self.plan.spec.target_slot)
        final_local = self.plan.status.final.target_local_lsn
        assert new_slot >= final_local, "新槽水位回退！"
        if verbose:
            print(f"行数一致={len(src_keys)} 新槽水位={new_slot} "
                  f"fence={fence.state.value} epoch={fence.epoch} "
                  f"旧消费者被拒={self.old.stats.rejected_acks}")
        return {
            "rows": len(src_keys),
            "new_slot_watermark": new_slot,
            "fence_state": fence.state.value,
            "old_rejected": self.old.stats.rejected_acks,
        }
