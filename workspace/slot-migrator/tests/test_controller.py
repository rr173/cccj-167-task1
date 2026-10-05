"""协调器与不变量测试（无第三方依赖，python -m unittest）。"""
from __future__ import annotations

import copy
import unittest

from slotmigrator import (
    CASStore,
    Consumer,
    Controller,
    FenceSide,
    FenceStore,
    FenceError,
    GateFailure,
    Harness,
    MemCluster,
    Phase,
    PlanRejected,
    PumpFault,
    TableRef,
    verify_proof,
)
from slotmigrator.controller import plan_fingerprint
from slotmigrator.memcluster import LSNRegression
from slotmigrator.proof import ProofError


ORDERS = TableRef("app", "orders")


def drive(h: Harness, until: Phase, n: int = 5000, writes: int = 1):
    for i in range(n):
        h.step(writes)
        if h.plan.status.phase is until:
            return i
    raise AssertionError(f"未到达 {until}, 当前 {h.plan.status.phase}: "
                         f"{h.plan.status.last_error}")


class HappyPathTests(unittest.TestCase):
    def test_full_migration_continuous_writes_no_loss(self):
        h = Harness(seed_rows=10)
        plan = h.create_plan("p1")
        start = plan.spec.start_lsn
        drive(h, Phase.READY)
        switched = h.switch()
        self.assertIs(switched.status.phase, Phase.SWITCHED)

        # 切换边界：新槽起点精确翻译自旧水位
        f = switched.status.final
        self.assertEqual(f.old_watermark_src_lsn,
                         h.src.slot_watermark("slot_orders"))
        mapped = h.dst.origin(plan.spec.subscription).remote_to_local[
            f.old_watermark_src_lsn]
        self.assertEqual(f.target_local_lsn, mapped)
        self.assertGreaterEqual(f.target_remote_lsn, f.old_watermark_src_lsn)
        self.assertGreater(f.old_watermark_src_lsn, start)

        # 切换后继续写入：全部到达目标，新消费者推进新槽
        for _ in range(150):
            h.step()
        info = h.assert_no_loss()
        self.assertEqual(info["fence_state"], "open_new")

    def test_update_and_delete_replay(self):
        h = Harness(seed_rows=3)
        plan = h.create_plan("p-ud")
        drive(h, Phase.READY)
        h.switch()
        # 在源上 update/delete 若干行，排空后目标必须完全一致
        h.src.update(ORDERS, "1", {"amount": 999})
        h.src.delete(ORDERS, "2")
        h.drain()
        sr = {r.pk: r.row for r in h.src.build_table_snapshot(ORDERS).rows}
        dr = {r.pk: r.row for r in h.dst.build_table_snapshot(ORDERS).rows}
        self.assertEqual(sr, dr)
        self.assertEqual(sr["1"][1], 999)
        self.assertNotIn("2", dr)

    def test_gates_all_green_and_frozen_fields(self):
        h = Harness()
        plan = h.create_plan("p2")
        self.assertEqual(plan.spec.tables, tuple(sorted([ORDERS], key=str)))
        drive(h, Phase.READY)
        gates = {g.gate: g for g in h.plan.status.gates}
        self.assertEqual(len(gates), 5)
        self.assertTrue(all(g.ok for g in gates.values()))


class FenceAndConsumerTests(unittest.TestCase):
    def test_late_old_consumer_ack_rejected_forever(self):
        h = Harness()
        h.create_plan("p3")
        drive(h, Phase.READY)
        h.switch()
        for _ in range(40):
            h.step()
        frozen_wm = h.src.slot_watermark("slot_orders")
        rejected_before = h.old.stats.rejected_acks
        # 旧消费者不断重试迟到 ack
        for _ in range(100):
            h.old.tick()
        self.assertEqual(h.src.slot_watermark("slot_orders"), frozen_wm)
        self.assertGreater(h.old.stats.rejected_acks, rejected_before)
        # 源槽水位再写也不会被旧消费者推进
        h.write_one()
        h.old.tick()
        self.assertEqual(h.src.slot_watermark("slot_orders"), frozen_wm)

    def test_no_two_valid_watermarks(self):
        h = Harness()
        h.create_plan("p4")
        drive(h, Phase.READY)
        # READY 时只开旧侧
        f = h.ctrl.fences.get("src-db", "slot_orders")
        self.assertEqual(f.state.value, "open_old")
        self.assertEqual(f.required_side(), FenceSide.OLD)
        h.switch()
        f = h.ctrl.fences.get("src-db", "slot_orders")
        self.assertEqual(f.state.value, "open_new")
        self.assertEqual(f.required_side(), FenceSide.NEW)
        # epoch 严格单调：1 -> 2(sealed) -> 3(open_new)
        self.assertEqual(f.epoch, 3)
        # 新消费者能推进，旧的不能
        for _ in range(30):
            h.step()
        self.assertGreater(h.new.stats.acks, 0)

    def test_sealed_window_rejects_all_acks(self):
        h = Harness()
        h.create_plan("p5")
        drive(h, Phase.READY)
        fence = h.ctrl.fences.get("src-db", "slot_orders")
        h.ctrl.fences.seal(fence)
        with self.assertRaises(FenceError):
            h.ctrl.fences.ack("src-db", "slot_orders", FenceSide.OLD,
                              fence.epoch - 1, "c", 123)
        with self.assertRaises(FenceError):
            # sealed 状态即使拿到 sealed epoch 也不能 ack
            h.ctrl.fences.ack("src-db", "slot_orders", FenceSide.OLD,
                              fence.epoch, "c", 123)

    def test_consumer_buffers_unacked_entries_no_loss(self):
        h = Harness()
        h.create_plan("p6")
        drive(h, Phase.READY)
        h.switch()
        # 旧消费者被拒后保留缓冲；新消费者独立消费目标
        for _ in range(20):
            h.step()
        h.drain()
        self.assertEqual(
            {r.pk for r in h.src.build_table_snapshot(ORDERS).rows},
            {r.pk for r in h.dst.build_table_snapshot(ORDERS).rows},
        )


class SchemaDriftTests(unittest.TestCase):
    def test_ddl_after_freeze_fails_plan_no_cutover(self):
        h = Harness()
        h.create_plan("p7")
        # 源端在迁移期间发生模式变更
        h.src.add_column(ORDERS, "tenant", "text", "t0")
        drive_to = h.ctrl.run_until
        with self.assertRaises(RuntimeError):
            drive_to("p7", Phase.READY, max_ticks=50,
                     between=lambda: None)
        plan = h.ctrl.get_plan("p7")
        self.assertIs(plan.status.phase, Phase.FAILED)
        self.assertIn("模式漂移", plan.status.last_error)
        # 旧侧栅栏保持开放，旧消费者不受影响
        fence = h.ctrl.fences.get("src-db", "slot_orders")
        self.assertEqual(fence.state.value, "open_old")
        with self.assertRaises(GateFailure):
            h.ctrl.switch("p7")

    def test_concurrent_plan_same_slot_rejected(self):
        c = Controller()
        src, dst = MemCluster("s"), MemCluster("d")
        for x in (src, dst):
            x.create_table("app", "orders", [("id", "bigint")], pk="id")
        src.insert(ORDERS, (1,))
        src.create_slot("slot_a")
        c.register_cluster(src); c.register_cluster(dst)
        c.create_plan(plan_id="a", source_cluster="s", source_slot="slot_a",
                      target_cluster="d", tables=[ORDERS])
        with self.assertRaises(PlanRejected):
            c.create_plan(plan_id="b", source_cluster="s", source_slot="slot_a",
                          target_cluster="d", tables=[ORDERS])


class FaultAndRetryTests(unittest.TestCase):
    def test_backfill_fault_retried_idempotently(self):
        h = Harness(seed_rows=6)
        h.create_plan("p8")
        h.dst.faults["backfill_fault"] = 1
        drive(h, Phase.READY)
        done = h.ctrl.get_plan("p8")
        # 回填表标记幂等：最终快照摘要一致
        self.assertTrue(done.status.backfill_check.match)
        h.switch()
        h.drain()
        h.assert_no_loss()

    def test_pump_apply_crash_no_duplicate_apply(self):
        h = Harness(seed_rows=4)
        plan = h.create_plan("p9")
        # 让 pump 在批中崩溃一次：已落盘条目映射保留、水位不前进
        h.dst.faults["pump_apply_fault"] = 1
        drive(h, Phase.READY)
        origin = h.dst.origin(plan.spec.subscription)
        # 每个远端 LSN 只映射一次（没有重复本地 WAL）
        self.assertEqual(len(origin.remote_to_local), origin.last_local_lsn)
        h.switch()
        h.drain()
        h.assert_no_loss()

    def test_pump_fetch_fault_retried(self):
        h = Harness(seed_rows=4)
        h.create_plan("p10")
        h.dst.faults["pump_fetch_fault"] = 2
        drive(h, Phase.READY)
        h.switch()
        h.drain()
        h.assert_no_loss()
        self.assertGreaterEqual(
            h.ctrl.get_plan("p10").status.attempts.get("fault", 0), 2)

    def test_switch_replay_after_open_new_crash(self):
        """open_new 已成功、状态/证明尚未落盘时崩溃 -> 重入安全收尾。"""
        h = Harness(seed_rows=5)
        plan = h.create_plan("p11")
        drive(h, Phase.READY)
        spec = plan.spec

        # 真实序列：seal -> 计划进入 SEALING -> 复验/建槽/open_new，
        # 然后在状态 CAS 为 SWITCHED 之前“崩溃”
        fence = h.ctrl.fences.get("src-db", "slot_orders")
        rev, cur = h.ctrl.store.get("plan/p11")
        fence = h.ctrl.fences.seal(fence)
        plan = h.ctrl._advance(cur, rev, Phase.SEALING)  # noqa: SLF001
        rev += 1
        gates = h.ctrl._sealed_verify(plan, pump=True)  # noqa: SLF001
        wm = h.src.slot_watermark("slot_orders")
        local = h.dst.map_remote_to_local(spec.subscription, wm)
        h.dst.create_consumer_slot(spec.target_slot, local)
        fence = h.ctrl.fences.open_new(fence)
        # 此时栅栏已 OPEN_NEW，但计划仍是 SEALING、无证明（模拟崩溃后重启）
        self.assertIs(h.ctrl.get_plan("p11").status.phase, Phase.SEALING)
        self.assertIsNone(h.ctrl.proof_for("p11"))
        fixed = h.ctrl.switch("p11")  # 走 OPEN_NEW 恢复分支
        self.assertIs(fixed.status.phase, Phase.SWITCHED)
        self.assertEqual(fixed.status.final.target_local_lsn, local)
        self.assertEqual(fence.epoch, 3)
        # 幂等：再次 switch 不会重复迁移栅栏/重复出证明
        again = h.ctrl.switch("p11")
        self.assertIs(again.status.phase, Phase.SWITCHED)
        self.assertEqual(
            h.ctrl.fences.get("src-db", "slot_orders").epoch, 3)
        proof = h.ctrl.proof_for("p11")
        verify_proof(proof, h.ctrl.proof_key)


class DigestAndGateTests(unittest.TestCase):
    def test_corrupt_target_row_blocks_cutover(self):
        h = Harness(seed_rows=5)
        h.create_plan("p12")
        # 在回填落地后、快照摘要比对前篡改目标行：包装 backfill 注入
        orig = h.dst.backfill_table

        def poisoned(sub, ts):
            orig(sub, ts)
            if ts.ref == ORDERS:
                h.dst.corrupt_row(ORDERS, "1", "amount", -1)

        h.dst.backfill_table = poisoned  # type: ignore[assignment]
        with self.assertRaises(AssertionError):
            drive(h, Phase.READY, n=20)
        plan = h.ctrl.get_plan("p12")
        self.assertIs(plan.status.phase, Phase.FAILED)
        self.assertIn("快照校验摘要不一致", plan.status.last_error)

    def test_corrupt_target_during_catchup_full_table_gate_blocks(self):
        h = Harness(seed_rows=3)
        h.create_plan("p13")
        drive(h, Phase.READY)
        # READY 后静默篡改目标一行（源/目标 WAL 链相同，只有全表对账能发现）；
        # 切换窗口 G6 必须失败并回退栅栏
        h.dst.corrupt_row(ORDERS, "1", "amount", -1)
        with self.assertRaises(GateFailure):
            h.ctrl.switch("p13")
        plan = h.ctrl.get_plan("p13")
        self.assertIs(plan.status.phase, Phase.READY)  # 回退，未切换
        fence = h.ctrl.fences.get("src-db", "slot_orders")
        self.assertEqual(fence.state.value, "open_old")  # 旧侧重新开放
        self.assertIn("G6_full_table_digest", plan.status.last_error)

    def test_lsn_regression_rejected_at_storage(self):
        c = MemCluster("x")
        c.create_table("app", "orders", [("id", "bigint")], pk="id")
        c.create_slot("s", restart_lsn=0)
        c.insert(ORDERS, (1,)); c.insert(ORDERS, (2,))
        c.advance_slot("s", 2)
        with self.assertRaises(LSNRegression):
            c.advance_slot("s", 1)
        self.assertEqual(c.slot_watermark("s"), 2)


class ProofTests(unittest.TestCase):
    def test_proof_verifies_and_tamper_detected(self):
        h = Harness()
        p = h.create_plan("p14")
        drive(h, Phase.READY)
        h.switch()
        for _ in range(20):
            h.step()
        proof = h.ctrl.proof_for("p14")
        checks = verify_proof(proof, h.ctrl.proof_key)
        self.assertTrue(all(checks.values()))

        # 篡改 final 中的位点 -> 签名失败
        tampered = copy.deepcopy(proof.to_dict())
        tampered["final"]["target_local_lsn"] = 999999
        with self.assertRaises(ProofError):
            verify_proof(tampered, h.ctrl.proof_key)

        # 错误密钥 -> 签名失败
        with self.assertRaises(ProofError):
            verify_proof(proof, b"wrong-key")

    def test_fingerprint_immutable_after_freeze(self):
        h = Harness()
        p = h.create_plan("p15")
        fp1 = plan_fingerprint(p.spec)
        drive(h, Phase.READY)
        h.switch()
        p2 = h.ctrl.get_plan("p15")
        self.assertEqual(plan_fingerprint(p2.spec), fp1)
        self.assertEqual(p2.status.final.plan_fingerprint, fp1)


class CASStoreTests(unittest.TestCase):
    def test_cas_conflict(self):
        store = CASStore()
        store.put("k", "v1")
        rev, v = store.get("k")
        self.assertEqual(rev, 1)
        store.cas("k", rev, "v2")
        from slotmigrator import CASConflict
        with self.assertRaises(CASConflict):
            store.cas("k", rev, "v3-lost-update")
        self.assertEqual(store.get("k")[1], "v2")


if __name__ == "__main__":
    unittest.main(verbosity=2)
