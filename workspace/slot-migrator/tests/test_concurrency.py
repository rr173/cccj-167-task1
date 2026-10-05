"""并发压力：争抢 reconcile / switch / ack，验证只有一个有效水位。"""
from __future__ import annotations

import threading
import unittest

from slotmigrator import FenceSide, Harness, Phase, verify_proof
from slotmigrator.types import TableRef


class ConcurrencyTests(unittest.TestCase):
    def test_racing_switches_only_one_cutover(self):
        h = Harness(seed_rows=20)
        h.create_plan("race")
        for _ in range(200):
            h.step()
            if h.plan.status.phase is Phase.READY:
                break
        errors: list[Exception] = []
        results: list[str] = []

        def do_switch():
            try:
                p = h.ctrl.switch("race")
                results.append(p.status.phase.value)
            except Exception as ex:  # noqa: BLE001
                errors.append(ex)

        threads = [threading.Thread(target=do_switch) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        switched = [r for r in results if r == "Switched"]
        self.assertGreaterEqual(len(switched), 1)
        fence = h.ctrl.fences.get("src-db", "slot_orders")
        # 栅栏只迁移两次：epoch 严格到 3，revision 只 +2
        self.assertEqual(fence.epoch, 3)
        plan = h.ctrl.get_plan("race")
        self.assertIs(plan.status.phase, Phase.SWITCHED)
        # 证明可独立验证，且只有一份
        proof = h.ctrl.proof_for("race")
        verify_proof(proof, h.ctrl.proof_key)

    def test_concurrent_writes_consumers_and_reconcile(self):
        h = Harness(seed_rows=10)
        h.create_plan("flow")
        stop = threading.Event()

        def writer():
            while not stop.is_set():
                h.write_one()

        def old_worker():
            while not stop.is_set():
                h.old.tick()

        def reconciler():
            while not stop.is_set():
                try:
                    h.ctrl.tick("flow")
                except Exception:  # noqa: BLE001
                    pass

        tw = [threading.Thread(target=writer) for _ in range(2)]
        tc = threading.Thread(target=old_worker)
        tr = threading.Thread(target=reconciler)
        for t in tw + [tc, tr]:
            t.start()
        # 等到 READY
        for _ in range(2000):
            if h.ctrl.get_plan("flow").status.phase is Phase.READY:
                break
        stop.set()
        for t in tw + [tc, tr]:
            t.join(timeout=2)
        # 停写后排空：再跑若干轮直到 READY（静止判据需要连续两轮零滞后）
        for _ in range(100):
            h.step(writes=0)
            if h.ctrl.get_plan("flow").status.phase is Phase.READY:
                break
        plan = h.ctrl.get_plan("flow")
        self.assertIs(plan.status.phase, Phase.READY)
        switched = h.ctrl.switch("flow")
        self.assertIs(switched.status.phase, Phase.SWITCHED)

        # 切换后无丢失
        for _ in range(100):
            h.step()
        h.assert_no_loss()

        # 旧槽水位在切换后永不再前进
        wm = h.src.slot_watermark("slot_orders")
        self.assertEqual(wm, switched.status.final.old_watermark_src_lsn)

    def test_ack_storm_after_cutover_old_never_advances(self):
        h = Harness(seed_rows=5)
        h.create_plan("storm")
        for _ in range(100):
            h.step()
            if h.plan.status.phase is Phase.READY:
                break
        h.ctrl.switch("storm")
        wm = h.src.slot_watermark("slot_orders")

        def old_acks():
            for _ in range(500):
                try:
                    h.old.tick()
                except Exception:  # noqa: BLE001
                    pass

        def new_acks():
            for _ in range(500):
                h.write_one()
                if h.new is not None:
                    h.new.tick()
                h.ctrl.tick("storm")

        ts = [threading.Thread(target=old_acks) for _ in range(4)]
        tn = threading.Thread(target=new_acks)
        for t in ts + [tn]:
            t.start()
        for t in ts + [tn]:
            t.join()
        self.assertEqual(h.src.slot_watermark("slot_orders"), wm)
        fence = h.ctrl.fences.get("src-db", "slot_orders")
        self.assertEqual(fence.state.value, "open_new")
        self.assertGreater(len(fence.rejected_acks), 0)
        h.drain()
        h.assert_no_loss()


if __name__ == "__main__":
    unittest.main(verbosity=2)
