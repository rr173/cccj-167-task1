"""命令行入口。

用法
====
* ``python -m slotmigrator.cli demo``  跑一遍带源端持续写入的完整迁移并打印证明
* ``python -m slotmigrator.cli serve [--port 8080]``  启动计划 API/对账/证明服务
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from .digest import lsn_str
from .harness import Harness
from .proof import verify_proof
from .server import Server
from .types import Phase


def run_demo() -> int:
    h = Harness(seed_rows=8)
    plan = h.create_plan("plan-demo")
    print(f"[1] 计划冻结 {plan.spec.plan_id}: start_lsn={lsn_str(plan.spec.start_lsn)}"
          f" 模式=v{plan.spec.schema_version.version}"
          f" 表={[str(t) for t in plan.spec.tables]}")

    # 源端持续写入期间推进 backfill + catchup
    i = 0
    while h.plan.status.phase is not Phase.READY:
        h.step()
        i += 1
        if i > 5000:
            print("未在时限内 READY"); return 1
    rv = h.ctrl.reconcile_view(plan.spec.plan_id)
    print(f"[2] READY（{i} 轮），闸门：")
    for g in rv["gates"]:
        print(f"      {'✔' if g['ok'] else '✗'} {g['gate']}: {g['detail']}")

    print("[3] 原子切换（密封栅栏 -> 追平复验 -> 建新槽 -> 打开新侧）…")
    p = h.switch()
    f = p.status.final
    print(f"      旧水位 {lsn_str(f.old_watermark_src_lsn)}"
          f" -> 新槽本地起点 {lsn_str(f.target_local_lsn)}"
          f"（目标远端 {lsn_str(f.target_remote_lsn)}）")

    # 切换后继续写入：新消费者在目标槽消费；旧消费者仍“迟到”
    for _ in range(60):
        h.step()
    h.assert_no_loss()
    h.old_consumer_late_ack()
    fence = h.ctrl.fences.get("src-db", "slot_orders")
    print(f"[4] 切换后写入追平；旧消费者迟到确认被拒 {h.old.stats.rejected_acks} 次"
          f"（栅栏={fence.state.value} epoch={fence.epoch}）")
    print(f"      新消费者 ack {h.new.stats.acks} 批，"
          f"新槽水位={lsn_str(h.new.cluster.slot_watermark(p.spec.target_slot))}")

    proof = h.ctrl.proof_for(plan.spec.plan_id)
    checks = verify_proof(proof, h.ctrl.proof_key)
    print(f"[5] 切换证明 {proof.proof_id}：{len(checks)} 项独立校验全部通过")
    print("      摘要:", proof.digest)
    print("      签名:", proof.signature[:32], "…")
    print(f"\n对账页面: /reconcile/{plan.spec.plan_id}（serve 模式）")
    return 0


def run_serve(port: int) -> int:
    srv = Server(port=port)
    url = srv.start()
    print(f"API/页面已启动：{url}")
    print("  对账页面 /reconcile/<id> · 证明页面 /proof/<id>")
    try:
        while True:
            srv.auto_tick()
            time.sleep(0.05)
    except KeyboardInterrupt:
        srv.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="slotmigrator")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("demo", help="端到端演示")
    s = sub.add_parser("serve", help="启动 HTTP API + 页面")
    s.add_argument("--port", type=int, default=8080)
    args = ap.parse_args(argv)
    if args.cmd == "demo":
        return run_demo()
    return run_serve(args.port)


if __name__ == "__main__":
    sys.exit(main())
