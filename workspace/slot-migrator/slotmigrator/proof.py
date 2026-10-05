"""切换证明：把“为什么能切换”固化成可独立验证的证据包。

证明内容
========
* 冻结计划指纹（源位点/目标订阅/表集合/模式版本/start_lsn）；
* 切换瞬间的最终对账事实（旧水位、目标远端位点、目标本地位点、源当前位点）；
* 快照摘要与从 start_lsn 起的增量摘要；
* 全部闸门结果（位点追平 + 摘要一致）；
* 栅栏切换前后的 revision 与 sealed/new epoch；
* 事件序列（谁在什么时候推进过什么）。

证明对规范 JSON 做 SHA-256 签名（HMAC 风格，密钥即“控制器证明密钥”，
真实系统可替换为 KMS/非对称签名），并提供独立的 :func:`verify_proof`：
对账页面和审计方无需信任控制器，也能确认证明未被篡改、字段自洽。
"""
from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any

from .digest import canonical_json, lsn_str
from .types import CutoverFinal, GateResult, Plan, PlanSpec


PROOF_VERSION = 1


@dataclass(frozen=True)
class CutoverProof:
    proof_id: str
    version: int
    plan: dict[str, Any]
    final: dict[str, Any]
    gates: list[dict[str, Any]]
    events: list[dict[str, Any]]
    invariants: list[str]
    summary: dict[str, Any]
    digest: str
    signature: str

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False, sort_keys=True)

    def to_dict(self) -> dict[str, Any]:
        return {
            "proof_id": self.proof_id,
            "version": self.version,
            "plan": self.plan,
            "final": self.final,
            "gates": self.gates,
            "events": self.events,
            "invariants": self.invariants,
            "summary": self.summary,
            "digest": self.digest,
            "signature": self.signature,
        }


class ProofError(Exception):
    pass


def _signing_payload(p: dict[str, Any]) -> str:
    unsigned = {k: v for k, v in p.items() if k not in ("digest", "signature")}
    return canonical_json(unsigned)


def build_proof(
    plan: Plan,
    gates: list[GateResult],
    events: list[Any],
    proof_key: bytes,
) -> CutoverProof:
    assert plan.status.final is not None
    final: CutoverFinal = plan.status.final
    spec: PlanSpec = plan.spec

    final_payload = {
        "old_watermark_src_lsn": final.old_watermark_src_lsn,
        "old_watermark_src_lsn_text": lsn_str(final.old_watermark_src_lsn),
        "target_remote_lsn": final.target_remote_lsn,
        "target_local_lsn": final.target_local_lsn,
        "source_insert_lsn": final.source_insert_lsn,
        "snapshot_digest": final.snapshot_digest,
        "full_table_digest": final.full_table_digest,
        "journal_digest_from_start": final.journal_digest_from_start,
        "schema_version": {"version": final.schema_version.version,
                           "digest": final.schema_version.digest},
        "plan_fingerprint": final.plan_fingerprint,
        "fence_revision_before": final.fence_revision_before,
        "fence_revision_after": final.fence_revision_after,
        "sealed_epoch": final.sealed_epoch,
    }

    # 证明中显式列出“可机器检查的不变量断言”
    invariants = [
        "target_remote_lsn >= old_watermark_src_lsn  # 目标确认位点不落后于旧水位",
        "target_local_lsn == map(旧水位)             # 新槽起点精确翻译自旧水位",
        "target_remote_lsn <= source_insert_lsn      # 不可能超前于源",
        "snapshot_digest == target_snapshot_digest   # 快照行镜像一致",
        "full_table_digest(src) == full_table_digest(tgt)  # 切换边界全表对账一致",
        "journal_digest(source) == journal_digest(target)  # 截至旧水位增量段一致",
        "schema_version.digest == plan.schema_version.digest  # 模式未漂移",
        "fence.state: open_old -> sealed -> open_new, epoch 严格递增",
        "旧消费者 epoch < sealed_epoch => 其迟到 ack 必被拒绝",
    ]
    summary = {
        "plan_id": spec.plan_id,
        "source_slot": spec.source_slot,
        "target_slot": spec.target_slot,
        "source_cluster": spec.source_cluster,
        "target_cluster": spec.target_cluster,
        "start_lsn": spec.start_lsn,
        "cutover_lsn_src": final.old_watermark_src_lsn,
        "cutover_lsn_target_local": final.target_local_lsn,
        "gates_passed": len(gates),
        "switched_at": plan.status.switched_at,
    }

    payload = {
        "proof_id": plan.status.proof_id,
        "version": PROOF_VERSION,
        "plan": plan.spec.fingerprint_payload(),
        "final": final_payload,
        "gates": [g.__dict__ for g in gates],
        "events": [{"seq": e.seq, "ts": e.ts, "kind": e.kind,
                    "ref": e.ref, "detail": e.detail} for e in events],
        "invariants": invariants,
        "summary": summary,
    }
    body = _signing_payload(payload)
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    signature = hmac.new(proof_key, body.encode("utf-8"), hashlib.sha256).hexdigest()
    payload["digest"] = digest
    payload["signature"] = signature
    return CutoverProof(**payload)


def verify_proof(proof: CutoverProof | dict[str, Any], proof_key: bytes,
                 replayed_events: list[Any] | None = None) -> dict[str, Any]:
    """独立验证证明。返回检查项结果；任一失败抛 :class:`ProofError`。

    检查：签名、证明摘要、计划指纹一致性、位点自洽、闸门全绿、摘要非空。
    """
    d = proof.to_dict() if isinstance(proof, CutoverProof) else dict(proof)
    checks: dict[str, Any] = {}

    body = _signing_payload(d)
    expect_sig = hmac.new(proof_key, body.encode("utf-8"), hashlib.sha256).hexdigest()
    checks["signature"] = hmac.compare_digest(expect_sig, d.get("signature", ""))
    expect_digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    checks["digest"] = hmac.compare_digest(expect_digest, d.get("digest", ""))

    f = d["final"]
    checks["fingerprint_match"] = (
        f["plan_fingerprint"] == _fingerprint_of_plan(d["plan"])
    )
    checks["lsn_target_not_behind_old"] = (
        f["target_remote_lsn"] >= f["old_watermark_src_lsn"]
    )
    checks["target_local_lsn_present"] = f["target_local_lsn"] >= 0
    checks["lsn_not_ahead_of_source"] = (
        f["target_remote_lsn"] <= f["source_insert_lsn"]
    )
    checks["snapshot_digest_present"] = bool(f["snapshot_digest"])
    checks["full_table_digest_present"] = bool(f.get("full_table_digest"))
    checks["journal_digest_present"] = bool(f["journal_digest_from_start"])
    checks["schema_present"] = bool(f["schema_version"]["digest"])
    checks["fence_epochs_advanced"] = (
        f["fence_revision_after"] > f["fence_revision_before"] and f["sealed_epoch"] >= 1
    )
    gates = d.get("gates", [])
    checks["gates_present"] = len(gates) >= 6
    checks["all_gates_green"] = all(bool(g.get("ok")) for g in gates)
    checks["journal_within_bounds"] = (
        d["plan"]["start_lsn"] <= f["old_watermark_src_lsn"]
    )
    if replayed_events is not None:
        replay_seqs = [e.seq for e in replayed_events]
        proof_seqs = [e["seq"] for e in d.get("events", [])]
        checks["events_prefix_match"] = proof_seqs[:len(proof_seqs)] == replay_seqs[:len(proof_seqs)]

    failed = [k for k, ok in checks.items() if not ok]
    if failed:
        raise ProofError(f"切换证明校验失败: {failed}")
    return checks


def _fingerprint_of_plan(plan_payload: dict[str, Any]) -> str:
    # 与构建端使用同一规范序列化
    return hashlib.sha256(canonical_json(plan_payload).encode("utf-8")).hexdigest()
