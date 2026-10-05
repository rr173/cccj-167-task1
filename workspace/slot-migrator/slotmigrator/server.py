"""HTTP 计划 API + 位点对账页面 + 切换证明页面（仅标准库）。

端点
====
* ``GET  /api/clusters``                       集群与表
* ``POST /api/plans``                          创建冻结迁移计划
* ``GET  /api/plans``                          计划列表
* ``GET  /api/plans/{id}``                     计划详情
* ``GET  /api/plans/{id}/reconcile``           位点对账（JSON）
* ``POST /api/plans/{id}/reconcile``           推进一轮 reconcile
* ``POST /api/plans/{id}/switch``              原子切换
* ``GET  /api/plans/{id}/proof``               切换证明（JSON）
* ``POST /api/write``                          演示：源端持续写入
* ``GET  /reconcile/{id}``                     对账页面（HTML）
* ``GET  /proof/{id}``                         切换证明页面（HTML）
"""
from __future__ import annotations

import html
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from .controller import GateFailure, PlanRejected
from .harness import Harness
from .proof import verify_proof
from .types import Phase, TableRef
from .digest import lsn_str


class Server:
    def __init__(self, host: str = "127.0.0.1", port: int = 8080) -> None:
        self.host = host
        self.port = port
        self.harness = Harness(seed_rows=8)
        self.ctrl = self.harness.ctrl
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._auto_id = 0

    # ------------------------------------------------------------ 生命周期

    def start(self) -> str:
        handler = self._make_handler()
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler)
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, daemon=True
        )
        self._thread.start()
        return f"http://{self.host}:{self._httpd.server_port}"

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()

    def auto_tick(self) -> None:
        """供演示线程周期调用：推进计划/复制（源端继续写入不中断）。"""
        h = self.harness
        if h.plan is None:
            return
        try:
            h.step(writes=1)
        except Exception:  # noqa: BLE001 - 演示线程不能因单轮故障退出
            pass

    # ------------------------------------------------------------ 业务处理

    def _create_plan(self, body: dict[str, Any]) -> dict[str, Any]:
        tables = [TableRef(*t.split(".", 1)) for t in body.get(
            "tables", ["app.orders"])]
        self._auto_id += 1
        pid = body.get("plan_id") or f"plan-{self._auto_id:03d}"
        plan = self.ctrl.create_plan(
            plan_id=pid,
            source_cluster="src-db",
            source_slot="slot_orders",
            target_cluster="dst-db",
            tables=tables,
        )
        self.harness.plan = plan
        self.harness.old.plan_id = pid
        return self._plan_json(plan)

    def _plan_json(self, plan) -> dict[str, Any]:
        spec, st = plan.spec, plan.status
        return {
            "plan_id": spec.plan_id,
            "phase": st.phase.value,
            "revision": st.revision,
            "frozen": {
                "source_cluster": spec.source_cluster,
                "source_slot": spec.source_slot,
                "target_cluster": spec.target_cluster,
                "subscription": spec.subscription,
                "target_slot": spec.target_slot,
                "tables": [t.fqn for t in spec.tables],
                "start_lsn": spec.start_lsn,
                "start_lsn_text": lsn_str(spec.start_lsn),
                "schema_version": spec.schema_version.version,
                "schema_digest": spec.schema_version.digest,
                "consumer_watermark_at_plan": spec.consumer_watermark_at_plan,
            },
            "last_error": st.last_error,
            "proof_id": st.proof_id,
        }

    def _switch(self, plan_id: str) -> dict[str, Any]:
        plan = self.ctrl.switch(plan_id)
        return {"phase": plan.status.phase.value,
                "proof_id": plan.status.proof_id,
                "final": plan.status.final and {
                    "old_watermark": plan.status.final.old_watermark_src_lsn,
                    "target_remote": plan.status.final.target_remote_lsn,
                    "target_local": plan.status.final.target_local_lsn,
                }}

    # ------------------------------------------------------------ HTTP

    def _make_handler(self):
        srv = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):  # 安静日志
                pass

            def _send(self, code: int, obj: Any, ctype: str = "application/json"):
                if ctype == "application/json":
                    data = json.dumps(obj, ensure_ascii=False, indent=2).encode()
                else:
                    data = obj.encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", f"{ctype}; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _body(self) -> dict[str, Any]:
                n = int(self.headers.get("Content-Length", 0))
                if not n:
                    return {}
                raw = self.rfile.read(n)
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return {}

            def do_GET(self):  # noqa: N802
                path = urlparse(self.path).path
                try:
                    if path == "/api/clusters":
                        return self._send(200, srv._clusters_json())
                    if path == "/api/plans":
                        return self._send(200, [srv._plan_json(p)
                                               for p in srv.ctrl.list_plans()])
                    if path.startswith("/api/plans/"):
                        pid, tail = _split_tail(path, "/api/plans/")
                        if tail == "":
                            return self._send(200, srv._plan_json(
                                srv.ctrl.get_plan(pid)))
                        if tail == "reconcile":
                            return self._send(200, srv.ctrl.reconcile_view(pid))
                        if tail == "proof":
                            proof = srv.ctrl.proof_for(pid)
                            if proof is None:
                                return self._send(404, {"error": "尚未切换，无证明"})
                            return self._send(200, proof.to_dict())
                    if path.startswith("/reconcile/"):
                        pid = path[len("/reconcile/"):]
                        return self._send(200, render_reconcile(
                            srv.ctrl.reconcile_view(pid)), "text/html")
                    if path.startswith("/proof/"):
                        pid = path[len("/proof/"):]
                        proof = srv.ctrl.proof_for(pid)
                        if proof is None:
                            return self._send(404, {"error": "尚未切换，无证明"})
                        try:
                            checks = verify_proof(proof, srv.ctrl.proof_key)
                        except Exception as ex:  # noqa: BLE001
                            checks = {"error": str(ex)}
                        return self._send(200, render_proof(proof, checks),
                                          "text/html")
                    if path in ("/", "/index.html"):
                        return self._send(200, render_index(srv), "text/html")
                    self._send(404, {"error": "not found"})
                except KeyError:
                    self._send(404, {"error": "plan not found"})
                except Exception as ex:  # noqa: BLE001
                    self._send(500, {"error": str(ex)})

            def do_POST(self):  # noqa: N802
                path = urlparse(self.path).path
                body = self._body()
                try:
                    if path == "/api/plans":
                        return self._send(201, srv._create_plan(body))
                    if path == "/api/write":
                        lsn = srv.harness.write_one()
                        return self._send(200, {"lsn": lsn})
                    if path.startswith("/api/plans/"):
                        pid, tail = _split_tail(path, "/api/plans/")
                        if tail == "reconcile":
                            plan = srv.harness.reconcile_step()
                            return self._send(200, srv._plan_json(plan))
                        if tail == "switch":
                            return self._send(200, srv._switch(pid))
                    self._send(404, {"error": "not found"})
                except (GateFailure, PlanRejected) as ex:
                    self._send(409, {"error": str(ex)})
                except KeyError:
                    self._send(404, {"error": "plan not found"})
                except Exception as ex:  # noqa: BLE001
                    self._send(500, {"error": str(ex)})

        return Handler

    def _clusters_json(self) -> dict[str, Any]:
        h = self.harness
        src_lsn = h.src.current_lsn()
        dst_applied = None
        if h.plan is not None:
            dst_applied = h.dst.origin(
                h.plan.spec.subscription).applied_remote_lsn
        return {
            "source": {
                "name": h.src.name, "insert_lsn": src_lsn,
                "insert_lsn_text": lsn_str(src_lsn),
                "tables": [str(t) for t in h.tables],
            },
            "target": {
                "name": h.dst.name,
                "applied_remote_lsn": dst_applied,
            },
            "written_rows": h.written,
        }


def _split_tail(path: str, prefix: str) -> tuple[str, str]:
    """解析 ``/api/plans/{pid}[/{tail}]``，返回 (pid, tail)。"""
    rest = path[len(prefix):].strip("/")
    if "/" in rest:
        pid, tail = rest.split("/", 1)
    else:
        pid, tail = rest, ""
    return pid, tail


# ================================================================ HTML 页面

_CSS = """
body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#0f1420;color:#e6e9ef}
.wrap{max-width:1080px;margin:0 auto;padding:24px}
h1{font-size:20px} h2{font-size:15px;color:#9fb0c8;margin-top:28px}
.card{background:#171e2e;border:1px solid #26314a;border-radius:10px;padding:16px;margin:12px 0}
table{border-collapse:collapse;width:100%;font-size:13px}
td,th{border-bottom:1px solid #26314a;padding:7px 9px;text-align:left;vertical-align:top}
th{color:#9fb0c8;font-weight:600}
.ok{color:#4ade80;font-weight:700} .bad{color:#f87171;font-weight:700}
.mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;color:#a5b4cf}
.pill{display:inline-block;padding:2px 10px;border-radius:999px;background:#26314a;font-size:12px}
a{color:#7cc4ff}
code{background:#0c111c;padding:2px 6px;border-radius:5px}
"""


def _esc(x: Any) -> str:
    return html.escape(str(x))


def _gate_rows(view: dict[str, Any]) -> str:
    rows = []
    for g in view.get("gates", []):
        cls = "ok" if g["ok"] else "bad"
        mark = "✔ 一致/满足" if g["ok"] else "✗ 未满足"
        rows.append(
            f"<tr><td class=mono>{_esc(g['gate'])}</td>"
            f"<td class='{cls}'>{mark}</td>"
            f"<td>{_esc(g['detail'])}</td>"
            f"<td class=mono>obs={_esc(g.get('observed'))} "
            f"exp={_esc(g.get('expected'))}</td></tr>"
        )
    return "".join(rows) or "<tr><td colspan=4>暂无闸门结果（等待 reconcile）</td></tr>"


def render_reconcile(view: dict[str, Any]) -> str:
    f, p = view["frozen"], view["positions"]
    fence = view.get("fence") or {}
    rejected = fence.get("rejected_acks", [])
    rej_rows = "".join(
        f"<tr><td class=mono>{_esc(r['consumer'])}</td>"
        f"<td class=mono>{_esc(r['side'])}</td>"
        f"<td class=mono>epoch {_esc(r['epoch'])}/{_esc(r['current_epoch'])}</td>"
        f"<td class=mono>LSN {_esc(r['lsn'])}</td>"
        f"<td>{_esc(r['reason'])}</td></tr>"
        for r in rejected[-20:]
    ) or "<tr><td colspan=5>无被拒确认</td></tr>"
    return f"""<!doctype html><html><head><meta charset=utf-8>
<title>位点对账 · {_esc(view['plan_id'])}</title><style>{_CSS}</style></head>
<body><div class=wrap>
<h1>位点对账 — <code>{_esc(view['plan_id'])}</code>
<span class=pill>{_esc(view['phase'])}</span> rev {view['revision']}</h1>

<h2>冻结计划</h2>
<div class=card><table>
<tr><th>源集群/槽</th><td class=mono>{_esc(f['source_cluster'])}/{_esc(f['source_slot'])}</td></tr>
<tr><th>目标集群/订阅/新槽</th><td class=mono>{_esc(f['target_cluster'])} · {_esc(f['subscription'])} · {_esc(f['target_slot'])}</td></tr>
<tr><th>冻结表集合</th><td class=mono>{_esc(', '.join(f['tables']))}</td></tr>
<tr><th>计划起点 start_lsn</th><td class=mono>{_esc(f['start_lsn_text'])} ({f['start_lsn']})</td></tr>
<tr><th>冻结模式版本</th><td class=mono>v{f['schema_version']} digest={_esc(f['schema_digest'][:16])}…</td></tr>
<tr><th>计划指纹</th><td class=mono>{_esc(f['fingerprint'])}</td></tr>
</table></div>

<h2>切换闸门（位点追平 + 校验摘要）</h2>
<div class=card><table>
<tr><th>闸门</th><th>结果</th><th>说明</th><th>观测/期望</th></tr>
{_gate_rows(view)}
</table></div>

<h2>双端位点</h2>
<div class=card><table>
<tr><th>源当前插入位点</th><td class=mono>{_esc(p['source_insert_lsn_text'])}</td></tr>
<tr><th>旧槽（消费者）水位</th><td class=mono>{_esc(p['old_slot_watermark_text'])}</td></tr>
<tr><th>目标订阅槽水位（源侧确认）</th><td class=mono>{_esc(p['subscription_slot_watermark'])}</td></tr>
<tr><th>目标回放远端位点</th><td class=mono>{_esc(p['target_applied_remote_lsn_text'])}</td></tr>
<tr><th>目标 -> 源 滞后条目</th><td class=mono>{_esc(p['lag_entries'])}</td></tr>
<tr><th>新目标消费槽水位</th><td class=mono>{_esc(p['new_target_slot_watermark'])}</td></tr>
<tr><th>快照摘要</th><td class=mono>{_esc(view.get('snapshot_digest'))}</td></tr>
</table></div>

<h2>栅栏（同一时刻只允许一个有效水位）</h2>
<div class=card><table>
<tr><th>状态</th><th>epoch</th><th>revision</th></tr>
<tr><td class=mono>{_esc(fence.get('state'))}</td>
<td class=mono>{_esc(fence.get('epoch'))}</td>
<td class=mono>{_esc(fence.get('revision'))}</td></tr>
</table></div>

<h2>被拒绝的消费者确认（迟到/切换窗口）</h2>
<div class=card><table>
<tr><th>消费者</th><th>侧</th><th>epoch</th><th>位点</th><th>拒绝原因</th></tr>
{rej_rows}
</table></div>
<p class=mono>{_esc(view.get('last_error') or '')}</p>
<p><a href="/proof/{_esc(view['plan_id'])}">查看切换证明 →</a></p>
</div></body></html>"""


def render_proof(proof, checks) -> str:
    d = proof.to_dict()
    fin = d["final"]
    gate_rows = "".join(
        f"<tr><td class=mono>{_esc(g['gate'])}</td>"
        f"<td class='{ 'ok' if g['ok'] else 'bad'}'>"
        f"{'✔' if g['ok'] else '✗'}</td>"
        f"<td>{_esc(g['detail'])}</td></tr>"
        for g in d["gates"]
    )
    inv = "".join(f"<li class=mono>{_esc(x)}</li>" for x in d["invariants"])
    check_rows = "".join(
        f"<tr><td class=mono>{_esc(k)}</td>"
        f"<td class='{'ok' if v is True else 'bad'}'>{_esc(v)}</td></tr>"
        for k, v in checks.items()
    )
    return f"""<!doctype html><html><head><meta charset=utf-8>
<title>切换证明 · {_esc(d['proof_id'])}</title><style>{_CSS}</style></head>
<body><div class=wrap>
<h1>切换证明 — <code>{_esc(d['proof_id'])}</code></h1>

<h2>最终对账事实（密封窗口内冻结）</h2>
<div class=card><table>
<tr><th>旧消费者最终水位（源 LSN）</th><td class=mono>{_esc(lsn_str(fin['old_watermark_src_lsn']))} ({fin['old_watermark_src_lsn']})</td></tr>
<tr><th>目标确认远端位点</th><td class=mono>{_esc(lsn_str(fin['target_remote_lsn']))} ({fin['target_remote_lsn']})</td></tr>
<tr><th>新槽起点（目标本地 LSN）</th><td class=mono>{_esc(lsn_str(fin['target_local_lsn']))} ({fin['target_local_lsn']})</td></tr>
<tr><th>源当前位点</th><td class=mono>{_esc(lsn_str(fin['source_insert_lsn']))}</td></tr>
<tr><th>快照摘要</th><td class=mono>{_esc(fin['snapshot_digest'])}</td></tr>
<tr><th>增量摘要（start..旧水位）</th><td class=mono>{_esc(fin['journal_digest_from_start'])}</td></tr>
<tr><th>模式摘要</th><td class=mono>v{fin['schema_version']['version']} {_esc(fin['schema_version']['digest'][:20])}…</td></tr>
<tr><th>栅栏 revision</th><td class=mono>{fin['fence_revision_before']} → {fin['fence_revision_after']}（sealed epoch {fin['sealed_epoch']}）</td></tr>
<tr><th>计划指纹</th><td class=mono>{_esc(fin['plan_fingerprint'])}</td></tr>
</table></div>

<h2>闸门复验结果</h2><div class=card><table>
<tr><th>闸门</th><th>结果</th><th>说明</th></tr>{gate_rows}</table></div>

<h2>不变量断言</h2><div class=card><ul>{inv}</ul></div>

<h2>独立验证（签名 + 自洽性）</h2><div class=card><table>
<tr><th>检查项</th><th>结果</th></tr>{check_rows}</table></div>

<h2>密码学证据</h2><div class=card>
<p>摘要 SHA-256：<code>{_esc(d['digest'])}</code></p>
<p>HMAC 签名：<code>{_esc(d['signature'])}</code></p>
<p class=mono>事件数：{len(d['events'])}</p></div>
<p><a href="/reconcile/{_esc(d['plan']['plan_id'])}">← 返回位点对账</a></p>
</div></body></html>"""


def render_index(srv: Server) -> str:
    plans = srv.ctrl.list_plans()
    rows = "".join(
        f"<tr><td><a href='/reconcile/{_esc(p.spec.plan_id)}'>{_esc(p.spec.plan_id)}</a></td>"
        f"<td class=mono>{_esc(p.status.phase.value)}</td>"
        f"<td class=mono>{_esc(lsn_str(p.spec.start_lsn))}</td>"
        f"<td class=mono>{_esc(p.spec.source_slot)} → {_esc(p.spec.target_slot)}</td>"
        f"<td>{'<a href=/proof/' + _esc(p.spec.plan_id) + '>证明</a>' if p.status.phase is Phase.SWITCHED else '—'}</td></tr>"
        for p in plans
    ) or "<tr><td colspan=5>暂无计划。用 POST /api/plans 创建。</td></tr>"
    return f"""<!doctype html><html><head><meta charset=utf-8>
<title>逻辑复制槽迁移控制器</title><style>{_CSS}</style></head>
<body><div class=wrap>
<h1>跨集群逻辑复制槽迁移</h1>
<div class=card><table>
<tr><th>计划</th><th>阶段</th><th>冻结起点</th><th>槽迁移</th><th>证明</th></tr>
{rows}
</table></div>
<h2>API 速览</h2><div class=card mono>
POST /api/plans （body: {{"tables":["app.orders"]}}）<br>
POST /api/plans/{{id}}/reconcile · POST /api/plans/{{id}}/switch<br>
GET  /api/plans/{{id}}/reconcile · GET  /reconcile/{{id}}<br>
GET  /api/plans/{{id}}/proof · GET  /proof/{{id}} · POST /api/write
</div></div></body></html>"""
