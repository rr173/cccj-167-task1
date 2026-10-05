"""确定性哈希与规范序列化。

源端和目标端必须对“同一份数据”得到**字节完全相同**的摘要，因此所有哈希都基于
:func:`canonical_json`（键排序、无空白、固定分隔符），绝不能依赖 dict 插入顺序
或 ``str()`` 的临时表示。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable, Mapping, Sequence

from .types import LSN, TableRef, TableSchema, TableSnapshot, WALEntry


def canonical_json(obj: Any) -> str:
    """跨端一致的 JSON 文本：键排序、无多余空白、元组视同列表。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=_default)


def _default(o: Any) -> Any:  # pragma: no cover - 防御性
    if isinstance(o, (set, frozenset)):
        return sorted(o)
    if hasattr(o, "__fspath__"):
        return str(o)
    raise TypeError(f"not canonical-json serializable: {type(o)!r}")


def sha256_hex(data: str | bytes) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def digest_payload(payload: Any) -> str:
    return sha256_hex(canonical_json(payload))


def lsn_str(lsn: LSN) -> str:
    """格式化为 PostgreSQL 风格 ``hi/lo``（32 位分界），纯展示用。"""
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:08X}"


def digest_tables_schema(tables: Mapping[TableRef, TableSchema]) -> str:
    payload = [tables[k].digest_payload() for k in sorted(tables, key=lambda t: t.fqn)]
    return digest_payload({"kind": "schema", "tables": payload})


def row_hash(columns: Sequence[tuple[str, str]], row: tuple[Any, ...]) -> str:
    payload = {name: _norm(v) for (name, _t), v in zip(columns, row)}
    return digest_payload({"kind": "row", "row": payload})


def snapshot_digest(tables: Iterable[TableSnapshot]) -> str:
    """一致快照摘要：每张表的列定义 + 按主键排序的 (pk, 行哈希) 链。

    用前一个块的哈希参与下一块（链式），避免集合重排之外的顺序类问题，
    同时单行损坏即可定位。
    """
    parts: list[dict[str, Any]] = []
    for ts in sorted(tables, key=lambda t: t.ref.fqn):
        prev = ""
        chain: list[dict[str, str]] = []
        for img in sorted(ts.rows, key=lambda r: r.pk):
            h = row_hash(ts.columns, img.row)
            block = digest_payload({"pk": img.pk, "row_hash": h, "prev": prev})
            chain.append({"pk": img.pk, "block": block})
            prev = block
        parts.append(
            {
                "table": ts.ref.fqn,
                "columns": [list(c) for c in ts.columns],
                "rows": chain,
            }
        )
    return digest_payload({"kind": "snapshot", "tables": parts})


def journal_digest(
    entries: Sequence[WALEntry],
    schema_lookup: Mapping[TableRef, Sequence[tuple[str, str]]],
    bounds: tuple[int | None, int | None] | None = None,
) -> str:
    """增量 WAL 段摘要：对每条 entry 的规范负载做链式哈希。

    源与目标对**同一条逻辑变更**（origin LSN、表、前后镜像、操作类型）必须算出
    相同字节，否则说明目标回放结果与源不一致（丢事件/重复应用/错应用）。
    ``bounds`` 显式给定逻辑位点边界——目标端条目本地 LSN 不同，必须传入与源
    一致的 origin 位点边界。
    """
    prev = ""
    blocks: list[str] = []
    for e in entries:
        cols = schema_lookup.get(e.ref)
        block = digest_payload(
            {"block": e.to_payload(cols), "prev": prev}
        )
        blocks.append(block)
        prev = block
    if entries:
        start_lsn = bounds[0] if bounds else (entries[0].origin_lsn or entries[0].lsn)
        end_lsn = bounds[1] if bounds else (entries[-1].origin_lsn or entries[-1].lsn)
    else:
        start_lsn = end_lsn = None
    return digest_payload({"kind": "journal", "start_lsn": start_lsn,
                           "end_lsn": end_lsn,
                           "blocks": blocks})


def _norm(v: Any) -> Any:
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return str(v)
