# slotmigrator — 跨集群逻辑复制槽迁移控制器

在源集群**持续写入**的情况下，把逻辑复制槽（及消费者）从源集群原子迁移到目标集群，
保证：

- **不丢事件**：先建立一致快照、回填，再流式传输增量；
- **不重复推进**：每步幂等，泵按 LSN 去重，复制槽水位存储层拒绝回退；
- **不会出现两个有效消费水位**：`open_old → sealed → open_new` 栅栏 + 单调
  fencing epoch，旧消费者的迟到确认一律被拒绝并审计；
- **只有目标确认位点与校验摘要都一致才能切换**：5+1 道闸门在**密封窗口内复验**，
  失败自动回退旧槽，绝不强切；
- **切换可证明**：产出带 HMAC 签名的切换证明，审计方可独立重验签名、位点自洽
  和全部闸门结果。

仅依赖 Python 3.10+ 标准库；数据库集群以内存模拟件实现，但保留了与真实
PostgreSQL 逻辑复制一一对应的关键机制（WAL/LSN、confirmed flush、复制原点
origin LSN 映射、一致快照、至少一次投递 + 幂等回放）。

## 快速开始

```bash
# 端到端演示（源端持续写入 → 对账 → 原子切换 → 迟到确认被拒 → 证明）
python3 -m slotmigrator.cli demo

# 计划 API + 位点对账页面 + 切换证明页面
python3 -m slotmigrator.cli serve --port 8080
#   页面:  http://127.0.0.1:8080/
#         http://127.0.0.1:8080/reconcile/<plan-id>
#         http://127.0.0.1:8080/proof/<plan-id>

# 测试
python3 -m unittest discover -s tests
```

## 计划 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/plans` | 创建**冻结**迁移计划（源位点、目标订阅、表集合、模式版本） |
| GET  | `/api/plans` / `/api/plans/{id}` | 计划列表/详情 |
| POST | `/api/plans/{id}/reconcile` | 推进一轮声明式 reconcile（幂等） |
| GET  | `/api/plans/{id}/reconcile` | 位点对账数据（JSON） |
| POST | `/api/plans/{id}/switch` | 原子切换（非 READY 返回 409） |
| GET  | `/api/plans/{id}/proof` | 切换证明（切换前 404） |
| POST | `/api/write` | 演示：源端持续写入 |

创建计划示例：

```json
POST /api/plans
{ "plan_id": "p1", "tables": ["app.orders"] }
```

冻结字段（创建后不可变，并参与计划指纹）：源/目标集群、源槽、目标订阅与新槽、
表集合、模式版本摘要、`start_lsn`（一致快照位点）、创建时旧消费者水位。

## 阶段状态机

```
Pending → Backfilling → CatchingUp → Ready → Sealing → Switched
   任意阶段检测到模式漂移 / 摘要不一致 / 目标损坏 → Failed
   切换窗口闸门复验失败 → 回 Ready（栅栏回退旧侧），可重试
```

- **Backfilling**：以冻结快照位点的行镜像幂等回填目标，比对**快照链式摘要**。
- **CatchingUp**：泵从 `start_lsn` 流式回放增量；每个周期评估闸门，并要求
  「连续两轮零滞后」静止，避免采样瞬间碰巧追平的误判。
- **Sealing**：密封栅栏（旧消费者所有在途 ack 立即失效）→ 全程持源锁排空 →
  在**无主可验证窗口**内复验全部闸门 → 把旧水位翻译为目标本地 LSN 并建新槽 →
  打开新侧栅栏 → 落最终状态与切换证明。

## 切换闸门

| 闸门 | 含义 |
| --- | --- |
| G1 schema_identical | 源模式与冻结计划一致（DDL 漂移立即终止计划） |
| G2 target_applied | 目标回放远端位点 ≥ 旧消费者水位 |
| G3 lsn_caught_up | 排空复检后目标位点 == 源当前位点（+ 静止判据） |
| G4 old_watermark_past_snapshot | 旧消费者已越过快照位点（快照前事件不丢） |
| G5 incremental_digest | 源/目标增量 WAL 段**链式摘要逐字节一致** |
| G6 full_table_digest | 密封窗口内全表行镜像摘要一致（捕获静默物理损坏） |

所有哈希基于规范 JSON（键排序、固定分隔符），目标 WAL 条目携带 origin LSN，
因此两端对同一逻辑变更算出相同字节。

## 正确性机制

- **冻结与指纹**：计划在「先源锁后目标锁」的单个临界区内原子创建；
  `fingerprint = sha256(canonical(frozen spec))`，写入切换证明。
- **CAS 协调**：计划状态带 revision，所有推进走 compare-and-set；
  冲突放弃本轮重读，杜绝两个协调者同时切换。
- **栅栏/epoch**：`FenceStore.ack(side, epoch, plan_id)` 是推进水位的唯一入口；
  SEALED 期间拒绝一切 ack，OPEN_NEW 后旧 epoch/旧 side 永久拒绝并记录。
- **幂等泵**：每条远端变更在同一目标临界区落本地 WAL + origin→本地 LSN 映射；
  批末崩溃重试按映射跳过，水位只提交一次；回填按表标记；UPDATE 覆盖、
  DELETE 空操作、INSERT upsert。
- **位点翻译**：新消费槽起点 = `map_origin(旧水位)`（目标本地 LSN），
  未映射即拒绝切换。
- **崩溃窗口**：`open_new` 成功但计划状态/证明未落盘时，重入走恢复分支
  幂等收尾，栅栏不会二次迁移。

## 切换证明内容

冻结计划指纹、最终对账事实（旧水位/目标远端位点/新槽本地起点/源当前位点）、
快照摘要、全表摘要、增量摘要、模式摘要、栅栏 revision 与 sealed epoch、
全部闸门结果、事件序列、不变量断言，以及 SHA-256 摘要与 HMAC 签名。
`verify_proof(proof, key)` 独立校验签名、指纹一致、位点自洽、闸门全绿；
篡改任一位点/摘要字段或用错密钥都会失败。

## 代码结构

```
slotmigrator/
  types.py       领域类型（LSN/WAL/快照/冻结计划/阶段/闸门）
  digest.py      规范 JSON + 快照/模式/增量链式摘要
  store.py       CAS 存储 + 追加事件日志
  fence.py       open_old/sealed/open_new 栅栏与 epoch 裁决
  memcluster.py  内存数据库集群（槽/WAL/origin/幂等泵/故障注入）
  controller.py  声明式 reconcile、闸门、原子切换、对账视图
  consumer.py    消费者（缓冲未确认条目，ack 必须过栅栏）
  proof.py       切换证明构建与独立验证
  harness.py     演示/测试世界装配
  server.py      HTTP 计划 API + 对账/证明 HTML 页面
  cli.py         demo / serve
tests/           22 个测试：快乐路径、DDL 漂移、损坏拦截、故障重试、
                 崩溃重放、迟到 ack、并发争抢、证明篡改
```
