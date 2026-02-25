# 任务清单: change_event_real_change

```yaml
@feature: change_event_real_change
@created: 2026-02-25
@status: completed
@mode: R3
```

<!-- LIVE_STATUS_BEGIN -->
状态: completed | 进度: 22/22 (100%) | 更新: 2026-02-25 14:49:18
当前: 已完成（通过 mvn test 验证）
<!-- LIVE_STATUS_END -->

## 进度概览

| 完成 | 失败 | 跳过 | 总数 |
|------|------|------|------|
| 21 | 0 | 1 | 22 |

---

## 任务列表

### 1. core-api：变更追踪契约（ChangeScope）

- [√] 1.1 在 `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeTracking.java` 新增 thread-local 的 command-scope（begin/close + valueChanged/ttlChanged flags）
- [√] 1.2 定义并文档化语义：无 active scope 时 `mark*` 必须 no-op；`beginScope()` 必须重置 flags；close 必须清理 active（避免泄漏到下一条命令）
  - 依赖: 1.1
- [√] 1.3 更新 `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java` 的 JavaDoc：事件载荷仍是 argv 快照（可重放），但发射条件升级为“命令成功 + 真实变更才 emit”
  - 依赖: 2.2

### 2. core-command：命令层 emit gate（移除 isWriteCommand SSOT）

- [√] 2.1 在 `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java` 中为 handler.execute 包裹 `YierdisChangeTracking.beginScope()`（try-with-resources），并确保事务 enqueue 早返回路径不打开 scope
  - 依赖: 1.1
- [√] 2.2 将 change event 发射条件改为 `YierdisChangeTracking.changedAny()`；保证未变更时不执行 `copyArgv(cmd)`（避免无意义分配）
  - 依赖: 2.1
- [√] 2.3 删除 `YierdisFastCommandProcessor#isWriteCommand`（或至少不再用于 change event 判定），并补齐注释说明“emit 依据=真实变更事实”
  - 依赖: 2.2

### 3. core-db：真实变更打点（Value/Keyspace/TTL）

- [√] 3.1 Strings：在 `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java` 的 `Strings#setString` 中，仅当 `didSet==true` 时调用 `YierdisChangeTracking.markValueChanged()`（修复 `SET ... NX` 未写入仍 emit 的虚假事件）
  - 依赖: 1.1
- [√] 3.2 Strings：在 `Strings#append/#setBit/#incrBy` 中，当语义上发生“真实写入”时标记 `markValueChanged()`（避免写命令变成漏发）
  - 依赖: 1.1
- [√] 3.3 Hashes：在 `Hashes#hset` 中标记 `markValueChanged()`（不做 value-equals 深比较）；在 `Hashes#hdel` 中仅当 `removed>0` 时标记
  - 依赖: 1.1
- [√] 3.4 Lists：在 `Lists#lpush/#rpush` 中标记 `markValueChanged()`；在 `Lists#lpop/#rpop` 中仅当实际弹出元素数>0 时标记
  - 依赖: 1.1
- [√] 3.5 Sets：在 `Sets#sadd/#srem` 中仅当返回值>0（实际增删成员）时标记 `markValueChanged()`
  - 依赖: 1.1
- [√] 3.6 ZSets：在 `ZSets#zadd` 中补齐“score 更新但 added=0”仍属于真实变更的探测，并据此标记 `markValueChanged()`；`zrem*` 系列仅当 removed>0 时标记
  - 依赖: 1.1
- [√] 3.7 HLL：在 `Hll#pfadd` 中当返回值为 1 时标记 `markValueChanged()`；在 `Hll#pfmerge` 中标记 `markValueChanged()`（修复现状 `isWriteCommand` 漏掉 PFMERGE 的漂移风险）
  - 依赖: 1.1
- [√] 3.8 Keyspace/TTL：在 `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java` 的 `del/flushDb/expire/pexpire/expireAt*/persist` 路径按“真实变更”标记 value/ttl（例如 `DEL` 删除 0 个 key 不标记；`PERSIST` 无 TTL 不标记；`EXPIRE missing` 不标记）。注意：不要在过期清理/淘汰等维护路径打点
  - 依赖: 1.1

### 4. tests：回归矩阵（真实变更 vs no-op）

- [√] 4.1 更新 `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeSinkTest.java`：从 “write commands only” 改为 “real changes only”，补充 `SET NX` 未写入 / `DEL missing` / `EXPIRE missing` / `PERSIST` 无 TTL 的不发射断言
  - 依赖: 2.2, 3.1, 3.8
- [√] 4.2 增加事务用例：`MULTI` 入队阶段不发射；`EXEC` 重放时仅对真实写入的命令发射（可新增测试类或扩展现有 test）
  - 依赖: 2.1, 2.2
- [√] 4.3 增加 ZADD 关键用例：对同 member 更新 score（added=0）仍必须发射事件（防止“真实变更但 added=0”漏发）
  - 依赖: 3.6
- [√] 4.4 增加 PFMERGE 用例：确保写入后能发射事件（证明已消除 `isWriteCommand` 漂移漏发）
  - 依赖: 3.7

### 5. 知识库同步（契约更新）

- [√] 5.1 更新 `.helloagents/modules/db.md` 的 “Contract: Snapshot / ChangeEvent” 段落：明确 change event 语义=“命令成功 + 真实变更才 emit”，并列出典型 no-op 示例（SET NX/DEL 0/PERSIST 无 TTL/EXPIRE missing）
- [-] 5.2 如 `.helloagents/wiki/arch.md` 与 `.helloagents/modules/arch.md` 均作为 SSOT，则同步更新相关段落（避免知识库漂移）；否则明确其一为 SSOT 并在另一处标注迁移/废弃说明（本次变更未修改 arch.md 的陈述点，暂无需同步）

### 6. 验证与交付

- [√] 6.1 在仓库根目录运行 `mvn test`，确保新增/更新用例通过（并关注是否存在因 emit 语义变化导致的其它回归）
  - 依赖: 4.1, 4.2, 4.3, 4.4
- [√] 6.2 运行方案包校验：`python3 -X utf8 \"/home/feng/.codex/helloagents/scripts/validate_package.py\" --path \"/home/feng/code/project/Yierdis\" 202602251158_change-event-real-change`

---

## 执行日志

| 时间 | 任务 | 状态 | 备注 |
|------|------|------|------|
| 2026-02-25 14:49:18 | 1.x-4.x | completed | 变更追踪 + emit gate + DB 打点 + 单测矩阵落地 |
| 2026-02-25 14:48:28 | 6.1 | completed | `mvn test` BUILD SUCCESS |
| 2026-02-25 14:49:10 | 6.2 | completed | validate_package: valid=true |

---

## 执行备注

> 记录执行过程中的重要说明、决策变更、风险提示等
