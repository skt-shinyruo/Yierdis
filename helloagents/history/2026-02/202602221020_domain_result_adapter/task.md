# Task List: core 分层解耦（domain result → adapter，移除 db/数据结构对 ReplySink 的依赖）

Directory: `helloagents/plan/202602221020_domain_result_adapter/`

---

## 1. yierdis-core（domain result API + adapter 基建）
- [√] 1.1 新增 domain result 与 sink：定义标量/聚合结果（支持 `null`、heap bytes slice、`BytesSlice`、`long` ASCII），新增：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/result/*`（新增若干类型）
  verify why.md#requirement-存储算法层协议解耦domain-result--adapter-scenario-get-在-off-heap-string-下仍保持低分配写出
- [√] 1.2 新增命令层 adapter：将 domain result 写入 `ReplyWriter`（集中协议依赖），新增：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/*`（新增 1 个 adapter 类）
  verify why.md#requirement-存储算法层协议解耦domain-result--adapter-scenario-lrange--smembers--zrange-等聚合读仍可-streaming-输出

## 2. yierdis-core（ops 接口迁移：去 ReplySink）
- [√] 2.1 迁移 `StringOps`：移除 `getStringForReply(..., ReplySink)`，改为返回标量 domain result；同步修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/StringOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/StringCommands.java`
  verify why.md#requirement-存储算法层协议解耦domain-result--adapter-scenario-get-在-off-heap-string-下仍保持低分配写出
- [√] 2.2 迁移 `ListOps`：用聚合 domain result 替代 `lrangeCount/lrangeWriteTo`；同步修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/ListOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ListCommands.java`
  verify why.md#requirement-存储算法层协议解耦domain-result--adapter-scenario-lrange--smembers--zrange-等聚合读仍可-streaming-输出
- [√] 2.3 迁移 `SetOps`：用聚合 domain result 替代 `smembersCount/smembersWriteTo`；同步修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/SetOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/SetCommands.java`
  verify why.md#requirement-存储算法层协议解耦domain-result--adapter-scenario-lrange--smembers--zrange-等聚合读仍可-streaming-输出
- [√] 2.4 迁移 `HashOps`（HGETALL）：用 map domain result 替代 `hgetallPairCount/hgetallWriteTo`；同步修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/HashOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/HashCommands.java`
  verify why.md#requirement-存储算法层协议解耦domain-result--adapter-scenario-lrange--smembers--zrange-等聚合读仍可-streaming-输出
- [√] 2.5 迁移 `ZSetOps`：用聚合 domain result 替代 `*Count/*WriteTo`（含 WITHSCORES 与 byscore variants）；同步修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/ZSetOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ZSetCommands.java`
  verify why.md#requirement-存储算法层协议解耦domain-result--adapter-scenario-lrange--smembers--zrange-等聚合读仍可-streaming-输出

## 3. yierdis-core（db/value/off-heap 内部迁移：去 ReplySink import）
- [√] 3.1 迁移 listpack cursor：将 `writeTo(ReplySink)` 改为写入 domain sink/result，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisListpack.java`
  depends on task 1.1
- [√] 3.2 迁移 off-heap listpack cursor：同上，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/db/offheap/YierdisUnsafeOffHeapListpack.java`
  depends on task 1.1
- [√] 3.3 迁移集合 value：`HashValue/ListValue/SetValue/ZSetValue` 去除 `ReplySink`，改为产出 domain result 或写入 domain sink，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/db/HashValue.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/ListValue.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/SetValue.java`
  verify why.md#requirement-存储算法层协议解耦domain-result--adapter-scenario-lrange--smembers--zrange-等聚合读仍可-streaming-输出
- [√] 3.4 迁移 `ZSetValue`/off-heap ZSet：去除 `ReplySink`，并保持 WITHSCORES 输出一致，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/db/ZSetValue.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/offheap/YierdisUnsafeOffHeapZSet.java`
  verify why.md#requirement-存储算法层协议解耦domain-result--adapter-scenario-lrange--smembers--zrange-等聚合读仍可-streaming-输出

## 4. yierdis-core（off-heap 异常解耦）
- [√] 4.1 移除 off-heap 对 `YierdisCommandException` 的依赖：改为中立异常并在命令层保证错误语义不变，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/db/offheap/YierdisUnsafeOffHeapZSet.java`
  verify why.md#requirement-off-heap-数据结构不依赖命令异常-scenario-zadd-score-非法时错误语义保持一致

## 5. Security Check
- [√] 5.1 执行安全检查（G9）：确认 domain result/adapter 不缓存引用、off-heap slice 生命周期安全、错误 message 净化策略不弱化；记录发现与结论
  - 结论：domain result/adapter 路径不缓存输入引用；`BulkStringSink` 约束“同步消费且不得保留引用”，`BulkStringReplyAdapter` 满足该约束
  - off-heap slice：结果对象仅在单次命令执行路径中被消费；未新增跨命令生命周期的引用持有/缓存
  - 错误 message：非法 score 抛 `IllegalArgumentException("value is not a valid float")`，上层映射为 `ERR value is not a valid float`；message 为常量，未引入注入面

## 6. Documentation Update
- [√] 6.1 更新知识库文档以匹配新分层（db/offheap 不再依赖 ReplySink），至少更新：
  - `helloagents/wiki/modules/db.md`
  - `helloagents/wiki/modules/protocol.md`
  - `helloagents/wiki/modules/command.md`
- [√] 6.2 更新 `helloagents/CHANGELOG.md`：记录本次分层边界变化、风险提示与回归结果

## 7. Testing
- [√] 7.1 新增架构边界回归测试（禁止 db/offheap import `yier.bubu.redis.protocol.*`），新增：
  - `yierdis-core/src/test/java/yier/bubu/redis/*`（新增 1 个测试类）
- [√] 7.2 运行 `mvn test` 确保全量测试通过，并记录关键结果
  - 命令：在仓库根目录执行 `mvn test`
  - 结果：BUILD SUCCESS
  - 备注：若使用 `mvn test -rf :yierdis-core` 可能因 reactor 前置模块未构建导致依赖解析失败，建议直接跑全量 `mvn test`
