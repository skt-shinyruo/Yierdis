<!-- migrated_from: history/2026-01/202601171043_arch_unify_protocol_ctx/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# 任务列表：架构收敛（移除 Deprecated Alias / 协议栈统一 / ConnectionContext / 可观测性 / 命令路由加速）

Directory: `helloagents/plan/202601171043_arch_unify_protocol_ctx/`

---

## 1. Bytes / Deprecated Alias 移除（Breaking）
- [√] 1.1 在 `yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapSlice.java` 收敛读写接口到 `yier.bubu.redis.bytes.*`，verify `why.md#requirement-bytes-alias-removal` / `why.md#scenario-compile-time-enforcement`
- [√] 1.2 将 `yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisByteArraySink.java` 改为实现 `yier.bubu.redis.bytes.BytesSink`，verify `why.md#requirement-bytes-alias-removal`
- [√] 1.3 将 `yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisByteBufferSink.java` 改为实现 `yier.bubu.redis.bytes.DirectBytesSink`，verify `why.md#requirement-bytes-alias-removal`
- [√] 1.4 删除 deprecated alias：`yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisBytesSink.java`，verify `why.md#requirement-bytes-alias-removal`
- [√] 1.5 删除 deprecated alias：`yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisDirectBytesSink.java`，verify `why.md#requirement-bytes-alias-removal`
- [√] 1.6 删除 deprecated alias：`yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisBytesSource.java`，verify `why.md#requirement-bytes-alias-removal`
- [√] 1.7 迁移 off-heap 实现对 bytes alias 的引用：`yierdis-offheap/netty/src/main/java/yier/bubu/redis/db/offheap/netty/YierdisNettyByteBufSink.java`、`yierdis-offheap/netty/src/main/java/yier/bubu/redis/db/offheap/netty/YierdisNettyByteBufSource.java`，verify `why.md#requirement-bytes-alias-removal`，depends on 1.1-1.6
- [√] 1.8 迁移 off-heap 实现对 bytes alias 的引用：`yierdis-offheap/unsafe/src/main/java/yier/bubu/redis/db/offheap/unsafe/YierdisUnsafeOffHeapAllocator.java`、`yierdis-offheap/foreign/src/main/java/yier/bubu/redis/db/offheap/foreign/YierdisForeignOffHeapAllocator.java`，verify `why.md#requirement-bytes-alias-removal`，depends on 1.1-1.6
- [√] 1.9 迁移 core 对 bytes alias 的引用：`yierdis-core/src/main/java/yier/bubu/redis/db/offheap/YierdisUnsafeOffHeapRawSlice.java`、`yierdis-core/src/main/java/yier/bubu/redis/db/offheap/YierdisUnsafeOffHeapString.java`，verify `why.md#requirement-bytes-alias-removal`，depends on 1.1-1.6

## 2. 协议栈统一：回复 fast-path / codec 收敛
- [√] 2.1 在 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java` 抽取可复用解析工具（CRLF/parseInt/limits），verify `why.md#requirement-protocol-unification` / `why.md#scenario-client-bench-use-same-fast-codec`
- [√] 2.2 在 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespDecoder.java` 收敛为 frame/zero-copy 取向（或引入新 fast reply decoder 并迁移引用），verify `why.md#requirement-protocol-unification`，depends on 2.1
- [√] 2.3 更新 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespDecoderTest.java` 覆盖新 decoder 行为与限制参数，verify `why.md#requirement-protocol-unification`

## 3. client/bench 迁移到同源 codec
- [√] 3.1 更新 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java` 使用收敛后的 decoder（移除 `RespObject` 依赖，改为 frame-based 回复解析），verify `why.md#requirement-protocol-unification`，depends on 2.2
- [√] 3.2 更新 `yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java` 覆盖：正常请求、超时断连、回复解码正确性，verify `why.md#requirement-protocol-unification`
- [√] 3.3 更新 `yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java` 使用同源 decoder，减少分配干扰 benchmark，verify `why.md#requirement-protocol-unification`，depends on 2.2

## 4. ConnectionContext（直接迁移，不做最小整理）
- [√] 4.1 新增 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/ConnectionContext.java`：实现 `RespSession` + 承载 executor/统计字段，verify `why.md#requirement-connection-context` / `why.md#scenario-single-context-for-protocol-and-executor`
- [√] 4.2 在 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java` 初始化并绑定 `ConnectionContext`（单一 `Channel.attr`），verify `why.md#requirement-connection-context`，depends on 4.1
- [√] 4.3 将 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/NettyRespSession.java` 迁移/替换为 `ConnectionContext`（移除 PROTOCOL attr），verify `why.md#requirement-connection-context`，depends on 4.1
- [√] 4.4 将 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java` 迁移到 `ConnectionContext`（删除 pending/backpressure/closing 多 attr），verify `why.md#requirement-connection-context`，depends on 4.1-4.3
- [√] 4.5 将 `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java` 改为基于 `ConnectionContext` 构造 `RespWriter`，verify `why.md#requirement-connection-context`，depends on 4.1-4.3

## 5. 可观测性（性能优先）
- [√] 5.1 在 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java` 增加低开销统计更新点（队列/背压/超预算/关闭等），verify `why.md#requirement-observability` / `why.md#scenario-debug-and-stats`，depends on 4.4
- [√] 5.2 在 `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java` 新增 `INFO`/`STATS` 命令输出统计摘要，verify `why.md#requirement-observability`
- [√] 5.3 增加观测相关测试：`yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`（或新增测试文件）覆盖背压/closing/统计变化，verify `why.md#requirement-observability`

## 6. 命令路由加速（CommandRegistry）
- [√] 6.1 改造 `yierdis-core/src/main/java/yier/bubu/redis/command/CommandRegistry.java` 从线性扫描升级为 O(1) 索引（保持零分配），verify `why.md#requirement-command-routing-index` / `why.md#scenario-o1-command-lookup`
- [√] 6.2 新增/更新 `yierdis-core/src/test/java/yier/bubu/redis/command/CommandRegistryTest.java`（如不存在则新增）覆盖大小写、冲突、unknown 命令，verify `why.md#requirement-command-routing-index`

## 7. Security Check
- [√] 7.1 执行安全检查（G9）：输入校验、错误消息净化、权限/敏感信息、DoS 上限与资源回收、避免引入 EHRB 风险

## 8. 文档与知识库同步
- [√] 8.1 更新 `helloagents/wiki/arch.md` 与相关 module 文档（`helloagents/wiki/modules/protocol.md`、`helloagents/wiki/modules/server.md`、`helloagents/wiki/modules/client.md`、`helloagents/wiki/modules/offheap.md`）同步新的 SSOT 与依赖关系
- [√] 8.2 更新 `helloagents/CHANGELOG.md` 记录 Breaking change 与迁移摘要

## 9. 测试与验证
- [√] 9.1 运行 `mvn test`（全量），并记录关键用例覆盖：codec、ConnectionContext、CommandRegistry、client timeout 语义
- [√] 9.2 使用 `yierdis-bench` 做基础对比验证（吞吐/延迟/分配趋势），确认无明显性能回退
