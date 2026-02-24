<!-- migrated_from: history/2026-01/202601162335_fix_arch_issues/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List：修复 5 个架构/实现问题（server/client/bytes 交界处）

Directory: `helloagents/plan/202601162335_fix_arch_issues/`

---

## 1. server：QUIT 顺序语义与 close-after-reply

- [√] 1.1 在协议层增加 close-after-reply 表达能力（建议扩展 `RespWriter`），用于命令层请求“回复后关闭连接”；verify why.md#核心场景
  - 目标文件：`yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespWriter.java`
- [√] 1.2 在命令层实现 `QUIT`（注册为 server 命令），并通过 writer 请求 close-after-reply；verify why.md#Requirement-1pipeline-顺序语义下的-quit
  - 目标文件：`yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java`
- [√] 1.3 执行器支持 close-after-reply：flush 后关闭连接，并在 QUIT 后跳过该连接剩余已入队任务（仅回收，不执行 DB）；verify why.md#Requirement-1pipeline-顺序语义下的-quit
  - 目标文件：`yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- [√] 1.4 移除/收敛 handler 对 `QUIT` 的 special-case，让所有命令统一走执行器；verify why.md#Requirement-1pipeline-顺序语义下的-quit
  - 目标文件：`yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- [√] 1.5 补充/调整测试覆盖 pipeline QUIT 顺序与 “QUIT 后命令不执行”；verify why.md#Requirement-1pipeline-顺序语义下的-quit
  - 目标文件：`yierdis-server/src/test/java/yier/bubu/redis/FastPipelineTest.java`（或新增同目录测试类）

## 2. bytes/offheap/server：Netty bytes adapter 边界收敛

- [√] 2.1 新增模块 `yierdis-bytes-netty` 并加入父 POM modules；verify why.md#Requirement-2bytesnetty-适配边界清晰
  - 目标文件：`pom.xml`
  - 目标文件：`yierdis-bytes-netty/pom.xml`
- [√] 2.2 在 `yierdis-bytes-netty` 提供 `NettyByteBufSink`（实现 `yier.bubu.redis.bytes.DirectBytesSink` 并可 unwrap `ByteBuf`）；verify why.md#Requirement-2bytesnetty-适配边界清晰
  - 目标文件：`yierdis-bytes-netty/src/main/java/yier/bubu/redis/bytes/netty/NettyByteBufSink.java`
- [√] 2.3 server 写回路径改用 `NettyByteBufSink`，避免直接依赖 `yierdis-offheap-netty` 的 sink；verify why.md#Requirement-2bytesnetty-适配边界清晰
  - 目标文件：`yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
  - 目标文件：`yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- [√] 2.4 offheap-netty 的 slice 写出 fast-path 识别 `NettyByteBufSink`（保持零拷贝/少拷贝）；verify why.md#Requirement-2bytesnetty-适配边界清晰
  - 目标文件：`yierdis-offheap/netty/src/main/java/yier/bubu/redis/db/offheap/netty/YierdisNettyOffHeapAllocator.java`
- [√] 2.5 Maven 依赖收敛：server/offheap-netty 引入 `yierdis-bytes-netty` 依赖，并清理不必要的传递依赖；verify why.md#影响范围
  - 目标文件：`yierdis-server/pom.xml`
  - 目标文件：`yierdis-offheap/netty/pom.xml`

## 3. client：超时后连接不可复用（避免响应错配）

- [√] 3.1 `execute()` 超时后关闭连接并标记 client 不可用（后续调用直接失败）；verify why.md#Requirement-3client-超时不导致响应错配
  - 目标文件：`yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java`
- [√] 3.2 增加 client 超时相关测试（可通过控制 server 延迟或模拟 handler）；verify why.md#Requirement-3client-超时不导致响应错配
  - 目标文件：`yierdis-client/src/test/java/...`（新增/调整测试类）

## 4. client/CLI：help/version SSOT（消除硬编码）

- [√] 4.1 为 `yierdis-client` 增加与 core 类似的资源注入（`yierdis-version.properties`），并改造 `--help` 输出读取 version；verify why.md#Requirement-4cli-helpversion-不硬编码
  - 目标文件：`yierdis-client/pom.xml`
  - 目标文件：`yierdis-client/src/main/resources-filtered/yierdis-version.properties`
  - 目标文件：`yierdis-client/src/main/java/yier/bubu/redis/client/YierdisCli.java`

## 5. server：非法参数退出码与启动诊断

- [√] 5.1 非法参数时返回非 0 退出码（保持 usage 输出）；verify why.md#Requirement-5server-参数错误可脚本化识别
  - 目标文件：`yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`
- [-] 5.2（可选）启动时打印关键配置摘要（队列/背压/协议上限/off-heap 等）；verify why.md#Requirement-5server-参数错误可脚本化识别
  > Note: 可选项；当前 `YierdisServerBootstrap` 已打印 off-heap 后端/可用 providers，暂不额外扩展启动摘要以避免日志噪音。
  - 目标文件：`yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`

## 6. Security Check

- [√] 6.1 执行安全检查（输入上限、错误信息 CRLF 清洗、连接关闭语义、资源释放路径）；特别关注 QUIT 后任务跳过是否仍保证 `RespFrame/ByteBuf` 被回收

## 7. Documentation Update（知识库同步）

- [√] 7.1 更新架构与模块文档，确保 “依赖边界/单入口执行” 与代码一致；verify why.md#需求背景
  - 目标文件：`helloagents/wiki/arch.md`
  - 目标文件：`helloagents/wiki/modules/server.md`
  - 目标文件：`helloagents/wiki/modules/offheap.md`
  - 目标文件：`helloagents/wiki/modules/client.md`

## 8. Testing

- [√] 8.1 `mvn test` 全量回归（含 server/client/protocol/offheap）
- [-] 8.2（可选）运行简单 smoke：启动 server，使用 `yierdis-client` 执行 `PING/HELLO 3/QUIT`；并用 pipeline 用例验证 QUIT 顺序与关闭
  > Note: 可选项；已通过单元/集成测试覆盖 QUIT 顺序与 client 行为，未额外执行手工 smoke。
