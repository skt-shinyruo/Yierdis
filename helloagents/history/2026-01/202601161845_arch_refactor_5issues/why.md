# Change Proposal: 架构问题 5 项治理（解耦 / 生命周期 / 背压 / RESP3 对齐 / 可维护性）

## Requirement Background

当前代码已经形成一条清晰的“请求 → 解码 → 入队 → 单线程执行 → 回包”链路：

- Client：`YierdisCli`/`YierdisClient` 通过 Netty 发送 RESP2 数组（bulk string argv），并用 `RespDecoder` 解码 RESP2 回复。
- Server：Netty pipeline 使用 `RespCommandDecoder` 将请求解码为 `RespCommand`（包含 frame 引用），交给 `YierdisFastCommandHandler`，再提交给 `NettyCommandExecutor` 单线程执行，最终由 `YierdisFastCommandProcessor` 分发命令并操作 `YierdisDb`。

但在继续演进之前，存在 5 类架构与实现层面的风险/问题，会降低可维护性、可测试性与稳定性：

1. **层次边界与依赖方向不清晰**：协议层与 off-heap/db 包路径耦合；执行器同时承担队列调度、Netty I/O 写回、协议 session 等多重职责。
2. **生命周期“隐式契约”风险高**：`RespCommand` 依赖手工 `recycle()` 释放 frame（常见为 Netty `ByteBuf` slice）；启动失败/异常路径可能漏释放。
3. **背压/拒绝策略语义不一致**：过载时容易出现“反复 decode → busy → decode”的放大效应；某些条件下可能出现“禁读后无法恢复”的风险。
4. **协议能力不对称**：Server 支持 `HELLO 3` 切换 RESP3 并返回 map / null，但内置 client/CLI 仅支持 RESP2，导致 RESP3 分支缺乏工程化验证。
5. **可维护性/可观测性不足**：启动装配逻辑集中在 `main`；异常处理缺乏足够日志；CLI 解析规则与 server inline command 解析存在漂移风险。

## Change Content

1. 重新梳理模块边界与依赖：抽出通用 bytes 抽象，减少 `protocol ↔ offheap/db` 的反向依赖；为执行器引入更清晰的职责拆分。
2. 强化资源生命周期：让关键对象具备更安全的关闭/回收路径，降低遗漏 `recycle()` 的概率；补齐启动失败与异常路径的清理。
3. 统一背压语义：区分“暂停读取（背压）”与“拒绝请求（busy）”；为全局过载补充可恢复的 backpressure 机制与测试覆盖。
4. 让内置 client/CLI 能覆盖 RESP3 最小子集：至少支持 `HELLO 3` 的 map（`%`）与 RESP3 null（`_`），并更新打印逻辑与测试。
5. 改善可维护性与观测：抽取 server bootstrap，补充关键日志；统一/复用 inline args 解析规则（至少共享测试用例），并同步文档。

## Impact Scope

- **Modules:**
  - `yierdis-server`
  - `yierdis-client`
  - `yierdis-protocol`
  - `yierdis-protocol-netty`
  - `yierdis-offheap`（至少 `api`，可能涉及 `netty`）
  - `yierdis-core`
  - `yierdis-args`
- **Files (expected):**
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java`
  - `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java`
  - `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespDecoder.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObject.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespType.java`
  - `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java`
  - `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisCli.java`
  - `pom.xml`（父模块 + 依赖调整）
  - 以及新增模块/类与相应测试文件
- **APIs:**
  - 可能新增/调整 server 参数（用于 global backpressure 水位/策略等）
  - client/CLI 增强 RESP3 支持（不破坏现有 RESP2 默认行为）

## Core Scenarios

<a id="r1"></a>
### Requirement: R1 模块边界清晰化
**Module:** yierdis-protocol / yierdis-offheap / yierdis-server
目标：让依赖关系更直观可控，减少“协议层引用 DB/offheap 包路径”的误导性耦合；降低 `NettyCommandExecutor` 的职责复杂度。

<a id="r1-s1"></a>
#### Scenario: R1-S1 协议层依赖方向修正
- 期望结果：`yierdis-protocol` 不再直接依赖 `yierdis-offheap-api` 的包路径；bytes 抽象位于中立模块（或以兼容方式过渡）。

<a id="r1-s2"></a>
#### Scenario: R1-S2 执行器职责拆分
- 期望结果：队列调度/背压策略与 Netty 写回解耦；核心逻辑可在不依赖 Netty 的情况下做单测验证。

<a id="r2"></a>
### Requirement: R2 生命周期与资源安全
**Module:** yierdis-protocol(-netty) / yierdis-server
目标：减少 frame/ByteBuf 泄漏风险；启动失败与异常路径可可靠释放资源。

<a id="r2-s1"></a>
#### Scenario: R2-S1 命令 frame 回收一致性
- 期望结果：无论 success/busy/异常路径，`RespCommand` 都能被可靠回收或由统一机制兜底。

<a id="r2-s2"></a>
#### Scenario: R2-S2 启动失败不泄漏
- 期望结果：即使 server 在“初始化中途”失败（allocator/db/executor/Netty bind），也不会残留 allocator/direct memory/线程资源。

<a id="r3"></a>
### Requirement: R3 背压语义一致化
**Module:** yierdis-server
目标：明确区分“背压（暂停读取）”与“拒绝（busy）”；降低过载放大，保证可恢复。

<a id="r3-s1"></a>
#### Scenario: R3-S1 连接级背压只影响读取，不丢请求
- 期望结果：当连接 pending/bytes 达到高水位时，server 对该连接进入背压（关闭 autoRead），但不会直接对“已经读到的当前命令”返回 busy（除非全局过载/关闭）。

<a id="r3-s2"></a>
#### Scenario: R3-S2 全局过载可恢复
- 期望结果：当全局队列/bytes 预算耗尽时，触发全局 backpressure（可配置或有默认滞回），并在队列下降到低水位后自动恢复读取，避免 busy 风暴与永久禁读。

<a id="r4"></a>
### Requirement: R4 RESP3 能力对齐（最小子集）
**Module:** yierdis-protocol / yierdis-protocol-netty / yierdis-client
目标：工程内置 client/CLI 能覆盖 server 的 RESP3 分支（至少 HELLO 3），以便持续回归。

<a id="r4-s1"></a>
#### Scenario: R4-S1 client 能解析 HELLO 3 的 map
- 期望结果：client/CLI 能正确解析并打印 RESP3 的 `%` map 类型（至少用于 `HELLO 3` 返回结构）。

<a id="r4-s2"></a>
#### Scenario: R4-S2 client 能解析 RESP3 null
- 期望结果：在 RESP3 模式下（或返回 `_` 时），client/CLI 不崩溃，能以 `(nil)` 形式展示。

<a id="r5"></a>
### Requirement: R5 可维护性与可观测性提升
**Module:** yierdis-server / yierdis-client / docs
目标：降低 main 复杂度，提升异常定位能力，避免解析规则漂移。

<a id="r5-s1"></a>
#### Scenario: R5-S1 server bootstrap 可复用/可测试
- 期望结果：将装配、启动、关闭流程从 `main` 抽出为可复用组件，便于在集成测试/工具中启动/停止。

<a id="r5-s2"></a>
#### Scenario: R5-S2 异常路径可观察
- 期望结果：协议错误与内部错误在 server 端有可检索的日志（区分级别/类别），同时对外仍保持安全的错误消息。

<a id="r5-s3"></a>
#### Scenario: R5-S3 CLI 与 server inline parser 规则一致
- 期望结果：两者共享同一套解析规则或共享同一套测试向量，避免输入行为不一致导致的误判。

## Risk Assessment

- **Risk:** 模块拆分/依赖调整引入编译与打包变更  
  **Mitigation:** 采用渐进式迁移（先引入中立模块 + 兼容桥接/保留旧接口，再逐步切换依赖）；分阶段提交；保持 Maven 构建可用。
- **Risk:** 背压语义调整改变过载时客户端体验  
  **Mitigation:** 明确策略（背压优先、拒绝仅用于全局过载/关闭）；增加单测/压测；保留可配置参数与默认值。
- **Risk:** RESP3 对象模型扩展影响现有 RESP2 逻辑  
  **Mitigation:** 保持 RESP2 默认路径不变；RESP3 仅在显式切换后启用；补齐兼容测试。
- **Risk:** 资源释放路径遗漏导致 direct memory 泄漏  
  **Mitigation:** 统一 close/recycle 语义；在测试中启用更严格的资源泄漏检查；覆盖 busy/异常/关闭场景。
