<!-- migrated_from: history/2026-01/202601171043_arch_unify_protocol_ctx/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# 技术设计：架构收敛（移除 Deprecated Alias / 协议栈统一 / ConnectionContext / 可观测性 / 命令路由加速）

## 技术方案

### 核心技术

- Java 17
- Netty 4.1（TCP + pipeline）
- Maven multi-module
- RESP2/RESP3（最小子集）协议模型与 codec

### 实现要点

1. **bytes 抽象 SSOT：彻底移除 deprecated alias（Breaking）**
   - `yierdis-bytes` 作为唯一 bytes API：`BytesSink/BytesSource/DirectBytesSink/BytesSlice`。
   - `yierdis-offheap/api` 删除历史 alias（`YierdisBytesSink/YierdisBytesSource/YierdisDirectBytesSink` 等），并将现有 sink/source 的实现直接改为实现 `yier.bubu.redis.bytes.*`。
   - `YierdisOffHeapSlice` 对外写出/读入接口签名统一收敛到 `yier.bubu.redis.bytes.*`，并保持 netty/off-heap fast-path（例如通过 `yierdis-bytes-netty` 的 `NettyByteBufSink` 进行零拷贝写出）。

2. **协议栈统一：client/bench 回复解码迁移到与 server 同源的 fast-path/codec**
   - 在 `yierdis-protocol-netty` 抽取解析共用能力（例如 CRLF 定位、整数解析、DoS 上限控制、错误语义），供 request/response 两类 decoder 复用。
   - 引入（或重写）“回复 fast decoder”，输出 frame/zero-copy 取向的数据结构（例如 `NettyRespFrame` 或其上层视图），避免构建 `RespObject` 树与 `String/byte[]` 大量分配。
   - client：从 `RespObject` 收敛到 frame-based 解析（按需解析 OK/ERR/int/bulk 等），并保持 FIFO request/response 配对语义（超时→连接不可复用）。
   - bench：复用同一 decoder，与 server 的协议行为/限制保持一致；在 strictReplies 模式下提供更严格校验。

3. **连接态迁移：单一 `ConnectionContext`（直接迁移，不做最小整理）**
   - 新增 `yier.bubu.redis.protocol.netty.ConnectionContext`：
     - 作为 channel-level SSOT，统一保存：
       - 协议协商状态（实现 `RespSession`）
       - executor 相关连接态（pending/bytes/backpressure/closing/状态机）
       - 连接级统计（为可观测性提供容器）
     - 以单一 `AttributeKey<ConnectionContext>` 绑定到 `Channel`。
   - 删除/收敛现有分散状态：
     - `NettyRespSession` 的 PROTOCOL attr → 迁移到 ConnectionContext
     - `NettyCommandExecutor` 的多组 attr（pending/pendingBytes/autoreadDisabled/closing/state）→ 迁移到 ConnectionContext
   - 迁移策略：以“全量替换”为目标，避免长期双写/双读引入的不一致风险。

4. **可观测性优先（低开销，可按需打开）**
   - 在 `ConnectionContext` 与 `NettyCommandExecutor` 内维护关键指标（计数器/时间戳/水位变化次数）。
   - 在 core 命令层提供查询入口：
     - 新增 `INFO`/`STATS`（或同等）命令输出 server 全局与连接级统计摘要（文本形式优先，避免复杂对象分配）。
   - 增强日志：
     - 队列饱和/背压状态变化：低频日志（可配置采样/间隔）
     - drain tick 超预算/慢命令：可配置阈值，避免常态噪音

5. **命令路由加速：`CommandRegistry` 升级为 O(1) 索引结构**
   - 保持现有约束：运行时查找零分配、大小写不敏感。
   - 方案：在注册阶段构建 hash 索引（开地址/链式桶均可），查找阶段对输入命令名做 ASCII case-insensitive hash 并 probe；冲突时进行二次比对（保持正确性）。
   - 为观测预留：命中计数、unknown 命令计数、hash 冲突统计（可选，按需启用）。

## 架构设计

```mermaid
flowchart TD
  N[Netty Channel] --> Ctx[ConnectionContext (SSOT)]
  N --> D1[RespCommandDecoder]
  N --> D2[RespReplyDecoder (fast)]
  D1 --> H[CommandHandler]
  H --> E[NettyCommandExecutor]
  E --> P[YierdisFastCommandProcessor]
  P --> W[RespWriter]
  E --> O[Stats/Observability]
  O --> Cmd[INFO/STATS command]
  Ctx --> D1
  Ctx --> D2
  Ctx --> E
```

## Architecture Decision ADR

### ADR-20260117-01：以 `yierdis-bytes` 作为唯一 bytes 抽象，删除 off-heap api 的 alias（Breaking）

**Context：**
- 现状：存在多套 bytes 接口（alias 与新 bytes module 并存），依赖边界不清晰，容易在热路径误用与扩散。

**Decision：**
- `yierdis-bytes` 为 SSOT；删除 `YierdisBytesSink/YierdisBytesSource/YierdisDirectBytesSink` 等 alias，并全仓迁移到 `yier.bubu.redis.bytes.*`。

**Rationale：**
- 减少概念重复与依赖噪音，降低未来重构成本；让模块依赖方向更符合语义边界。

**Alternatives：**
- 方案 A：继续保留 alias 作为兼容层 → 拒绝原因：长期双轨必然扩散，维护成本持续上升。

**Impact：**
- 产生 Breaking change，需要同步更新变更记录与迁移说明；但对项目长期演进更有利。

### ADR-20260117-02：client/bench 回复解码收敛到与 server 同源的 fast-path/codec

**Context：**
- 现状：client/bench 使用 correctness-first decoder，分配与行为细节与 server codec 不一致，导致优化与一致性成本上升。

**Decision：**
- 将回复解码改为 frame-based fast decoder，与 server 的解析核心、限制参数与错误语义保持一致。

**Rationale：**
- 同源实现降低分叉成本；bench 作为性能工具应避免额外分配干扰测量。

**Alternatives：**
- 方案 A：继续保留当前 decoder，仅做局部优化 → 拒绝原因：仍然“双轨”，难以保证长期一致。

**Impact：**
- client/bench API 可能需要调整（从 `RespObject` 到 frame/视图），需要配套测试与使用说明更新。

### ADR-20260117-03：引入 `ConnectionContext` 作为连接态 SSOT，替换多 `Channel.attr`

**Context：**
- 现状：协议状态与执行器状态分别由多个 `AttributeKey` 维护，状态机演进时容易遗漏与产生不一致。

**Decision：**
- 新增单一 `ConnectionContext`，同时承载协议协商、执行器连接态与连接级统计；仅保留一个 `AttributeKey<ConnectionContext>`。

**Rationale：**
- 显式化连接态模型，减少隐式耦合；便于扩展可观测性与调试能力。

**Alternatives：**
- 方案 A：最小整理（继续多 attr，仅补充命名/封装）→ 拒绝原因：用户明确要求直接迁移。

**Impact：**
- 需要一次性迁移 server pipeline 与 executor 的状态读写；需补齐资源回收与竞态相关测试。

### ADR-20260117-04：内建可观测性以“命令查询 + 低开销计数器”为主

**Context：**
- 现状：缺少统一 stats/诊断入口；引入第三方 metrics 体系会扩大依赖面与配置复杂度。

**Decision：**
- 优先实现内建 stats：通过 `INFO/STATS` 提供可查询输出；内部使用低开销计数器/采样，按需启用更详细日志。

**Rationale：**
- 满足“可观测性优先”的同时控制复杂度与开销；适配当前项目的学习/研究属性。

**Alternatives：**
- 方案 A：引入 Micrometer/Prometheus → 拒绝原因：依赖与运维成本更高，超出当前目标。

**Impact：**
- 需要定义 stats 输出格式与兼容性策略；输出应避免大规模分配。

### ADR-20260117-05：`CommandRegistry` 使用 allocation-free hash 索引替代线性扫描

**Context：**
- 现状：线性扫描在命令增长时引入额外开销，也不利于做更细粒度统计。

**Decision：**
- 注册阶段构建 hash 索引；查找阶段基于 ASCII case-insensitive hash 进行 O(1) 查找，并在冲突时二次比对。

**Rationale：**
- 兼顾性能与可维护性；保持运行时零分配约束。

**Impact：**
- 需要新增/调整单元测试以覆盖大小写、冲突、未知命令等边界情况。

## 安全与性能

- **安全：**
  - 保持协议 DoS 上限（bulk/array/line/nesting 等）可配置且默认保守。
  - 错误消息继续做 CRLF 清洗，避免 response splitting。
  - 观测输出避免泄漏敏感信息（仅输出统计，不输出用户数据内容）。
- **性能：**
  - 回复解码与命令解码共享 fast-path，减少分配与拷贝。
  - ConnectionContext 的统计字段采用低开销结构（必要时按需启用更重的统计）。
  - 命令路由索引结构保持运行时零分配，避免引入新的热点。

## 测试与发布

- **测试：**
  - 增加/补齐 codec 相关测试：回复解码、限制参数、错误语义一致性。
  - 增加 ConnectionContext 迁移后的集成测试：背压进入/退出、closing 行为、资源回收。
  - 增加 CommandRegistry 新实现的单元测试：大小写、冲突、unknown 命令。
  - 运行全量 `mvn test`，并用 `yierdis-bench` 做基础对比（关注分配与吞吐变化）。
- **发布：**
  - 作为 Breaking refactor：更新 `helloagents/CHANGELOG.md` 与模块文档；必要时调整版本号策略（按项目约定执行）。
