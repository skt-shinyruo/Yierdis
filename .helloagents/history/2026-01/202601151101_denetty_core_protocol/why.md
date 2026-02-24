# Change Proposal: core/protocol 去 Netty 依赖（边界收敛）

## Requirement Background

当前项目的知识库（SSOT）里对模块依赖方向有明确约束：`yierdis-core` / `yierdis-protocol` 不应直接依赖 `io.netty.*`，以保证：

1. 核心语义（DB/数据结构/命令语义/RESP 语义）可在非 Netty 场景复用与测试；
2. Netty 仅作为 I/O 适配层存在，避免“协议/核心层被 Netty 类型渗透”导致的边界漂移；
3. 降低后续重构成本（例如替换网络框架、增加非 Netty 的基准/回放工具、抽离纯协议层）。

但代码现状中，`yierdis-core` 与 `yierdis-protocol` 仍存在对 Netty 类型/工具（例如 `ByteBuf`、`PlatformDependent`、`Channel` Attribute）的直接引用，导致：

- SSOT 与代码不一致（长期会误导后续改动）；
- 可维护性下降（协议层/核心层更难被单测隔离，依赖树更难收敛）；
- 可插拔性下降（off-heap/codec/协议状态与 Netty 耦合，难以替换实现）。

本变更选择“反向重构实现以对齐约束”（而非仅更新文档），以把代码边界重新拉回到 SSOT 所描述的方向。

## Change Content

1. **拆分 protocol：**
   - 保留 `yierdis-protocol` 作为 Netty-free 的 RESP SSOT（对象模型、命令抽象、写出逻辑的核心抽象）。
   - 引入独立的 Netty 适配模块（建议命名 `yierdis-protocol-netty`），承载 Netty codec / pipeline 相关实现。

2. **核心层去 Netty 类型渗透：**
   - 让 `yierdis-core` 不再直接引用 `io.netty.*`（包括 `ByteBuf` 与 `PlatformDependent`）。
   - 核心层与协议层之间通过“Netty-free 的 bytes source/sink 抽象”交互，保持低分配写出路径与二进制安全语义。

3. **off-heap 内存操作边界收敛：**
   - `yierdis-core` 内部的 Unsafe/off-heap 数据结构不再直接使用 Netty 的 `PlatformDependent`。
   - 统一通过 Unsafe 后端自身的能力（或其封装）完成 raw memory 读写与 copy，避免 Netty 内部 API 侵入核心层。

## Impact Scope

- **Modules:**
  - `yierdis-protocol`（职责收敛为 Netty-free SSOT）
  - `yierdis-core`（移除对 Netty 类型/工具的直接引用）
  - `yierdis-server` / `yierdis-client` / `yierdis-bench`（切换到新的 Netty 适配模块）
  - `yierdis-offheap-unsafe`（提供/封装 raw memory 操作能力，支撑 core 去 Netty 依赖）
- **Files:** 预计为多文件改动（协议/核心/适配层/测试/文档均会涉及）
- **APIs:** 对外命令语义不变；内部构造函数/类位置可能变化（需要同步更新测试与文档）
- **Data:** 无持久化数据结构变更（仍为内存数据库）

## Core Scenarios

### Requirement: core/protocol 不直接依赖 Netty
**Module:** protocol + core
核心语义与协议语义必须可在不引入 Netty 的情况下编译与单元测试。

#### Scenario: 仅 server/client/bench 引入 Netty
条件：只在 I/O 与 codec 适配层引入 Netty
- 预期：`yierdis-core` / `yierdis-protocol` 编译期无 `io.netty.*` 直接引用
- 预期：server/client/bench 仍可复用同一份协议/命令语义 SSOT

### Requirement: 低分配写出路径保留
**Module:** protocol + core + server
保持原有“命令执行直接写 RESP”的模式，不因抽象拆分引入大规模中间分配。

#### Scenario: bulk string 优先 slice 写出
条件：value 存储在 off-heap
- 预期：`GET/HGETALL/SMEMBERS/ZRANGE...` 等回复可继续使用 slice/sink 直接写出
- 预期：不强制把 off-heap 数据回拷到 heap `byte[]`

### Requirement: RESP2/RESP3 协议状态一致
**Module:** protocol + server
`HELLO 3` 后连接进入 RESP3 回复模式（最小子集），并可切回 RESP2。

#### Scenario: 连接级协议状态
条件：同一 TCP 连接多条命令连续执行
- 预期：`HELLO 3` 后，nil 回复使用 RESP3 null（`_`）
- 预期：协议状态在连接内保持，不因 handler/executor 切换而丢失

## Risk Assessment

- **Risk:** 大范围模块拆分/抽象调整导致行为回归（协议编码、对象生命周期、引用计数、off-heap 释放）
  - **Mitigation:** 以“保持行为不变”为最高优先级；分阶段重构；补充回归测试；全量 `mvn test` + `scripts/smoke.sh`
- **Risk:** bytes source/sink 抽象引入额外拷贝或热点开销
  - **Mitigation:** 设计为可利用 memoryAddress 的 fast-path；保持原有零拷贝/少拷贝路径
- **Risk:** 引用计数/生命周期管理错误导致 ByteBuf 泄漏或 UAF（use-after-free）
  - **Mitigation:** 明确 `RespCommand`/frame 的 ownership 与 release 责任；增加泄漏回归测试与严格断言

