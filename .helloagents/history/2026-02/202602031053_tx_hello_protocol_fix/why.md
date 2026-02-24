# Change Proposal: TX HELLO 协议切换修复 + 文档 SSOT 同步

## Requirement Background

当前实现的事务模型为“最小实现”：
- `MULTI` 开启连接级事务态；在 MULTI 模式下，除 `MULTI/EXEC/DISCARD` 外的命令会被直接入队并返回 `QUEUED`。
- `EXEC` 在同一个连接回复里（复用同一个 `RespWriter`）按顺序执行队列中的命令，并输出一个“数组/列表”作为结果容器。

问题在于 `HELLO 3` 属于**连接级协议协商命令**：它会立即将连接切换为 RESP3，并写出 RESP3 的 `map`（`%...`）。
当 `HELLO 3` 被允许出现在事务队列中时，`EXEC` 的外层回复仍可能是 RESP2（数组 `*...`），但数组内部却会写出 RESP3 的 `%` 前缀，
导致 RESP2 客户端在解析 `EXEC` 的数组元素时直接失败，产生“协议流被破坏”的风险（且一旦发生，后续请求/响应也无法可靠继续）。

同时，知识库（`helloagents/wiki/*`）的 `api.md` 与实际代码实现已出现漂移，容易造成使用者按文档排障/学习时得出错误结论，
削弱项目作为教学/演示的核心价值。

## Change Content

1. **事务协议护栏（核心修复）**：在 MULTI 模式下禁止 `HELLO`（含 `HELLO 2/3`）入队与执行；返回明确错误并将事务标记为 aborted（`EXECABORT`），确保不会出现混合协议输出。
2. **回归测试补齐**：新增覆盖 “HELLO in MULTI/EXEC” 的 fast pipeline 回归测试，锁定字节级输出与协议状态不被破坏。
3. **知识库 SSOT 同步**：更新 `helloagents/wiki/api.md`（必要时补充 `overview.md`/相关模块文档）以与代码实现保持一致，并明确：
   - RESP3 的定位为“reply 侧最小子集”，request 仍以 RESP2 multi-bulk 为主路径
   - 单线程执行器下 O(N)/大输出命令的性能边界与 `-ERR busy <reason>` 行为
   - `maxmemory` 与 `off-heap` 的预算口径差异与配置踩坑提示

## Impact Scope

- **Modules:** `yierdis-protocol` / `yierdis-core` / `yierdis-server` / `helloagents/wiki`
- **Files (expected):**
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespTransactionState.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionState.java`
  - `yierdis-server/src/test/java/yier/bubu/redis/*Test.java`
  - `helloagents/wiki/api.md`（必要时补充 `helloagents/wiki/overview.md`）
- **APIs (Redis-style commands):** `MULTI/EXEC/DISCARD/HELLO`
- **Data:** None

## Core Scenarios

### Requirement: TX_HELLO_PROTOCOL_GUARD
**Module:** command / protocol / server
在事务模式下，禁止 `HELLO` 导致的协议切换与 RESP 容器类型变化，避免 `EXEC` 回复混入 RESP3 前缀破坏 RESP2 解析。

#### Scenario: MULTI 中执行 HELLO 3（RESP2 连接）
条件：连接当前为 RESP2，已进入 MULTI
- 预期：`HELLO 3` 返回 `ERR`，且不会返回 `QUEUED`
- 预期：事务被标记为 aborted；后续 `EXEC` 返回 `EXECABORT ...` 并丢弃队列
- 预期：连接协议保持 RESP2（例如后续 `GET missing` 仍返回 RESP2 null bulk string，而不是 RESP3 null）

#### Scenario: MULTI 中执行 HELLO 2（RESP3 连接）
条件：连接当前为 RESP3（此前已执行 `HELLO 3`），已进入 MULTI
- 预期：`HELLO 2` 同样返回 `ERR` 并将事务标记为 aborted
- 预期：避免在 `EXEC` 中途切回 RESP2 导致后续命令输出形态漂移（map/set/attribute 等）

### Requirement: DOCS_SSOT_SYNC
**Module:** helloagents/wiki
知识库的 API/行为描述必须与代码实现一致，避免教学与排障产生误导。

#### Scenario: api.md 与实现一致
条件：对照当前实现的命令注册表与行为
- 预期：`api.md` 覆盖 `COMMAND` 的最小子集、`SELECT <index>` 范围、多 Key/TTL 命令集合（含 `SCAN/PTTL/...`）与 `KEYS` glob 支持范围
- 预期：明确标注“与 Redis 的差异/简化点”

### Requirement: PERFORMANCE_BOUNDARY_CLARITY
**Module:** helloagents/wiki
明确单线程执行模型下的大输出/O(N) 风险与排障手段，避免误判为 bug。

#### Scenario: 高压与 busy 行为说明
条件：命令执行慢或输出过大导致 backlog 增长
- 预期：文档说明 `-ERR busy <reason>` 的触发语义与排障建议（配合 `INFO/STATS`）

### Requirement: RESP3_REQUEST_BOUNDARY_CLARITY
**Module:** helloagents/wiki
明确 RESP3 支持边界（reply 侧最小子集；request 仍以 RESP2 multi-bulk 为主）。

#### Scenario: RESP3 request 不作为兼容目标
条件：客户端发送 RESP3 request 形态
- 预期：文档明确该用例可能不兼容（decoder 会判 protocol error），并给出推荐做法（使用 redis-cli 的常规 multi-bulk request）

### Requirement: OFFHEAP_MAXMEMORY_CLARITY
**Module:** helloagents/wiki
明确 maxmemory 与 off-heap 的预算口径与配置建议，避免“以为有限制但实际无限增长”的误解。

#### Scenario: offheapMaxBytes=0 的风险提示
条件：启用 off-heap backend 且未配置 `--offheapMaxBytes`
- 预期：文档明确 off-heap 无硬上限；`--maxmemoryBytes` 不能替代 `--offheapMaxBytes`

## Risk Assessment

- **Risk:** 行为变化：原先 `HELLO` 可在 MULTI 下入队（`QUEUED`），修复后将返回错误并导致 `EXECABORT`。
  - **Mitigation:** 通过单测锁定行为；在知识库明确该兼容性边界；强调该改动用于保证协议安全与可解释性。
- **Risk:** 新增事务 aborted 控制点可能影响其他 MULTI 交互路径。
  - **Mitigation:** 仅对 `HELLO` 引入明确规则；其余命令保持既有行为；补齐回归测试覆盖。

