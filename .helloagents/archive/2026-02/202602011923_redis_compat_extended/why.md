<!-- migrated_from: history/2026-02/202602011923_redis_compat_extended/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: Redis 兼容性扩展（RESP3 / 多 DB / 事务 / PubSub / 持久化 / ACL/TLS）

## Requirement Background

当前项目是一个教学导向的 Redis-compatible server（Java 17 + Netty），以 RESP2 为默认协议，并支持通过 `HELLO 3` 切换为 RESP3 的最小回复子集（`README.md`、`yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`）。

现状的主要问题是：虽然“能用 `redis-cli` 跑通基础命令”，但对于真实 Redis 生态（客户端驱动、连接池、ORM/缓存框架、运维工具）来说，很多“默认假设”并不成立，导致兼容性与可预测性不足：

1. 协议/命令兼容性仍是子集：RESP3 主要是“回复形态切换”，对更多 RESP3 类型、push 消息、常见命令族的支持不足；`SELECT` 仅支持 DB0。
2. 内存预算/统计是 best-effort，且存在多套口径（DB 内存估算 vs 命令层预估 vs 执行器 backlog bytes vs off-heap 上限等），容易误配并出现“频繁 busy / OOM”。
3. Netty retained-slice 的生命周期与 executor backlog 绑定，存在“小命令 pin 住大 cumulation buffer”的驻留风险。
4. TTL 清理与淘汰策略是近似实现，对极端分布（大量短 TTL、热点写入、超大 keyspace）行为不可预测。
5. 串行执行与错误处理偏教学可控，与 Redis 生态常见预期（错误码、关闭行为、可观测性）不完全一致。

补充：在进一步阅读现有实现后，还发现一些“会在真实生态中踩坑”的具体差异点（属于兼容性/可运维性问题，而非单纯缺少功能）：

- `INFO` 输出形态与 Redis 不一致：当前 `INFO`/`STATS` 由 `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java` 输出为 RESP2 array / RESP3 map（key/value 对），而 Redis `INFO` 通常是 bulk string（文本分节）。部分工具/脚本会因此解析失败。
- `HELLO` 对额外参数是“静默忽略”：`yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java` 的 `hello(...)` 只处理协议版本切换，对 `AUTH`/`SETNAME` 等参数不校验也不执行，可能导致客户端误以为已生效（尤其在后续加入认证功能时会形成安全误导）。
- `HELLO 3` 的 reply 字段类型与 Redis 预期不一致：`ServerCommands.hello(...)` 在 RESP3 map 中把 `proto` 写为 bulk string（"3"），而 Redis 的 `proto` 更常见是 integer 类型；同时缺少 `id`、`modules` 等字段会让部分客户端/工具做能力探测时产生误判（需要“明确不支持并返回错误”或“按约定补齐”）。
- `SELECT` 的参数解析过窄：当前仅接受单字节 `'0'`，`SELECT 00` 之类在 Redis 中可解析的输入会失败。
- `FLUSHDB` 目前不做 arity/选项校验：会导致兼容行为不确定（Redis 支持 `ASYNC|SYNC`），也会让“多 DB 扩展”时语义更难收敛。
- `EXPIRE` 对 `seconds<=0` 与超大值溢出缺少对齐：当前实现把负数/0 归一为“立刻过期时间戳”，但不一定做到“命令后立即删除”；此外 `seconds*1000` 存在 long 溢出风险。
- `OBJECT ENCODING` 的回复类型不一致：当前输出为 simple string，而 Redis 习惯用 bulk string 返回非状态类字符串（可能影响严格解析的客户端/测试）。
- 缺少 `SCAN` 及其家族命令：很多运维/框架在遍历 keyspace 时依赖 `SCAN`（而不是 `KEYS`）。缺失会导致生态工具回退到 `KEYS` 并在大 keyspace 下触发慢命令/阻塞风险。
- 现有命令的参数校验与错误文本有多处“与 Redis 不同但又容易踩坑”的细节：例如 `SET` 的 `NX/XX` 冲突目前是“后者覆盖前者”（`yierdis-core/src/main/java/yier/bubu/redis/command/StringCommands.java`），`EX/PX` 对负数/溢出缺少一致的拒绝策略；数值解析错误目前会带上 label（`value is not an integer or out of range: seconds`），与 Redis 常见报错（不带 label）不同，影响依赖字符串匹配的工具/脚本与部分严格测试。
- 连接关闭/异常路径一致性不足：解码错误会直接关闭连接，但 server 未显式标记该连接为“closing”，已入队命令可能仍会在 executor 中执行（副作用仍发生但 reply 丢失），需要统一策略。
- `MEMORY STATS` 的 value 类型与 Redis 不一致：当前 `yierdis-core/src/main/java/yier/bubu/redis/command/KeyCommands.java` 在 RESP2/RESP3 下都把数值写为“十进制 bulk string”，而 Redis 通常返回 integer 类型；这会影响严格类型检查的工具/测试。
- 内置 client/CLI 的 RESP3 reply 覆盖不足且不支持 push：`yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespDecoder.java` 目前不识别 RESP3 set（`~`）、boolean（`#`）、double（`,`）、push（`>`）等类型；`yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java` 还是严格的 1-request-1-response 模型，一旦引入 PubSub push，很容易发生响应错配/客户端被动断连；因此协议扩展必须同步扩展内置 client 与测试基础设施，否则会“server 支持但工具不可用”。
- README/知识库与代码存在漂移：例如 `README.md` 仍描述 `COMMAND` 返回空数组、`KEYS` 仅支持简单 glob，但代码与 `helloagents/wiki/modules/*.md` 已发生演进；长期会造成使用者误判与生态工具配置错误。

本方案（Solution 2）目标是将项目从“教学 demo 子集”扩展到“更接近 Redis 的兼容扩展版”，优先解决与主流客户端互操作性最相关的缺口，并用可配置/可观测能力降低误用成本。

## Change Content

1. 扩展 RESP3：完善 `HELLO`/协议协商后的回复类型覆盖（不仅 map/set/null），为 PubSub 的 push 消息与更丰富的返回结构铺路。
2. 支持多 DB：实现 `SELECT 0..N-1`（可配置 `--databases`），并将 `FLUSHDB/INFO` 等语义与“当前 DB”对齐。
3. 增强 Redis 生态命令面：补齐最常被客户端/框架隐式依赖的命令（事务 MULTI/EXEC、PubSub、CONFIG/CLIENT、AUTH/ACL 基础能力等），并明确兼容边界。
4. 引入可选持久化：实现教学可控的最小 AOF（append-only file）能力（可选开关），保证“重启后可恢复”。
5. 提升资源模型可解释性：统一预算口径并提供 `INFO/CONFIG` 可观测输出，降低误配；同时强化 retained-slice 风险控制策略。
6. 收敛错误语义：将 “busy / protocol error / internal error / OOM” 等错误文本与行为向 Redis 常见约定靠拢，并保持 transport 层与 executor 层处理一致。
7. 补齐生态关键行为对齐：修复/补齐 `HELLO/INFO/SCAN/EXPIRE/OBJECT/FLUSHDB` 等“生态常用但目前实现存在差异”的具体语义，避免客户端/工具踩坑。
8. 工具与文档同步演进：扩展内置 client/CLI 与解析器以支持扩展后的 RESP3（含 push），并修复 README/知识库的漂移，降低回归与排障成本。

## Impact Scope

- **Modules:**
  - `yierdis-protocol`（RESP3 类型与写出能力）
  - `yierdis-protocol-netty`（必要时扩展 codec/帧策略）
  - `yierdis-core`（DB 多实例路由、事务、PubSub、持久化、淘汰/过期优化、命令实现）
  - `yierdis-server`（连接态扩展：DB index / MULTI 状态 / subscriptions；执行器/背压/错误处理收敛；可选 TLS）
  - `yierdis-args`（新增参数：databases/aof/tls/acl 等）
  - `yierdis-client`（内置调试 client/CLI：需要跟进 RESP3 类型与 push）
- **Files (expected):**
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespWriter.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespSession.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/*Commands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/*`
  - `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`
- **APIs:** 以 Redis 命令接口为准（非 HTTP）。新增/扩展命令族：`SELECT`、`MULTI/EXEC/DISCARD`、`SUBSCRIBE/PUBLISH`、`AUTH/ACL`、`CONFIG` 等。
- **Data:** 新增可选 AOF 文件（本地文件系统）；新增配置项（databases、tls、auth 等）。

## Core Scenarios

### Requirement: RESP3 Compatibility Expansion
**Module:** protocol / command
在 `HELLO 3` 后支持更完整的 RESP3 回复类型与必要的 push 机制，保证主流 RESP3 客户端不会因为“类型缺失/形态不符”而报错或退化。

#### Scenario: HELLO 3 handshake and reply types
客户端发送 `HELLO 3` 后：
- 服务端切换为 RESP3 回复，并对常见命令返回 RESP3 语义一致的类型（null、map、set、boolean、double、blob error/verbatim 等按需覆盖）。
- `COMMAND`/`INFO`/`CONFIG` 在 RESP3 下返回结构化数据时优先使用 map（可选兼容模式保持 RESP2 形态）。

### Requirement: Multi-DB Support
**Module:** server / core-db / command
支持多 DB（Redis 常见默认 16），连接级维护当前 DB index，命令针对当前 DB 执行。

#### Scenario: SELECT switching affects subsequent commands
连接执行：
- `SELECT 2` 返回 `OK`
- `SET k v` 仅写入 DB2；`SELECT 0` 后 `GET k` 返回 nil
- `FLUSHDB` 仅清空当前 DB；`FLUSHALL`（若实现）清空所有 DB

### Requirement: Transactions (MULTI/EXEC)
**Module:** command / server-session
补齐事务基本语义，满足常见客户端“探测/使用 MULTI”的假设。

#### Scenario: MULTI queues commands and EXEC applies atomically
- `MULTI` 进入事务态，后续写命令返回 `QUEUED`
- `EXEC` 以单线程语义按队列顺序执行并返回结果数组
- `DISCARD` 清空队列并退出事务态

### Requirement: PubSub + RESP3 Push
**Module:** server / command / protocol
支持 PubSub 命令族并在 RESP3 下以 push 形式下发消息，在 RESP2 下保持数组回复兼容。

#### Scenario: SUBSCRIBE then PUBLISH delivers messages
- 连接 A `SUBSCRIBE ch` 后收到订阅确认
- 连接 B `PUBLISH ch hello` 后，连接 A 收到消息推送（RESP2/RESP3 形态按协议协商）

### Requirement: Minimal Persistence (AOF)
**Module:** server / core
提供最小可用 AOF：开启后对写命令追加记录，启动时回放恢复；并提供基本的 fsync 策略配置。

#### Scenario: Restart restores dataset when AOF enabled
- 开启 AOF 后写入若干 key
- 重启 server 后数据恢复一致（在明确的兼容边界内）

### Requirement: ACL/TLS Baseline
**Module:** server / args / command
提供最小安全基线：`AUTH`（requirepass）/（可选）`ACL` 基础能力；以及可选 TLS listener（Netty SSL handler）。

#### Scenario: AUTH required blocks commands until authenticated
- 配置 `--requirepass` 后，未认证连接执行写命令返回 NOAUTH
- `AUTH <password>` 成功后恢复正常执行

### Requirement: Ecosystem Command Surface Compatibility (INFO/CONFIG/CLIENT/COMMAND/SCAN)
**Module:** command / server / protocol
补齐 Redis 生态里“客户端/工具经常隐式依赖”的命令面与输出形态，避免因 reply shape 不一致导致解析失败或错误降级。

#### Scenario: redis-cli / tooling expects Redis-like INFO
条件：客户端执行 `INFO`（可带 section，也可不带）
- 预期：RESP2 下返回 bulk string（包含基本 section 格式）；RESP3 下仍可被客户端正确解析（可选保持 bulk string 或提供 map，但必须明确一致的兼容策略）
- 预期：保留 yierdis 的结构化可观测输出能力（可通过 `STATS` 或 `INFO YIERDIS` 等方式提供），避免破坏现有排障能力

#### Scenario: keyspace traversal uses SCAN instead of KEYS
条件：工具/脚本通过 `SCAN 0 MATCH pattern COUNT n` 遍历
- 预期：支持 cursor 迭代返回（cursor + keys 数组），并在过期 key/rehash 等情况下保持“最佳努力”的一致性与不崩溃

### Requirement: Semantics Alignment for Existing Commands (HELLO/EXPIRE/OBJECT/FLUSHDB)
**Module:** command / db / server
对现有已实现命令做语义对齐，避免“看起来支持但行为细节不一致”造成生态兼容问题。

#### Scenario: HELLO options must be applied or rejected
条件：客户端发送 `HELLO 3 AUTH user pass` 或 `HELLO 3 SETNAME x`
- 预期：支持的选项必须真实生效；不支持的选项必须返回明确错误（禁止静默忽略）

#### Scenario: EXPIRE seconds<=0 and overflow handling
条件：客户端执行 `EXPIRE key 0` 或超大 seconds
- 预期：行为与 Redis 对齐（0/负数按约定删除或立刻失效），并避免 long 溢出导致的异常 TTL 行为

#### Scenario: OBJECT ENCODING returns bulk string
条件：客户端执行 `OBJECT ENCODING key`
- 预期：返回 bulk string（而非 simple string），并在 key 不存在时返回 nil

#### Scenario: FLUSHDB arity/options are validated
条件：客户端执行 `FLUSHDB` 或 `FLUSHDB ASYNC`
- 预期：参数合法时执行；不合法时返回语法错误或 wrong arity（保持与 Redis 接近的错误风格）

## Risk Assessment

- **Risk:** 范围过大导致交付不可控、兼容行为不一致、性能回退、测试成本陡增。
  - **Mitigation:** 明确“必须兼容的客户端/命令集合”与分阶段里程碑；每个阶段都有可运行/可回滚的最小闭环（协议/命令/测试/文档）。
- **Risk:** 协议与错误语义变化可能破坏现有测试/使用方式。
  - **Mitigation:** 引入兼容模式开关（例如保持 RESP2 shape/错误文本），并以集成测试覆盖 redis-cli 与典型 client 行为。
- **Risk:** AOF/TLS/ACL 涉及文件与密钥配置，易产生安全/运维误用。
  - **Mitigation:** 默认关闭；提供明确文档；避免把敏感信息写入日志；参数校验与安全默认值。
- **Risk:** `INFO/SCAN` 等生态命令的实现若不严格，会导致工具误判或性能问题（例如 scan 退化为全量复制）。
  - **Mitigation:** 明确“兼容边界 + 性能上限 + 降级策略”，并在测试/bench 中覆盖关键场景（大 keyspace、短 TTL、rehash）。
