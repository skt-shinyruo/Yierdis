# Technical Design: redis_hardening

## Technical Solution

### Core Technologies
- Java 17
- Netty 4.x（RESP2 over TCP）
- JUnit4（现有测试框架）

### Implementation Key Points

1. **执行架构：I/O 与命令执行解耦**
   - 保持“单线程命令语义”（类似 Redis 主线程），新增一个单线程 `CommandExecutor` 作为唯一的 DB/命令执行入口。
   - Netty I/O worker 线程只做：RESP 解码 → 投递到 `CommandExecutor` → 异步写回响应。
   - 为避免积压导致 OOM，引入可配置的队列上限与拒绝策略（返回 `-ERR busy` / `-OOM` 风格错误）。

2. **重命令保护栏（防止执行器被长期占用）**
   - 对扫描/大返回类命令引入统一保护参数：
     - 最大遍历 key 数 / 最大返回元素数
     - 最大输出字节数（写回前估算或写回过程中计数）
     - 最大执行时间片（达到则返回错误并中止）
   - 默认值保持偏保守，作为“工程兜底”，避免单条命令长期占用执行器导致系统不可用。

3. **RESP 错误输出安全净化**
   - 在 RESP error 写出层做统一净化：过滤 `\\r`/`\\n`、限制最大长度、避免将不可信输入直接拼进 error。
   - unknown command 等错误改为“安全展示”：
     - 要么不回显用户输入
     - 要么回显经过净化与限长的可打印片段（可选：不可打印字节用 `?` 或 hex 概要表示）

4. **maxmemory 语义与统计**
   - 将 `maxmemoryBytes` 定义为“数据集内存预算”（用于淘汰/拒写判断），其统计由两部分构成：
     - heap 侧：对 key/value 元数据的估算（仅统计实际在 heap 上保留的部分）
     - off-heap 侧：直接使用 `YierdisOffHeapAllocator.usedBytes()` 作为真实占用
   - 关键点：**避免双计数**。当某类数据实际存放在 off-heap 时，heap 估算不再重复计入其 payload 长度。
   - 与 `--offheapMaxBytes` 的关系：
     - `offheapMaxBytes` 仍作为 hard cap，防止 native 内存失控；
     - `maxmemoryBytes` 作为整体预算（对用户更直觉），两者同时开启时分别生效。

5. **命令执行路径收敛（SSOT）**
   - 以写出式执行为唯一权威路径（统一走 `RespWriter`），将 `CommandProcessor` 降级为测试辅助或彻底移除。
   - 建立命令表驱动（命令名 → handler），减少 if/else 链，并统一参数校验、错误映射与安全净化。
   - 将现有测试逐步迁移为“端到端走 RESP pipeline”的测试，锁定协议与行为一致性。

6. **off-heap 内存安全兜底**
   - 增加“资源释放可回归验证”测试：在 Unsafe/Netty allocator 后端下跑一组操作序列，最后 `db.shutdown()` 后断言 `allocator.usedBytes()` 回到基线。
   - 对关键 close/free 路径补强：确保删除、覆盖、淘汰、过期清理、shutdown 都能释放对应 off-heap 资源。

## Architecture Design

```mermaid
flowchart TD
    C[Client / redis-cli] -->|RESP2 over TCP| N[Netty I/O]
    N -->|decode RespCommand| D[RespCommandDecoder]
    D -->|enqueue| X[CommandExecutor (single thread)]
    X -->|exec + build reply| P[Unified Command Processor]
    P -->|RespWriter writes ByteBuf| R[Reply]
    R -->|writeAndFlush| N
```

## Architecture Decision ADR

### ADR-001: 引入单线程 CommandExecutor 解耦 I/O 与执行
**Context:** 当前命令执行运行在 Netty I/O event-loop 上，重命令会阻塞 I/O；同时 DB 通过 `bindToCurrentThread` 强约束单线程访问。  
**Decision:** 增加单线程 `CommandExecutor`，作为唯一 DB/命令执行入口；I/O 线程只投递请求并异步写回。  
**Rationale:** 保持单线程语义的同时，避免 event-loop 承担重逻辑；为未来 I/O 多线程扩展铺路。  
**Alternatives:** 仅保留单 event-loop + 加限制 → 拒绝原因：无法彻底隔离 I/O 阻塞；遇到复杂逻辑仍会拖住连接生命周期。  
**Impact:** 引入队列/背压与跨线程生命周期管理（命令对象/ByteBuf 释放），需要新增测试覆盖。

### ADR-002: maxmemory 口径定义为“数据集预算（heap+off-heap）”
**Context:** 当前估算在 off-heap 模式下明显漏计；用户难以理解淘汰/拒写触发时机。  
**Decision:** `maxmemoryBytes` 口径覆盖数据集整体（heap 元数据估算 + off-heap 实占）；同时保留 `offheapMaxBytes` 作为 hard cap。  
**Rationale:** 更贴近用户直觉与 Redis 的“数据集预算”理念，同时避免 native OOM。  
**Alternatives:** 仅统计 heap 或仅统计 off-heap → 拒绝原因：与实际占用偏差过大，难以调参。  
**Impact:** 需要避免双计数并补齐 MEMORY USAGE/淘汰逻辑的一致性测试。

### ADR-003: 收敛为单一命令实现（写出式为 SSOT）
**Context:** `CommandProcessor` 与 `YierdisFastCommandProcessor` 并存，长期易漂移。  
**Decision:** 以写出式为唯一权威实现；对象式路径要么移除，要么仅保留测试用途且由同一底层实现驱动。  
**Rationale:** 降低维护成本与行为分叉风险；测试更贴近真实服务路径。  
**Alternatives:** 保留双实现并加强同步 → 拒绝原因：长期成本高，仍易出现遗漏。  
**Impact:** 需要重构测试与部分代码结构（命令表、统一错误映射）。

## Security and Performance

- **Security:**
  - RESP error 文本统一净化（防 CRLF 注入/响应拆分）
  - 输入上限与队列上限（DoS 防护）
  - 大返回命令限量/限时/限输出（资源防御）
- **Performance:**
  - I/O 线程池可配置（多连接场景更稳）
  - 命令执行单线程保持可预测性
  - 继续保留 RESP 解码的低分配路径（零拷贝 slice）

## Testing and Deployment

- **Testing:**
  - 端到端 RESP pipeline 测试：覆盖命令语义与错误风格（包括 CRLF 注入尝试）
  - maxmemory + off-heap 后端下的触顶/淘汰/拒写测试
  - off-heap allocator 泄漏回归测试（shutdown 后 usedBytes 归零/回基线）
  - 执行器队列满的 backpressure 测试（返回 `ERR busy` 等）
- **Deployment:**
  - 先跑 `mvn test`；再用 `redis-cli --resp2` 做手工回归（PING/SET/GET/EXPIRE/TTL/KEYS 等）
