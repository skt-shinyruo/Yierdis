<!-- migrated_from: history/2026-01/202601081106_netty_executor_integration/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: Netty 执行器融合改造（覆盖问题 1/2/3/4/5）

## Technical Solution

### Core Technologies
- Java + Netty（服务端网络与管线）
- Netty `EventExecutorGroup(单线程)`（全局单线程命令语义执行）
- 现有 RESP2/部分 RESP3（`HELLO 3`）协议实现
- off-heap allocator（none/netty/unsafe）与现有 DB/数据结构

### Implementation Key Points
1. **执行模型融合（Solution 2 选择）**
   - 新增/替换为基于 `EventExecutorGroup(1)` 的命令执行器，确保 DB 访问仅发生在该单线程上（全局单线程语义）。
   - 处理跨线程写回：在执行线程生成响应 ByteBuf，通过 Netty 安全写回并采用 flush 合并策略。

2. **背压闭环**
   - 以“执行侧 backlog”作为核心信号：当 pending 命令数超过 high watermark，暂停该连接 `autoRead=false`；降到 low watermark 后恢复 `autoRead=true`。
   - 忙时策略：当 backlog 或队列容量超限时，拒绝接收新命令并返回 `-ERR busy`（并计入 bench errors）。
   - 保障：无论拒绝还是暂停读，都必须回收命令帧资源（避免 retained slice 泄漏）。

3. **flush 合并与 pipeline 友好**
   - 避免每命令 `writeAndFlush`；改为 `write` 聚合，并在“本次 drain 结束”或“达到批量上限”时 `flush`。
   - 保证响应顺序：同一连接内严格按请求顺序写回；全局单线程语义保证跨连接的顺序按执行先后确定。

4. **低分配与 off-heap 写入路径改造（聚焦 hot path）**
   - 优先覆盖 bench 热路径：`SET/GET`。
   - off-heap backend 下 `SET` 避免“先物化堆 `byte[]` 再拷贝到 off-heap”的双拷贝：提供从 `RespCommand` 的 frame slice 直接写入 off-heap buf/string 的能力。
   - 对 heap backend 保持语义一致（仍需要为存储创建独立 `byte[]`，但避免无意义的额外中转）。

5. **bench 正确性与 RESP3 基础类型兼容**
   - `RespResponseSkipper` 扩展支持 RESP3 的基础类型（例如 `%`/`_` 等）以便未来扩展不破坏压测工具。
   - 压测线程对响应进行最小化判断：遇到 `-` 错误即计入 errors；可选开启严格校验（例如期望 `SET` 返回 `+OK`、`GET` 返回 `$...`/`_`）。

6. **maxmemory/过期/淘汰稳定性**
   - 将淘汰/过期清理的触发与预算显式化：在写路径上限速执行（时间预算/步数预算），避免长尾；维护任务保证在 backlog 压力下仍可被执行（或降级为 opportunistic 策略但可观测）。
   - 保持语义：`noeviction` 返回 OOM 错误；`allkeys-lru/random` 在压力下主动淘汰并保持可解释行为。

## Architecture Decision ADR

### ADR-001: 用 Netty `EventExecutorGroup(单线程)` 统一命令执行
**Context:** 当前模型为 I/O 解码 + 自建阻塞队列执行线程。该模型存在 per-command flush、跨线程投递开销、背压闭环不足等问题，并在 pipeline 高压下容易出现吞吐与尾延迟劣化。  
**Decision:** 采用 Netty `EventExecutorGroup(1)` 作为全局命令执行线程，所有 DB 访问与命令执行均在该线程发生，并实现批量写回与连接级背压。  
**Rationale:** 与 Netty 的任务/写回/线程模型更一致，便于实现 flush 合并与读控制；同时保持 Redis 风格单线程语义。  
**Alternatives:**  
- Solution 1（增量式修复）→ 拒绝原因：仍需维护自建执行线程与队列，flush/背压与 Netty 语义割裂，整体可演进性较弱。  
- Solution 3（单线程 I/O 直执行）→ 拒绝原因：会强制改变并发模型与吞吐上限，且对未来扩展（多 I/O）不友好。  
**Impact:** 内部执行路径与背压策略发生改变；需要调整测试与 bench 以确保正确性与可对比性。

## Security and Performance
- **Security:**
  - 保持协议输入上限（bulk/line/argc）与错误信息净化（防 CRLF 注入）。
  - 拒绝/暂停读场景必须保证资源回收与连接可恢复，避免 DoS 形成内存泄漏。
- **Performance:**
  - 减少 per-command flush；引入批处理 drain。
  - off-heap 写入减少双拷贝（聚焦 SET 热路径）。
  - 引入滞回阈值减少 autoRead 频繁抖动。

## Testing and Deployment
- **Testing:**
  - 追加/调整单元测试：响应顺序、backpressure 触发与恢复、busy 错误统计、RESP3 基础类型跳过、off-heap 写入不泄漏。
  - 使用现有 `EmbeddedChannel` 测试覆盖 handler/executor 行为。
- **Deployment:**
  - 参数化配置：watermark、batch flush 上限、执行器线程数（固定 1）、是否启用 strict bench 校验。
  - 通过 `yierdis-bench` 做 A/B 对比（none/netty/unsafe）并记录 errors 与 p95。

