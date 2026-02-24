<!-- migrated_from: history/2026-01/202601161551_arch_guardrails/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: 架构护栏与可观测性加固（arch_guardrails）

## Technical Solution

### Core Technologies
- Java 17（Maven multi-module）
- Netty 4.x（仅限 adapter/server/client/bench）
- `java.util.ServiceLoader`（off-heap 后端发现）
- JUnit4（回归测试）

### Implementation Key Points

1. **“边界解释”优先于“边界重命名”：**先通过文档与护栏把当前边界固定住，避免为了命名而引入大范围搬迁；同时预留“抽取 bytes 基础模块”的演进路径。
2. **off-heap 后端可观测：**在启动期输出 provider 发现结果与最终后端选择；当后端不可用时，报错提示清晰可操作。
3. **命令注册护栏：**增加最小命令集的回归测试（或启动期自检），让“遗漏注册”变成可见失败。
4. **连接级状态护栏：**补充测试，确保 `RespSession` 的状态存储为连接级（Netty `Channel.attr`），并覆盖多连接场景。
5. **内存口径统一：**保持 “估算但稳定、可解释” 的原则，补齐文档与测试覆盖，避免与实际行为漂移。

## Architecture Design

### 现状（已接受）
- `yierdis-protocol`：协议对象模型 + `RespWriter` + `RespFrame/RespSession` 抽象（Netty-free）
- `yierdis-protocol-netty`：codec/adapters（允许依赖 Netty）
- `yierdis-core`：DB + data structures + command processor（Netty-free）
- `yierdis-offheap-api`：bytes 抽象 + allocator API（Netty-free）
- `yierdis-offheap-*`：各后端实现（netty/unsafe/foreign）

### 可选演进（仅当后续确认需要时）
将通用 `YierdisBytesSource/YierdisBytesSink` 抽取为更底层的独立模块（例如 `yierdis-bytes`），使依赖语义更直观：`protocol` → `bytes`，`offheap-api` → `bytes`。

## Architecture Decision ADR

### ADR-20260116-04: 先加固护栏与可观测性，再决定是否抽取 bytes 模块
**Context:** 当前 module 边界已经满足“Netty-free SSOT”，但存在命名语义误导、SPI 可运维性与注册遗漏等退化风险。直接抽取新 module 虽可解决语义误导，但会触发多模块搬迁与 API 迁移成本。

**Decision:** 本轮优先实现“文档澄清 + 护栏 + 启动诊断 + 回归测试”，将抽取 `yierdis-bytes` 作为后续可选演进（需要进一步确认收益/成本）。

**Rationale:**
- 更低风险：先把退化风险变成可见失败（tests/self-checks）
- 更可验证：有了护栏后，再做 module 迁移更安全
- 维护成本更低：避免一次性引入大量 package/class 迁移

**Alternatives:**
- **Alternative A（立即抽取 bytes 模块）** → 拒绝原因：改动面过大，且当前通过文档/护栏已可达到主要目标
- **Alternative B（什么都不做）** → 拒绝原因：退化风险会随新增功能累积，排障成本上升

**Impact:**
- 启动期会增加一段轻量诊断输出（仅一次）
- CI 会增加少量回归测试用例（可接受）

## Security and Performance

- **Security:**
  - 启动诊断信息避免输出敏感数据（仅输出 backend/provider 类型与配置摘要）
  - provider 冲突/缺失时 fail-fast，避免 silent fallback 造成不可控行为
- **Performance:**
  - 运行时主路径不引入额外分配；护栏检查主要发生在启动/测试阶段
  - 日志输出仅在启动期执行一次

## Testing and Deployment

- **Testing:**
  - `mvn test` 全量回归
  - 新增：off-heap provider 发现测试、命令注册最小集测试、session 隔离测试
- **Deployment:**
  - `yierdis-server` shade 产物保持 `ServicesResourceTransformer`（保证 SPI resources 合并）
  - 如启用 `foreign-memory` profile，补充对应启动指引与验证用例
