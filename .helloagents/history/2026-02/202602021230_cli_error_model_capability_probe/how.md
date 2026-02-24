# Technical Design: CLI 统一错误模型与能力探测（Off-heap foreign 等）

## Technical Solution

### Core Technologies
- Java 17
- Maven（multi-module）+ shade 打包
- Netty（服务端/客户端网络层）
- picocli（server 参数解析）
- slf4j + logback（日志）

### Implementation Key Points

1. **统一错误模型（User error vs Bug）**
   - 定义一个“可预期配置错误”异常类型（建议放在 `yierdis-args`，便于 server/tools 复用），携带：
     - `exitCode`（默认 `2`）
     - 是否需要打印 usage（server 参数错误通常需要）
     - 面向用户的短错误信息（不含堆栈）
   - `ServerConfig.fromArgs(...)`：
     - 统一捕获 picocli `ParameterException` 与 `normalizeAndValidate()` 抛出的校验错误；
     - 输出：错误信息（stderr）+ usage（stderr）；
     - 行为：继续抛出 `IllegalArgumentException`（保持现有测试与调用者语义），但保证输出已完成。
   - `YierdisServer.main(...)`：
     - 对“可预期配置错误”与“启动期可预期错误（如 off-heap backend 不可用）”进行友好输出并 `System.exit(2)`；
     - 对未知异常不捕获（保留堆栈以便定位真实 bug）。

2. **能力探测（capability probe）**
   - 在 off-heap 创建入口处补齐“构建能力 + 运行能力”判断：
     - 构建能力：foreign provider class 是否存在（`Class.forName` 反射探测即可）。
     - 运行能力：`ModuleLayer.boot().findModule(\"jdk.incubator.foreign\")` 是否存在（仅基于字符串，避免编译期依赖）。
   - 对 `foreign` 的失败输出分层：
     - “未编译进来” → 引导 `mvn -Pforeign-memory ...`
     - “未启用模块” → 引导 `java --add-modules jdk.incubator.foreign ...`
   - 将 off-heap backend 初始化失败转换为“可预期配置错误”异常类型，由 `YierdisServer` 统一打印并退出。

3. **文档同步**
   - 更新 `README.md`：
     - 修正“未包含能力”列表（事务/命令覆盖等）与实现一致；
     - 明确 `foreign` 后端的构建与运行前置条件，并强调默认构建不可用时的提示行为。
   - 同步 `helloagents/wiki/*`（如存在对外能力清单/架构说明引用），并记录到 `helloagents/CHANGELOG.md`。

## Architecture Decision ADR

### ADR-001: 统一 CLI/启动错误模型并引入能力探测
**Context:** 目前参数校验与启动期能力缺失会产生静默退出或长堆栈，降低可用性与可维护性。  
**Decision:** 引入“可预期配置错误”异常类型与能力探测逻辑，将用户可修复的问题以稳定输出呈现；未知异常保持原样抛出。  
**Rationale:** 兼顾教学项目的简洁性与工程可用性；避免把真实 bug 误判为配置错误。  
**Alternatives:** 全局捕获所有异常并统一格式化输出 → 拒绝原因：会吞掉关键堆栈，降低调试效率。  
**Impact:** CLI 输出更稳定且可操作；对错误分类的边界需要通过测试与日志策略约束。

## Security and Performance
- **Security:**
  - 参数校验继续严格；拒绝非法组合，避免进入不可控状态。
  - 对“用户可预期错误”默认不输出堆栈，减少噪音与潜在信息泄露（同时保留日志必要信息）。
- **Performance:**
  - capability probe 仅在启动/初始化阶段执行，复杂度 O(1)；
  - 反射探测结果可缓存（如有必要），避免重复探测。

## Testing and Deployment
- **Testing:**
  - 为 `ServerConfig.fromArgs(...)` 增加覆盖：校验错误时也会打印 usage（捕获 stderr 输出）；
  - 为 off-heap foreign 不可用场景增加测试：确保错误信息可预期且不输出长栈（可采用单元测试或轻量进程集成测试，视实现落点而定）。
  - 运行 `mvn test` 作为回归基线。
- **Deployment:**
  - 默认构建：`mvn -DskipTests package`
  - foreign 构建：`mvn -Pforeign-memory -DskipTests package`
  - foreign 运行：`java --add-modules jdk.incubator.foreign -jar ... --offheapBackend foreign`

