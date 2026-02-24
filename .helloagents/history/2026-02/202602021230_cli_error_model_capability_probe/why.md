# Change Proposal: CLI 统一错误模型与能力探测（Off-heap foreign 等）

## Requirement Background

当前项目作为“教学/演示导向”的 Redis 兼容实现，在可用性与可诊断性上暴露出一些明显问题，导致使用者在配置错误或能力缺失时很难快速定位原因：

1. **参数校验失败会静默退出**：部分非法参数组合（例如 `--offheapBackend netty --offheapKeysEnabled`）会触发校验异常并以退出码 `2` 退出，但缺少明确的错误提示与 usage 输出，影响调试与脚本集成。
2. **能力缺失在运行期才暴露且伴随长栈**：`--offheapBackend foreign` 在默认构建产物中通常不可用（需 Maven profile + JVM module 参数），但 CLI 仍允许选择，启动时抛出异常并打印堆栈，用户体验不佳，也会干扰定位真正的服务端 bug。
3. **文档与实现存在漂移**：README 对“已实现/未实现”范围的描述与代码现状不一致（例如事务相关能力、命令清单覆盖度），容易误导使用者与后续维护者。

本变更希望在不改变核心教学架构的前提下，显著提升“错误可诊断性”和“能力可预期性”，并同步文档 SSOT，降低误用成本。

## Change Content

1. 引入统一的 **CLI/启动错误模型**（区分“可预期用户配置错误”与“未知异常/bug”），统一错误输出、usage 行为与退出码。
2. 增加 **能力探测/预检（capability probe）**：
   - 对 off-heap backend（尤其 `foreign`）做“构建能力 + 运行能力”预检与可执行指引。
   - 对不合法参数组合给出清晰、可操作的提示（并保持退出码稳定）。
3. 同步 README 与知识库文档，保证“功能范围声明、已实现命令清单、限制说明”与代码一致。

## Impact Scope

- **Modules:**
  - `yierdis-args`（参数模型与校验、统一错误类型）
  - `yierdis-server`（参数解析/usage 输出、启动期错误处理）
  - `yierdis-offheap`（backend 可用性探测与错误信息）
  - 文档：`README.md`、`helloagents/wiki/*`、`helloagents/CHANGELOG.md`
- **APIs/Behavior:**
  - CLI 参数错误：输出更明确（新增/补齐 stderr 文本 + usage），退出码保持一致
  - `foreign` backend：默认构建下从“长栈失败”改为“可预期配置错误提示”

## Core Scenarios

### Requirement: CLI 错误可诊断性
**Module:** yierdis-server / yierdis-args

#### Scenario: 参数解析错误（未知参数/类型错误）
当用户传入无法解析的参数时：
- 输出：错误信息 + usage（便于直接修正）
- 行为：退出码稳定（保持 `2`），不输出堆栈

#### Scenario: 参数校验错误（合法参数但组合/范围非法）
当用户传入可解析但不合法的参数组合或范围时：
- 输出：错误信息 + usage（明确指出哪个参数/组合有问题）
- 行为：退出码稳定（保持 `2`），不输出堆栈

### Requirement: Off-heap backend 能力探测与友好失败
**Module:** yierdis-offheap / yierdis-server

#### Scenario: 默认构建选择 `--offheapBackend foreign`
当用户在默认构建产物中选择 `foreign` 后端：
- 输出：说明 foreign 后端未编译进当前构建，并提示使用 `-Pforeign-memory` 重新构建
- 行为：以“可预期配置错误”形式失败（退出码 `2`），不输出长栈

#### Scenario: 已编译 foreign 但运行缺少 `--add-modules`
当 foreign 后端已编译进 jar，但运行时缺少 `--add-modules jdk.incubator.foreign`：
- 输出：给出明确 JVM 参数提示
- 行为：以“可预期配置错误”形式失败（退出码 `2`），不输出长栈

### Requirement: 文档一致性（README/知识库 SSOT）
**Module:** Documentation

#### Scenario: README 与实现一致
更新 README 的“已实现命令”与“未包含能力”说明，使其与当前代码一致（例如事务支持、命令覆盖范围等）。

## Risk Assessment

- **Risk:** 过度捕获异常导致真实 bug 被误判为“配置错误”而失去堆栈  
  **Mitigation:** 仅捕获/转换明确的“用户输入/启动配置”错误类型；未知异常继续抛出并保留堆栈。
- **Risk:** CLI 输出变化可能影响依赖错误文本的脚本  
  **Mitigation:** 保持退出码与参数语义不变；错误输出尽量稳定且面向人类（脚本可依赖 exit code）。

