<!-- migrated_from: history/2026-02/202602021230_cli_error_model_capability_probe/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: CLI 统一错误模型与能力探测（Off-heap foreign 等）

Directory: `helloagents/plan/202602021230_cli_error_model_capability_probe/`

---

## 1. CLI 错误模型（SSOT）
- [√] 1.1 设计并新增“可预期配置错误”异常类型（包含 exit code/是否打印 usage 等元信息），落地在 `yierdis-args`，验证 why.md#requirement-cli-错误可诊断性-scenario-参数校验错误
- [√] 1.2 在 `yierdis-server` 的参数解析路径中统一捕获 parse/validate 错误并打印 stderr + usage，涉及 `yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java`，验证 why.md#requirement-cli-错误可诊断性-scenario-参数解析错误

## 2. 启动期错误处理（不吞真实异常）
- [√] 2.1 在 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java` 增加对“可预期配置错误”的友好输出与退出码处理，未知异常保留堆栈，验证 why.md#requirement-cli-错误可诊断性-scenario-参数校验错误
- [√] 2.2 将 off-heap backend 初始化失败（foreign 不可用/未启用模块）转换为“可预期配置错误”，落点优先在 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`，验证 why.md#requirement-off-heap-backend-能力探测与友好失败-scenario-默认构建选择---offheapbackend-foreign

## 3. Off-heap capability probe
- [√] 3.1 为 `foreign` 增加构建能力探测（provider class 是否存在）与运行能力探测（`jdk.incubator.foreign` module 是否启用），涉及 `yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapAllocators.java`，验证 why.md#requirement-off-heap-backend-能力探测与友好失败-scenario-已编译-foreign-但运行缺少---add-modules
- [√] 3.2 校准错误信息：给出可执行指令（profile + JVM 参数），并确保默认路径不会输出长栈，验证 why.md#requirement-off-heap-backend-能力探测与友好失败-scenario-默认构建选择---offheapbackend-foreign

## 4. 文档一致性（README/知识库）
- [√] 4.1 更新 `README.md` 的“已实现命令/未包含能力”描述，使其与当前实现一致（例如事务能力、命令清单覆盖范围），验证 why.md#requirement-文档一致性（readme-知识库-ssot）-scenario-readme-与实现一致
- [√] 4.2 同步知识库（至少更新相关模块/架构说明与变更记录），涉及 `helloagents/wiki/*` 与 `helloagents/CHANGELOG.md`，验证 why.md#requirement-文档一致性（readme-知识库-ssot）-scenario-readme-与实现一致

## 5. Security Check
- [√] 5.1 执行安全检查：确认不捕获/吞掉未知异常；错误输出不泄露敏感信息；输入校验完整（按 G9），并记录结论到 `helloagents/CHANGELOG.md`

## 6. Testing
- [√] 6.1 增补/调整单元测试：覆盖“校验错误也会打印 usage 且退出码稳定”的行为（可通过捕获 stderr），涉及 `yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java` 或新增测试文件
- [√] 6.2 回归验证：运行 `mvn test`，并手工验证关键 CLI 场景（foreign 不可用、非法参数组合）输出符合预期
