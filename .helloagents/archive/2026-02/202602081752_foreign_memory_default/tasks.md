<!-- migrated_from: history/2026-02/202602081752_foreign_memory_default/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: foreign-memory 默认可用（off-heap foreign）

Directory: `helloagents/plan/202602081752_foreign_memory_default/`

---

## 1. Build（默认构建包含 foreign）
- [√] 1.1 将 `yierdis-offheap/pom.xml` 中 profile `foreign-memory` 设为 activeByDefault（默认构建包含 `foreign` 模块）
- [√] 1.2 将 `yierdis-server/pom.xml` 中 profile `foreign-memory` 设为 activeByDefault（默认 shaded jar 依赖包含 `yierdis-offheap-foreign`）

## 2. Runtime（自动补齐 --add-modules）
- [√] 2.1 新增 `ForeignMemoryAutoModules`（`yierdis-server/src/main/java/yier/bubu/redis/ForeignMemoryAutoModules.java`），在选择 foreign 且模块未启用时自动重启补齐
- [√] 2.2 在 `YierdisServer.main` 启动前调用自动补齐逻辑（`yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`）

## 3. Documentation / Knowledge Base
- [√] 3.1 更新 `README.md`：foreign 后端默认构建/禁用方式/运行期行为说明
- [√] 3.2 更新 `helloagents/wiki/modules/offheap.md`：同步 foreign 默认可用与自动补齐说明，并补充变更历史
- [√] 3.3 更新 `helloagents/CHANGELOG.md`：记录 foreign 默认可用性增强

## 4. Diagnostics（错误提示一致性）
- [√] 4.1 更新 `YierdisOffHeapAllocators` foreign 缺失实现时的提示文本（保留 `foreign-memory` 引导与禁用提示）

## 5. Verification
- [√] 5.1 运行 `mvn test`（默认构建全量测试通过）
- [√] 5.2 运行 `mvn -DskipTests package`（默认构建可打包）

