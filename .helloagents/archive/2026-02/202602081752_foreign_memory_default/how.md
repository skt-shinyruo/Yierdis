<!-- migrated_from: history/2026-02/202602081752_foreign_memory_default/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: foreign-memory 默认可用（off-heap foreign）

## Technical Solution

### Core Technologies

- Maven profiles（activeByDefault）
- Java 17 module system（`--add-modules jdk.incubator.foreign`）
- 运行时能力探测 + 自重启（`ProcessBuilder`）

### Implementation Key Points

1. **默认构建包含 foreign 模块**
   - 在 `yierdis-offheap/pom.xml` 的 `foreign-memory` profile 增加 `<activeByDefault>true</activeByDefault>`，使 `yierdis-offheap/foreign` 默认进入 reactor。
   - 在 `yierdis-server/pom.xml` 的同名 profile 增加 `<activeByDefault>true</activeByDefault>`，使 `yierdis-offheap-foreign` 默认进入 server shaded jar 依赖集合。

2. **运行期自动补齐 `--add-modules`**
   - 在 `yierdis-server` 增加 `ForeignMemoryAutoModules`：
     - 当 `--offheapBackend` 为 `foreign` 且 boot layer 未解析 `jdk.incubator.foreign` 时触发；
     - 使用 `RuntimeMXBean#getInputArguments()` best-effort 复用当前 JVM 参数；
     - 通过 code source 推断 `-jar` 路径；无法定位时回退为 `-cp` + main class；
     - 使用 marker system property 避免潜在重启环路；
     - 子进程 `inheritIO()`，确保脚本/bench 重定向日志时子进程输出一致。

3. **文档与知识库同步**
   - README：更新 foreign 后端的构建/运行说明（默认构建已包含；可用 `-P!foreign-memory` 禁用；运行期建议显式 `--add-modules`，但支持自动补齐）。
   - wiki：同步 offheap 模块说明与变更历史。

## Security and Performance

- **Security:** 自动重启仅在用户显式选择 `--offheapBackend foreign` 时触发，避免默认路径引入额外行为。
- **Performance:** 自动重启最多一次；推荐生产环境仍显式添加 `--add-modules` 以避免一次重启开销。

## Testing and Deployment

- **Testing:** 运行 `mvn test` 覆盖默认构建下 foreign 模块编译/测试与 server 启动矩阵。
- **Packaging:** 运行 `mvn -DskipTests package` 验证 server shaded jar 默认包含 foreign 后端实现类。
- **Deployment:** 用户可直接运行 `java -jar ... --offheapBackend foreign`；如希望避免一次自动重启，可显式添加 `--add-modules jdk.incubator.foreign`。

