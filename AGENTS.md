# Repository Guidelines

## Project Structure & Module Organization
本项目是一个参考 Redis 思路实现的 Java 版内存 KV 服务端，主要用于教学和演示；它借鉴 Redis 的数据结构、命令子集、TTL、事务和 maxmemory 思路，但对外使用自定义 `Custom Protocol v1`，不是 Redis 的 drop-in replacement。`pom.xml` 是 Java 17 多模块构建的父聚合工程。核心契约、API、数据库逻辑、命令处理和运行时装配位于 `yierdis-core/`；协议与编解码位于 `yierdis-protocol/`；off-heap 后端位于 `yierdis-memory/`；字节工具位于 `yierdis-bytes/`；可执行应用位于 `yierdis-server/`、`yierdis-client/`、`yierdis-bench/`。共享的参数解析和执行器代码位于 `yierdis-args/` 与 `yierdis-executor-core/`。

各模块都使用标准 Maven 目录：`src/main/java` 与 `src/test/java`。设计说明和实现计划放在 `docs/superpowers/`。端到端验证优先使用 `scripts/smoke.sh` 与 `scripts/bench.sh`，不要只依赖单元测试。

保持当前边界不回退：契约在 `yierdis-core-contract`，协议模型与 codec 留在 `yierdis-protocol-*`，Netty 与面向 server 的装配留在 `yierdis-server`。

## Build, Test, and Development Commands
`mvn test`：运行全仓测试并生成 JaCoCo 报告。

`mvn -DskipTests package`：构建全部模块，并产出 server、client、bench 的可执行 jar。

`mvn -pl yierdis-server -am test`：只跑指定模块及其依赖；按需替换模块路径。

`mvn -P!foreign-memory test`：在没有 `jdk.incubator.foreign` 的环境中关闭 `foreign-memory` profile。

`java -jar yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --port 6378`：本地启动服务。随后执行 `java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar PING` 做最快速手工校验。

## Coding Style & Naming Conventions
使用 4 空格缩进、标准 Java 大括号风格，包名保持在 `yier.bubu.redis.*` 下。命名沿用现有模式：领域类型使用 `YierdisInstance` 这类明确名称，辅助类聚焦单一职责，测试类以 `*Test`、`*IntegrationTest`、`*GuardTest` 结尾。

仓库当前没有专门的 formatter 配置。保持 import 干净、方法职责集中、注释精简。不要把 server 专属逻辑重新塞回 core 模块；已有 `maven-enforcer-plugin` 和边界测试会拦截一部分回退。

## Testing Guidelines
测试应写在行为所属模块中。优先补窄范围单元测试；如果改动涉及协议分帧、Netty pipeline 或跨模块装配，再补集成测试。

定向验证可使用 `mvn -pl yierdis-core/yierdis-core-runtime -Dtest=SetCommandTest test` 这类命令。每个 bugfix 都应附带回归测试。仓库级 smoke 验证使用 `./scripts/smoke.sh`。

## Commit & Pull Request Guidelines
最近提交历史偏好简短、祈使句式的主题，通常带 `fix:`、`refactor:`、`docs:` 等前缀，例如 `fix: preserve set get wrongtype semantics`。

每个提交只处理一个清晰主题。提交 PR 时说明影响的模块，列出已执行的验证命令；如果对外行为有变化，补充 CLI 输出或协议交互示例。
