# Task List: core/protocol 去 Netty 依赖（边界收敛）

Directory: `helloagents/history/2026-01/202601151101_denetty_core_protocol/`

---

## 1. protocol（Netty-free SSOT）
- [√] 1.1 设计并实现 Netty-free 的协议状态接口（连接级 RESP2/RESP3），调整 `RespProtocol` 与 `RespWriter` 的构造/使用方式，验证 why.md#requirement-core-protocol-不直接依赖-netty
- [√] 1.2 重构 `RespWriter` 为 bytes sink 写入（基于 `yierdis-offheap-api`），保留 CRLF 过滤与限长逻辑，验证 why.md#requirement-低分配写出路径保留
- [√] 1.3 将 `yierdis-protocol` 的 Maven 依赖收敛为 Netty-free（移除 `netty-all` compile 依赖），确保模块编译通过

## 2. protocol-netty（Netty adapter）
- [√] 2.1 新增 Maven 模块 `yierdis-protocol-netty`（父 pom 增加 module + 新增 pom），用于承载 Netty codec/adapters
- [√] 2.2 迁移并适配 Netty codec（`RespCommandDecoder` / `RespDecoder` / `RespEncoder` 等）到 `yierdis-protocol-netty`，保证 server/client/bench/测试引用路径不变或最小变更
- [√] 2.3 实现 Netty 侧的 frame/session 适配（ByteBuf → bytes source；Channel attribute → session），并定义清晰的 ownership/release 责任（避免 ByteBuf 泄漏）

## 3. core（DB/数据结构/off-heap 访问）
- [√] 3.1 移除 `yierdis-core` 对 `io.netty.*` 的直接引用：将 raw memory copy/get/put 改为通过 Unsafe allocator 的封装能力完成（不在 core 直接使用 Netty `PlatformDependent`）
- [√] 3.2 重构 `YierdisObject` 等写路径：从命令 frame 的 bytes source 直接写入 off-heap buf/string，避免不必要的 heap 中转分配，验证 why.md#requirement-低分配写出路径保留
- [√] 3.3 校验 `maxmemoryBytes` 与 `MEMORY USAGE` 口径不发生行为回归；补充必要的回归断言

## 4. server/client/bench（适配接入）
- [√] 4.1 `yierdis-server` 切换到新的 `RespWriter`（bytes sink + session），保持请求/回复行为一致，验证 why.md#requirement-resp2-resp3-协议状态一致
- [√] 4.2 `yierdis-client` / `yierdis-bench` 依赖调整为使用 `yierdis-protocol-netty` 的 codec，实现无行为回归

## 5. Security Check
- [√] 5.1 执行安全检查（G9）：输入校验边界、CRLF 注入防护、off-heap 生命周期与释放路径、避免引入明文敏感信息

## 6. Testing
- [√] 6.1 执行 `mvn test` 并修复因模块拆分/类迁移造成的测试编译或行为回归
- [√] 6.2 执行 `./scripts/smoke.sh`（可选但推荐）验证 server/cli/bench strictReplies 链路

## 7. Documentation Update（SSOT 同步）
- [√] 7.1 更新 `helloagents/wiki/arch.md` 与相关模块文档（protocol/core/offheap/command/server），使其与最终代码一致
- [√] 7.2 更新 `helloagents/CHANGELOG.md` 并在完成后迁移本方案包到 `helloagents/history/YYYY-MM/`（按 G11）
