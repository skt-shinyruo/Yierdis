# 项目技术约定（SSOT）

> 本文件是项目技术规范的单一事实来源（SSOT），当与代码冲突时以代码行为为准，并及时回写更新本文档。

---

## 技术栈

- **语言/运行时**：Java 17
- **构建**：Maven（多模块）
- **网络**：Netty（RESP2/RESP3 over TCP；支持 inline command（调试用，支持引号/转义/`\\xHH`））
- **测试**：JUnit 4
- **日志**：SLF4J + Logback

---

## 代码与命名规范

- **包名前缀**：`yier.bubu.redis`
- **服务端类命名**：使用 `Yierdis*`（避免引入新的 `Redis*` 服务端类名）
- **缩进**：4 空格，无 Tab
- **协议实现边界**：默认 RESP2；支持最小 RESP3（`HELLO 3` 协商后切换；覆盖 HELLO map 与 null），并支持 inline command（用于调试，支持引号/转义/`\\xHH`）

---

## 错误处理与日志

- 命令执行错误统一以 RESP Error 形式返回（例如 `ERR ...` / `WRONGTYPE ...`）。
- 运行时日志通过 SLF4J 输出，默认使用 Logback 配置。

---

## 测试与验证

- 推荐命令：`mvn test`
- 测试要求：尽量可重复（避免 `Thread.sleep`；涉及 TTL 的断言允许小幅时间误差）
