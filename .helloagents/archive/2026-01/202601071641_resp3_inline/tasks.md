<!-- migrated_from: history/2026-01/202601071641_resp3_inline/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: 支持 RESP3 + Inline（提升 Redis 客户端兼容性）

Directory: `helloagents/plan/202601071641_resp3_inline/`

---

## 1. yierdis-server：协议层与握手
- [√] 1.1 增加连接级协议版本状态（RESP2/RESP3），并在 Netty Channel 上保存
- [√] 1.2 扩展请求解码器：在保留 RESP2 multi-bulk 的基础上，支持 inline command（`PING\\r\\n`）
- [√] 1.3 扩展 `HELLO`：支持 `HELLO 3` 并切换连接为 RESP3；RESP3 下返回 map 结构
- [√] 1.4 增加 RESP3 响应写出能力（至少覆盖：nil、HELLO map；其余类型复用 RESP2 子集）
- [√] 1.5 调整 server pipeline 与执行器写回路径，按连接协议版本选择 RESP2/RESP3 writer

## 2. 测试（回归 + 新增）
- [√] 2.1 新增单测：inline `PING` / `ECHO` 的解析与响应
- [√] 2.2 新增单测：`HELLO 3` 后 `GET missing` 返回 RESP3 nil
- [√] 2.3 新增单测：默认 RESP2 行为不变（确保现有测试全部通过）

## 3. Security Check
- [√] 3.1 执行安全检查（输入上限、CRLF 注入、防止 OOM/无限增长、无敏感信息硬编码）

## 4. Documentation Update（知识库同步）
- [√] 4.1 更新 `helloagents/project.md`：协议边界从“仅 RESP2”调整为“RESP2 + RESP3（最小子集）+ inline”
- [√] 4.2 更新 `README.md`：补充 `redis-cli` 默认连接/RESP3 的使用说明

## 5. Testing
- [√] 5.1 运行 `mvn test` 并记录结果
