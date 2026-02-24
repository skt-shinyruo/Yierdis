# 方案归档索引

> 通过此文件快速查找历史方案。

> 归档年份: [2026](_index.md)

## 快速索引（当前年份）

| 时间戳 | 名称 | 类型 | 涉及模块 | 决策 | 结果 |
|--------|------|------|---------|------|------|
| 202601011057 | bitmap_hll | Standard Development | command, db, offheap, test | - | ✅ 完成 |
| 202601031446 | protocol_error_handling | Lightweight Iteration | - | - | ✅ 完成 |
| 202601041004 | redis_hardening | Standard Development | - server | - | ✅ 完成 |
| 202601071641 | resp3_inline | Standard Development | - `yierdis-server`（Netty pipeline、协议解析、命令处理、响应写出） | - | ✅ 完成 |
| 202601080927 | inline_parser | Lightweight Iteration | - | - | ✅ 完成 |
| 202601081106 | netty_executor_integration | Standard Development | - `yierdis-server/`（网络/执行模型/背压/flush/DB 写入路径） | - | ✅ 完成 |
| 202601142007 | architecture_refactor | Standard Development | - | - | ✅ 完成 |
| 202601151101 | denetty_core_protocol | Standard Development | - `yierdis-protocol`（职责收敛为 Netty-free SSOT） | - | ✅ 完成 |
| 202601152357 | arch_hardening | Standard Development | - `yierdis-server`（关停顺序、执行器契约、decoder/executor 配置注入） | - | ✅ 完成 |
| 202601161128 | arch_refactor | Standard Development | - | - | ✅ 完成 |
| 202601161551 | arch_guardrails | Standard Development | `yierdis-protocol`, `yierdis-protocol-netty`, `yierdis-offheap-api`, `yierdis-offheap-*`, `yierdis-core`, `yierdis-server`, `helloagents/wiki/*` | - | ✅ 完成 |
| 202601161845 | arch_refactor_5issues | Standard Development | - `yierdis-server` | - | ✅ 完成 |
| 202601162335 | fix_arch_issues | Standard Development | - | - | ✅ 完成 |
| 202601171043 | arch_unify_protocol_ctx | Standard Development | - | - | ✅ 完成 |
| 202601171535 | arch_deep_refactor | Standard Development | - | - | ✅ 完成 |
| 202601171846 | arch_deep_refactor | Standard Development | - yierdis-protocol | - | ⚠️ 部分完成 |
| 202601172157 | executor_modularize | Unknown | - | - | ⏸ 未知 |
| 202601231314 | resp3_glob_hash_refactor | Standard Development | - `yierdis-core`（命令层、DB、HashValue、KEYS glob） | - | ✅ 完成 |
| 202602011923 | redis_compat_extended | Execution Command | - `yierdis-protocol`（RESP3 类型与写出能力） | - | ⚠️ 部分完成 |
| 202602020852 | redis_compat_milestones2 | Execution Command | protocol / protocol-netty / core(command,db) / server / client / args / helloagents docs | - | ⚠️ 部分完成 |
| 202602021230 | cli_error_model_capability_probe | Standard Development | - `yierdis-args`（参数模型与校验、统一错误类型） | - | ✅ 完成 |
| 202602022147 | redis_compat_alignment | Execution Command | - yierdis-protocol | - | ✅ 完成 |
| 202602031053 | tx_hello_protocol_fix | Standard Development | `yierdis-protocol` / `yierdis-core` / `yierdis-server` / `helloagents/wiki` | - | ✅ 完成 |
| 202602031225 | resp3_request_compat | Standard Development | - yierdis-protocol-netty | - | ✅ 完成 |
| 202602031537 | resp3_full_coverage | Standard Development | - yierdis-protocol（SSOT：类型/解析/写出语义） | - | ✅ 完成 |
| 202602031814 | db_prod_arch_contracts | Execution Command | - `yierdis-core`（db/keyspace/expires/value/memory stats/scan） | - | ⚠️ 部分完成 |
| 202602041128 | core_embedded_instance_runtime_api | Standard Development | - `yierdis-core`（新增 runtime API、抽离实例级协调逻辑） | - | ✅ 完成 |
| 202602041630 | db_prod_arch_contracts_full_newimpl | Standard Development | `yierdis-core`, `yierdis-server` | - | ✅ 完成 |
| 202602050900 | zero_copy_command_args | Unknown | - | - | ⏸ 未知 |
| 202602050920 | offheap_free_list_debug | Unknown | - | - | ⏸ 未知 |
| 202602061102 | resp_parser_ssot_alignment | Standard Development | - `yierdis-protocol`（SSOT：RESP wire support / skipper / parser 语义一致性） | - | ✅ 完成 |
| 202602061216 | resp_codec_test_matrix | Standard Development | - `yierdis-protocol`（SSOT：`RespWireSkipper`/`RespObjectParser` 的一致性验证与边界测试） | - | ✅ 完成 |
| 202602061601 | custom_protocol_v1 | Standard Development | - `yierdis-protocol`（新增协议无关抽象 + JSON codec；RESP 实现进入“legacy”） | - | ✅ 完成 |
| 202602081104 | protocol_v1_request_decoder_low_copy | Execution Command | `yierdis-protocol-netty`, `yierdis-protocol`, `yierdis-server`（pipeline 装配复用 decoder 行为） | - | ✅ 完成 |
| 202602081454 | db_executor_decouple | Standard Development | `yierdis-core`（db/ops/command）、`yierdis-protocol`（ReplySink 抽象）、`yierdis-server`（executor 组件化） | - | ⚠️ 部分完成 |
| 202602081752 | foreign_memory_default | Lightweight Iteration | - `yierdis-offheap` / `yierdis-offheap-foreign` | - | ✅ 完成 |
| 202602081942 | command_engine_boundary | Standard Development | - `yierdis-core`：`command/*`、`ops/*`、`runtime/YierdisInstance`、`db/YierdisDb*` | - | ✅ 完成 |
| 202602091258 | db_executor_decouple_v2 | Execution Command | - `yierdis-server`（executor/bootstrap/info provider 解耦与组件化） | - | ✅ 完成 |
| 202602091941 | protocol_v1_reply_ir | Standard Development | - `yierdis-protocol`（Reply IR + encoder + 统一错误净化/限长） | - | ✅ 完成 |
| 202602092316 | reply_byteslice_streaming_encoder | Standard Development | `yierdis-protocol`（encoder/writer/json codec SSOT） | - | ✅ 完成 |
| 202602212340 | module_naming_protocol_split | Standard Development | - `yierdis-server`（坐标调整） | - | ✅ 完成 |
| 202602221020 | domain_result_adapter | Standard Development | - `yierdis-core`（主要改动：ops/db/command） | - | ✅ 完成 |
| 202602222355 | command_context_split | Standard Development | - `yierdis-protocol-model`（新增 `CommandContext`，调整 `ReplyWriter/ReplyWriterFactory` 与 provider 接口） | - | ✅ 完成 |
| 202602241116 | arch_module_reorg | Standard Development | - 新增：`yierdis-core-api`、`yierdis-core-db`、`yierdis-core-command`、`yierdis-core-runtime`、`yierdis-executor-core` | - | ⚠️ 部分完成 |

## 按月归档

### 2026-01
- [202601011057_bitmap_hll](./2026-01/202601011057_bitmap_hll/) - 补齐 BITMAP / HyperLogLog（基于 STRING 复用）
- [202601031446_protocol_error_handling](./2026-01/202601031446_protocol_error_handling/) - protocol_error_handling
- [202601041004_redis_hardening](./2026-01/202601041004_redis_hardening/) - Change Proposal: redis_hardening
- [202601071641_resp3_inline](./2026-01/202601071641_resp3_inline/) - Change Proposal: 支持 RESP3 + Inline（提升 Redis 客户端兼容性）
- [202601080927_inline_parser](./2026-01/202601080927_inline_parser/) - inline_parser
- [202601081106_netty_executor_integration](./2026-01/202601081106_netty_executor_integration/) - Change Proposal: Netty 执行器融合改造（覆盖问题 1/2/3/4/5）
- [202601142007_architecture_refactor](./2026-01/202601142007_architecture_refactor/) - Change Proposal: architecture_refactor
- [202601151101_denetty_core_protocol](./2026-01/202601151101_denetty_core_protocol/) - Change Proposal: core/protocol 去 Netty 依赖（边界收敛）
- [202601152357_arch_hardening](./2026-01/202601152357_arch_hardening/) - Change Proposal: 架构加固（Shutdown / Backlog Bytes / Split-package / Off-heap Capabilities / Version SSOT）
- [202601161128_arch_refactor](./2026-01/202601161128_arch_refactor/) - Change Proposal: 架构重构（命令/DB 解耦 + 执行模型硬化 + 预算/背压可解释化）
- [202601161551_arch_guardrails](./2026-01/202601161551_arch_guardrails/) - Change Proposal: 架构护栏与可观测性加固（arch_guardrails）
- [202601161845_arch_refactor_5issues](./2026-01/202601161845_arch_refactor_5issues/) - Change Proposal: 架构问题 5 项治理（解耦 / 生命周期 / 背压 / RESP3 对齐 / 可维护性）
- [202601162335_fix_arch_issues](./2026-01/202601162335_fix_arch_issues/) - 修复 5 个架构/实现问题（server/client/bytes 交界处）
- [202601171043_arch_unify_protocol_ctx](./2026-01/202601171043_arch_unify_protocol_ctx/) - 架构收敛（移除 Deprecated Alias / 协议栈统一 / ConnectionContext / 可观测性 / 命令路由加速）
- [202601171535_arch_deep_refactor](./2026-01/202601171535_arch_deep_refactor/) - Yierdis 架构深度重构（执行器组件化 + 连接态解耦 + 协议严格性与资源生命周期加固）
- [202601171846_arch_deep_refactor](./2026-01/202601171846_arch_deep_refactor/) - Change Proposal: 架构深度重构（协议/执行/客户端分层与 SSOT 收敛）
- [202601172157_executor_modularize](./2026-01/202601172157_executor_modularize/) - Why: NettyCommandExecutor 组件化与不变量测试补齐
- [202601231314_resp3_glob_hash_refactor](./2026-01/202601231314_resp3_glob_hash_refactor/) - Change Proposal: RESP3 友好化 + KEYS Glob 对齐 + Hash(off-heap) 编码对齐 + 写入语义修复

### 2026-02
- [202602011923_redis_compat_extended](./2026-02/202602011923_redis_compat_extended/) - Change Proposal: Redis 兼容性扩展（RESP3 / 多 DB / 事务 / PubSub / 持久化 / ACL/TLS）
- [202602020852_redis_compat_milestones2](./2026-02/202602020852_redis_compat_milestones2/) - Change Proposal: Redis 生态兼容性（二期 Roadmap：事务 / PubSub / SCAN / 安全基线 / AOF）
- [202602021230_cli_error_model_capability_probe](./2026-02/202602021230_cli_error_model_capability_probe/) - Change Proposal: CLI 统一错误模型与能力探测（Off-heap foreign 等）
- [202602022147_redis_compat_alignment](./2026-02/202602022147_redis_compat_alignment/) - Change Proposal: Redis 兼容性对齐与功能扩展（RESP3 / 事务 / 全局 maxmemory）
- [202602031053_tx_hello_protocol_fix](./2026-02/202602031053_tx_hello_protocol_fix/) - Change Proposal: TX HELLO 协议切换修复 + 文档 SSOT 同步
- [202602031225_resp3_request_compat](./2026-02/202602031225_resp3_request_compat/) - Change Proposal: RESP3 Request 兼容性对齐（decoder 支持 attributes + scalar）
- [202602031537_resp3_full_coverage](./2026-02/202602031537_resp3_full_coverage/) - Change Proposal: RESP3 全覆盖（request + reply + streamed + push + attributes）
- [202602031814_db_prod_arch_contracts](./2026-02/202602031814_db_prod_arch_contracts/) - Change Proposal: DB Production-Grade Architecture Contracts (Redis Alignment)
- [202602041128_core_embedded_instance_runtime_api](./2026-02/202602041128_core_embedded_instance_runtime_api/) - Change Proposal: Core Embedded Instance Runtime API (Netty-free Instance SSOT)
- [202602041630_db_prod_arch_contracts_full_newimpl](./2026-02/202602041630_db_prod_arch_contracts_full_newimpl/) - Change Proposal: DB 生产级契约落地（全量切换新实现）
- [202602050900_zero_copy_command_args](./2026-02/202602050900_zero_copy_command_args/) - zero_copy_command_args
- [202602050920_offheap_free_list_debug](./2026-02/202602050920_offheap_free_list_debug/) - offheap_free_list_debug
- [202602061102_resp_parser_ssot_alignment](./2026-02/202602061102_resp_parser_ssot_alignment/) - Change Proposal: RESP 解析 SSOT 收敛与质量兜底
- [202602061216_resp_codec_test_matrix](./2026-02/202602061216_resp_codec_test_matrix/) - Change Proposal: RESP 解析质量兜底（Golden + Round-trip + Fuzz + 一致性差分）
- [202602061601_custom_protocol_v1](./2026-02/202602061601_custom_protocol_v1/) - Change Proposal: Custom Protocol v1（完全替换 RESP）
- [202602081104_protocol_v1_request_decoder_low_copy](./2026-02/202602081104_protocol_v1_request_decoder_low_copy/) - Change Proposal: Custom Protocol v1 request 解码低拷贝化
- [202602081454_db_executor_decouple](./2026-02/202602081454_db_executor_decouple/) - Change Proposal: DB/Executor 分层解耦与组件化（ReplySink + 巨型类拆分）
- [202602081752_foreign_memory_default](./2026-02/202602081752_foreign_memory_default/) - Change Proposal: foreign-memory 默认可用（off-heap foreign）
- [202602081942_command_engine_boundary](./2026-02/202602081942_command_engine_boundary/) - Change Proposal: Command/DB 边界收口与执行器协议解耦（DbEngine + Ops 化）
- [202602091258_db_executor_decouple_v2](./2026-02/202602091258_db_executor_decouple_v2/) - Change Proposal: DB/Executor 边界解耦 v2（移除 server→YierdisDb 直接依赖 + 持续拆分巨型类）
- [202602091941_protocol_v1_reply_ir](./2026-02/202602091941_protocol_v1_reply_ir/) - Change Proposal: Custom Protocol v1 Reply IR（协议语义中间层）与错误模型收敛
- [202602092316_reply_byteslice_streaming_encoder](./2026-02/202602092316_reply_byteslice_streaming_encoder/) - Change Proposal: reply BytesSlice streaming encoder（贯通 off-heap fast-path）
- [202602212340_module_naming_protocol_split](./2026-02/202602212340_module_naming_protocol_split/) - Change Proposal: 模块命名对齐与协议层拆分（protocol-model / protocol-codec）
- [202602221020_domain_result_adapter](./2026-02/202602221020_domain_result_adapter/) - Change Proposal: core 分层解耦（domain result → adapter，移除 db/数据结构对 ReplySink 的依赖）
- [202602222355_command_context_split](./2026-02/202602222355_command_context_split/) - Change Proposal: CommandContext 全链路重构（拆分输入 Session 与输出 ReplyWriter）
- [202602241116_arch_module_reorg](./2026-02/202602241116_arch_module_reorg/) - Change Proposal: 架构模块重组（大拆分/重组模块，收敛边界与可测试性）

## 结果状态说明
- ✅ 完成
- ⚠️ 部分完成
- ❌ 失败/中止
- ⏸ 未知
