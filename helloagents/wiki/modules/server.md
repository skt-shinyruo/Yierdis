# server

## Purpose

负责 Netty 服务端启动、管线组装与连接生命周期管理。

## Module Overview

- **Responsibility:** 端口监听、Pipeline 组装、定时任务（如 TTL 清理）的调度入口
- **Status:** ✅Stable
- **Last Updated:** 2026-01-04

## Specifications

### Requirement: RESP2 TCP 服务端
**Module:** server
启动一个基于 Netty 的 TCP 服务端，支持 RESP2 命令请求与响应写回。

#### Scenario: 基本连通性
条件：服务端启动并监听端口
- 预期：`redis-cli --resp2` 可连接并执行 `PING` 得到响应

#### Scenario: 协议错误（非法 RESP）
条件：客户端发送非法 RESP2 请求（解码阶段抛出 `Protocol error: ...`）
- 预期：服务端返回 `ERR Protocol error: ...` 并关闭连接
- 说明：连接关闭是“协议层错误”的默认策略，避免解码状态不一致影响后续请求

### Requirement: I/O 与命令执行解耦（单线程命令语义）
**Module:** server
将命令执行从 Netty I/O event-loop 中移出：I/O 线程仅负责解码与投递，命令在单线程 `CommandExecutor` 中串行执行（保持 Redis 风格单线程语义）。

#### Scenario: 多 worker I/O + 单线程执行
条件：`--ioThreads > 1` 且多个连接并发请求
- 预期：命令仍由同一个执行器线程串行执行
- 预期：DB 仅绑定到执行器线程；I/O 线程不直接访问 DB（避免线程安全问题）

#### Scenario: 执行队列满（背压）
条件：执行器队列达到 `--executorQueueCapacity`
- 预期：服务端立即返回 `ERR busy`，避免请求无界堆积导致 OOM/延迟雪崩

## Dependencies

- protocol
- command
- db

## Change History

- 2026-01-03：补充协议错误的连接生命周期处理约定（返回 `ERR` 并关闭连接）。
- 2026-01-04：引入单线程 `CommandExecutor` 解耦 I/O 与执行，并增加队列上限与 `ERR busy` 背压策略。
