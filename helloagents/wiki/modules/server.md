# server

## Purpose

负责 Netty 服务端启动、管线组装与连接生命周期管理。

## Module Overview

- **Responsibility:** 端口监听、Pipeline 组装、定时任务（如 TTL 清理）的调度入口
- **Status:** ✅Stable
- **Last Updated:** 2026-01-03

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

## Dependencies

- protocol
- command
- db

## Change History

- 2026-01-03：补充协议错误的连接生命周期处理约定（返回 `ERR` 并关闭连接）。
