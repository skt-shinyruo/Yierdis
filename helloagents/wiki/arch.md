# 架构设计

## Overall Architecture

```mermaid
flowchart TD
    C[Client / redis-cli] -->|RESP2/RESP3 over TCP| N[Netty Server]
    N --> D[RespCommandDecoder]
    D --> H[YierdisFastCommandHandler]
    H --> P[YierdisFastCommandProcessor]
    P --> DB[YierdisDb]
    DB --> KS[Keyspace + Expires]
    DB --> OH[Off-heap Allocator (optional)]
```

## Tech Stack

- **Backend:** Java 17 / Netty / Maven
- **Protocol:** RESP2（默认）+ RESP3（最小子集，HELLO 3 协商）+ inline command（调试用，支持引号/转义/`\\xHH`）
- **Data:** 内存数据结构（可选 off-heap）

## Core Flow

```mermaid
sequenceDiagram
    participant Client
    participant Netty
    participant Decoder as RespCommandDecoder
    participant Handler as CommandHandler
    participant Processor as CommandProcessor
    participant DB as YierdisDb

    Client->>Netty: TCP bytes (RESP2/RESP3 command request + inline)
    Netty->>Decoder: decode to RespCommand
    Decoder->>Handler: fire RespCommand
    Handler->>Processor: execute(cmd)
    Processor->>DB: read/write data
    DB-->>Processor: result / error
    Processor-->>Client: RESP2/RESP3 reply（由连接级协议状态决定）
```

## Major Architecture Decisions

| adr_id | title | date | status | affected_modules | details |
|--------|-------|------|--------|------------------|---------|
