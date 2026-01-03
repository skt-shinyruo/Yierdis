# 架构设计

## Overall Architecture

```mermaid
flowchart TD
    C[Client / redis-cli] -->|RESP2 over TCP| N[Netty Server]
    N --> D[RespCommandDecoder]
    D --> H[YierdisFastCommandHandler]
    H --> P[YierdisFastCommandProcessor]
    P --> DB[YierdisDb]
    DB --> KS[Keyspace + Expires]
    DB --> OH[Off-heap Allocator (optional)]
```

## Tech Stack

- **Backend:** Java 17 / Netty / Maven
- **Protocol:** RESP2
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

    Client->>Netty: TCP bytes (RESP2)
    Netty->>Decoder: decode to RespCommand
    Decoder->>Handler: fire RespCommand
    Handler->>Processor: execute(cmd)
    Processor->>DB: read/write data
    DB-->>Processor: result / error
    Processor-->>Client: RESP2 reply
```

## Major Architecture Decisions

| adr_id | title | date | status | affected_modules | details |
|--------|-------|------|--------|------------------|---------|

