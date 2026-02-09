# 命令 / API 手册（Custom Protocol v1）

## 概览

本项目对外暴露的“API”是 argv 风格命令，通过 **Custom Protocol v1 over TCP** 传输（对外不兼容 Redis 原生协议，Redis 生态客户端不可直接使用）。

协议要点：
- request framing：`<len>:<json-payload>\\n`（`len` 为 JSON payload 的 UTF-8 字节长度）
- request schema：JSON object：`{"cmd":"PING","args":["a","b"]}`（`args` 可省略；元素仅允许 `string|null`）
- reply framing：NDJSON（每条回复一个 JSON object，以 `\\n` 结尾）
- reply envelope：
  - 成功：`{"ok":true,"result":...}\\n`
  - 错误：`{"ok":false,"error":{"kind":"protocol|command|internal","message":"..."}}\\n`

> 内置 `YierdisCli` / `yierdis-client` 使用 Custom Protocol v1 发送命令并打印 NDJSON（单行）。

> 注意：request 的 `args` 元素只支持 `string|null`；遇到 `number/object/array` 等类型会被视为 protocol error。若需要传递数值/二进制，应在上层做编码（例如字符串化或 base64 文本）。

> 注意：该手册描述的是当前已实现能力；新增能力以 `helloagents/plan/` 与 `helloagents/history/` 为准（以代码为准，文档做同步）。

---

## 已实现命令（简化版）

### 通用
- `PING [message]`
- `ECHO <message>`
- `HELLO`（信息命令：返回 server/version/proto/mode/role；不做协议协商/切换）
- `COMMAND`（最小子集：`COMMAND`/`COMMAND COUNT`/`COMMAND INFO <name ...>`）
- `SELECT <index>`（默认支持 `0..15`；可通过 `--databases` 调整）
- `QUIT`
- `INFO [section]`（返回 JSON string；`section` 目前为最小子集）
- `STATS`（结构化统计；用于排障/教学）

#### HELLO（信息命令）

- 返回一个结构化对象（Custom Protocol 下表现为 JSON object）：`server/version/proto/mode/role`
- `proto` 当前固定为 `1`（Custom Protocol v1），不进行协议协商/切换
- `version` 来自构建版本（`project.version` 资源注入），避免硬编码常量漂移

### Key/TTL
- `SET key value [EX seconds|PX milliseconds] [NX|XX]`
- `GET key`
- `DEL key [key ...]`
- `EXISTS key [key ...]`
- `EXPIRE key seconds`
- `PEXPIRE key milliseconds`
- `EXPIREAT key unix_seconds`
- `PEXPIREAT key unix_milliseconds`
- `PERSIST key`
- `TTL key`
- `PTTL key`
- `KEYS pattern`（支持 Redis 风格最小 glob 子集：`*`/`?`/`[]` 范围与否定/反斜杠转义；按 byte 匹配）
- `SCAN cursor [MATCH pattern] [COUNT n]`（best-effort；cursor 为“遍历顺序偏移量”的语义）
- `TYPE key`
- `FLUSHDB [SYNC|ASYNC]`（本实现为单线程执行器，二者语义等价）
- `OBJECT ENCODING key`（教学用：查看内部编码）
- `MEMORY USAGE key`
- `MEMORY STATS`（结构化 object；字段集合保持稳定）

### String
- `INCR key`
- `DECR key`
- `APPEND key value`
- `STRLEN key`

### BITMAP（复用 STRING）
- `SETBIT key offset value`（value 仅支持 0/1；offset 超大时会被拒绝以避免不可控内存分配）
- `GETBIT key offset`
- `BITCOUNT key [start end]`（start/end 为 byte-range，支持负数索引）

### HyperLogLog / HLL（复用 STRING）
- `PFADD key element [element ...]`（不存在则创建；若 key 存在但不是 HLL string，则返回 `WRONGTYPE`）
- `PFCOUNT key [key ...]`
- `PFMERGE destkey sourcekey [sourcekey ...]`（结果写入 destkey，写入后会清除 destkey 的 TTL）

### List
- `LPUSH key value [value ...]`
- `RPUSH key value [value ...]`
- `LRANGE key start stop`
- `LPOP key [count]`
- `RPOP key [count]`

### Hash
- `HSET key field value [field value ...]`
- `HGET key field`
- `HGETALL key`
- `HLEN key`
- `HDEL key field [field ...]`

### Set
- `SADD key member [member ...]`
- `SMEMBERS key`
- `SISMEMBER key member`
- `SCARD key`

### ZSet
- `ZADD key score member [score member ...]`
- `ZRANGE key start stop [WITHSCORES] [REV]`
- `ZREVRANGE key start stop [WITHSCORES]`
- `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`
- `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]`
- `ZREMRANGEBYRANK key start stop`
- `ZREMRANGEBYSCORE key min max`
- `ZREM key member [member ...]`

### Transaction（最小子集）
- `MULTI`
- `EXEC`
- `DISCARD`

事务边界与兼容性提示（重要）：
- 事务队列为连接级状态，server 提供条数/bytes 的硬上限（防 OOM；触发错误会进入 aborted，后续 `EXEC` 返回 `EXECABORT` 并丢弃队列）。
- `HELLO` 属于连接级信息命令；为简化事务语义与护栏实现，MULTI 模式下仍禁止 `HELLO`（返回错误并触发 `EXECABORT`）。
