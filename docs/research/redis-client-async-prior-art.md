# Redis 客户端：未完成命令、超时、事务

对照的是三个已经定下来的约束：一条连接同一时刻只有一个调用方；命令提交后回复按提交顺序配对；连接模式只有普通和事务。

## 归还连接

- Jedis 把连接还回池之前调用 `resetState()`。管道的 `close()` 先 `sync()`，把已经写出的回复读完。Jedis 7 的事务 `close()` 在仍处于 `MULTI` 时发送 `DISCARD`。读完或丢弃之后，连接才给下一个借用者。来源：[JedisPool.returnResource](https://github.com/redis/jedis/blob/master/src/main/java/redis/clients/jedis/JedisPool.java)、[Pipeline.close](https://github.com/redis/jedis/blob/master/src/main/java/redis/clients/jedis/Pipeline.java)、[transactions-multi.md](https://github.com/redis/jedis/blob/v7.4.1/docs/transactions-multi.md)。
- Lettuce 的池只是归还对象。已经发出的命令留在这条连接的队列里，回复仍配回原来的 Future，后发出的命令排在后面。这依赖多个调用方共用同一条队列。事务期间要求外部保证只有一方在用这条连接。来源：[Connection Pooling](https://github.com/redis/lettuce/wiki/Connection-Pooling)、[Transactions](https://github.com/redis/lettuce/wiki/Transactions)。

## 超时

- Lettuce 的计时从命令写给传输层开始，和是否立刻 flush 无关。到点只把这一条 Future 完成成超时，不关闭连接。命令留在队列里，晚到的回复仍由它消化，后面的命令不会拿到这份回复。连接断开时才取消队列里的全部命令。来源：[CommandExpiryWriter](https://github.com/redis/lettuce/blob/6.3.2.RELEASE/src/main/java/io/lettuce/core/protocol/CommandExpiryWriter.java)、[AsyncCommand.complete](https://github.com/redis/lettuce/blob/6.3.2.RELEASE/src/main/java/io/lettuce/core/protocol/AsyncCommand.java)、[lettuce#954](https://github.com/redis/lettuce/issues/954)。
- 阻塞客户端在 `SO_TIMEOUT` 触发时无法继续划分后续字节，只能把连接标废。这是现在 yierdis-client 和 Jedis 读回复时的做法，不是 Lettuce 的命令超时。

## 事务

- ioredis 默认把 `multi()` 做成内存队列，`exec()` 时一次写出 `MULTI`、命令和 `EXEC`。来源：[ioredis README](https://github.com/redis/ioredis#pipelining)。
- Lettuce 在调用时就把 `MULTI` 和后续命令写出去，不等 `OK`。发出 `MULTI` 时连接进入事务。事务里每条命令的 Future 在 `EXEC` 结果到达时完成，拿到的是执行结果；`QUEUED` 不交给调用方。同步 API 在事务里返回 `null`，结果在 `exec()`。来源：[Transactions](https://github.com/redis/lettuce/wiki/Transactions)、[lettuce#1625](https://github.com/redis/lettuce/issues/1625)、[lettuce#67](https://github.com/redis/lettuce/issues/67)。
