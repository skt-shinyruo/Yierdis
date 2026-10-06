# CONTEXT — Yierdis 领域词汇表

本文件是 Yierdis 的**领域词汇表**：只定义术语，不含实现细节，也不记录设计决策。
术语在源码里的对应位置见 `docs/project-docs/glossary.md`。

## Language

**应用客户端**:
Yierdis 进程之外的调用方所持有的命令入口，指向一台正在运行的 Yierdis。
_Avoid_: YierdisClient、CLI、压测客户端、SDK、驱动、Jedis

**连接**:
应用客户端指向一台 standalone Yierdis 的一条命令通道。同一时刻只由一个调用方使用。
_Avoid_: YierdisClient、socket、EngineSession

**连接模式**:
一条连接在某一时刻所处的使用方式：普通、管道或事务。同一时刻只有一种。
_Avoid_: session、EngineSession

**连接池**:
持有多条连接，并把其中一条借给一个调用方的对象。
_Avoid_: 线程池

**管道**:
一条连接上已经写出、尚未取回回复的一串命令。
_Avoid_: 事务、批量、脚本

**管道对象**:
调用方在管道模式期间用来发命令的入口。它和那条连接是同一条命令通道。
_Avoid_: 事务对象、连接池

**事务**:
一条连接上由 MULTI 开始、由 EXEC 或 DISCARD 结束的排队。
_Avoid_: WATCH、管道、脚本

**事务对象**:
调用方在事务模式期间用来发命令的入口。它和那条连接是同一条命令通道。
_Avoid_: 第二条连接、管道对象

**原始命令**:
不按命令名解释参数、按 RESP 形状返回结果的一次调用。
_Avoid_: 内联命令、eval
