# Module Architecture

本文说明 Yierdis 的 Maven 模块是怎么拆的、依赖方向是什么、以及代码层和测试层如何一起守住这些边界。

## 一眼看懂的依赖图

可以先把项目看成“两条平行车道 + 一层最外组装”：

```text
bytes-lib
├─ core-contract ──> core-api ──> core-db ──> core-runtime
│                     ^             ^
│                     │             └─ memory-foreign
│                     └─ core-command ──> core-engine
│
├─ protocol-model ──> protocol-codec ──> protocol-netty
│
├─ bytes-netty
├─ executor-core
├─ args
│
├─ client  -> protocol-netty
├─ bench   -> args + protocol-codec + protocol-netty
└─ server  -> core-contract + core-engine + core-command + core-runtime
             + protocol-netty + bytes-netty + executor-core + args + memory-foreign
```

理解这张图时，最重要的判断不是“谁依赖谁”，而是“谁负责线上协议、谁负责执行契约、谁负责 DB、谁负责最后的组装”。

## 聚合模块

下面几个模块主要是 parent / aggregator，本身不是运行时代码：

- `yierdis-parent`
- `yierdis-core`
- `yierdis-protocol`
- `yierdis-bytes`
- `yierdis-memory`

它们的作用主要是：

- 统一模块结构
- 统一版本和构建配置
- 让依赖分组更容易理解

## bytes 基础层

### `yierdis-bytes-lib`

提供中立的 bytes 抽象，被 protocol 和 core 两边共享。

它的角色是：

- 避免上层逻辑过早绑定到 Netty
- 为 `BytesView`、`BytesSlice`、`BytesSink` 这类接口提供公共基础

### `yierdis-bytes-netty`

只负责把中立的 bytes 抽象接到 Netty。

它存在的意义是：

- Netty 适配层集中
- core / protocol 的非 Netty 代码不需要知道 `ByteBuf`

## protocol 车道

protocol 车道只负责“线上协议长什么样”，不负责命令执行语义。

### `yierdis-protocol-model`

放协议侧 DTO、协议 reply model、构建信息和一些协议侧类型。

关键认知：

- 它是 protocol-side model，不是 command execution model
- server 回包的语义 authority 不是这里，而是 `ReplyWriter`

### `yierdis-protocol-codec`

负责：

- JSON parser / writer
- Custom Protocol v1 的请求编码
- reply 解析与 codec 辅助

它仍然是 Netty-free 的纯 codec 层。

### `yierdis-protocol-netty`

负责：

- Netty decoder
- Netty 侧协议 glue

它是 protocol 车道中唯一真正碰 Netty 的模块。

## core 车道

core 车道负责“命令和 DB 怎么对话”，而不是“线上怎么发包”。

### `yierdis-core-contract`

这是执行契约层，放的是 transport-agnostic 的命令执行语义对象，例如：

- `ExecutionRequest`
- `ExecutionRecord`
- `ReplyWriter`
- `Session`

它的作用是把命令执行从 protocol DTO 里抽出来。

### `yierdis-core-api`

这是 command-facing DB 能力边界，放的是：

- `DbEngine`
- `DbReads`
- `DbWrites`
- `MemoryOps`
- 各种 `*ReadOps` / `*WriteOps`

它的意义是：

- 命令层只依赖稳定的能力接口
- 不直接依赖 `YierdisDb` 具体实现

对初学者来说，可以把它理解成：

- `core-api` 定义“命令层可以向 DB 提什么要求”
- `core-db` 决定“这些要求具体怎么完成”

### `yierdis-core-db`

这是实际的 DB / storage 实现，包括：

- keyspace
- expires
- strings / hash / list / set / zset / HLL
- TTL
- maxmemory
- memory accounting

这里是数据结构和存储策略最密集的模块。

### `yierdis-core-command`

这是传输无关的命令层。

它的职责是：

- 注册和分发命令
- 参数解析
- 把命令翻译成 `DbReads/DbWrites` 调用
- 不直接依赖 DB 实现

这个模块是整个架构里最刻意做“依赖倒置”的地方。

从代码逻辑上看，`core-command` 的职责可以再具体化成：

- `YierdisFastCommandProcessor`
  负责命令注册、事务前置判定、错误转换和最终分发
- `CommandSupport`
  负责把 `CommandContext` 里的会话态、DB 路由和参数解析 helper 收敛到一起
- 各个 `*Commands`
  负责具体命令的参数解释和回包语义

也就是说，命令层更像“翻译层”，而不是“存储实现层”。

### `yierdis-core-engine`

这是当前重构引入的 command execution kernel 边界。

它的职责是：

- 对外暴露唯一的命令执行入口 `YierdisEngine`
- 把 server 从 `YierdisFastCommandProcessor` 的具体构造里解耦出来
- 把定时 maintenance 入口也收敛到 engine 上
- 为后续迁移 session / transaction ownership 留出稳定边界

当前阶段的 `DefaultYierdisEngine` 仍然委托已有的 `YierdisFastCommandProcessor` 执行命令，也委托 runtime maintenance hook 做清理。

因此它不是新的命令实现层，而是先把“谁是执行入口”这件事固定下来：

- server 只调用 `YierdisEngine.execute(...)`
- executor 只接收 `engine::execute`
- maintenance 只调用 `engine.maintenanceTick()`

后续如果要把事务状态、session 状态或 typed command parsing 继续迁进 engine，外层 server / executor 不需要再被迫知道这些细节。

### `yierdis-core-runtime`

负责 runtime 级组装，而不是协议处理。

它做的事情包括：

- 多 DB 实例装配
- owner-thread runtime seam
- instance 生命周期
- global maxmemory 协调

它有意不重新承担 command processor 组装职责。

初学者如果只记一句话，可以记成：

- `core-runtime` 负责“实例怎么活”
- `core-command` 负责“命令怎么解释”

这两个模块在职责上是并排关系，不是上下级关系。

## 运行时 / 内存 / 调度辅助模块

### `yierdis-memory-foreign`

这是 JDK 25 FFM backend。

它为 core-db 提供：

- native memory runtime
- FFM allocator
- region / span / access 抽象

这里的角色是“内存底座”，不是“命令层的一部分”。

### `yierdis-executor-core`

这是 Netty-free 的调度 / 背压算法层。

它抽出来的价值在于：

- 提交和背压决策逻辑不必散落在 server 里
- 算法层可以不和具体 I/O 实现绑死

### `yierdis-args`

放 server 和工具共享的参数模型。

它的存在让：

- server
- bench
- 其他工具脚本

可以复用同一份参数语义，而不是各自维护一套 flag 解释。

## 最外层壳子

### `yierdis-server`

这是唯一真正把 protocol lane 和 core lane 拼起来的模块。

它负责：

- 进程入口
- 参数转 runtime config
- Netty pipeline
- protocol request -> `ExecutionRequest` 的适配
- command executor 和 runtime seam 的对接
- server-facing command module

换句话说，server 是“组装层”，不是“把所有逻辑都重新实现一遍的层”。

从代码逻辑上看，server 模块最核心的价值是把两条车道接起来：

- protocol 车道产出协议请求对象
- server 里的 `ProtocolCommandAdapter` 把它变成 `ExecutionRequest`
- server 把 `ExecutionRequest` 交给 `YierdisEngine`
- engine 再把请求交给当前命令处理实现
- reply 最后再通过 `ReplyWriter` 落回 server 的协议输出

这也是为什么很多“看起来可以顺手改到 core 里”的事情，其实应该只在 server 做。

### `yierdis-client`

client 的生产依赖主要走 protocol 车道，说明它的定位是：

- 会说 Custom Protocol v1 的客户端 / CLI
- 不是嵌入式 command/core 执行环境

### `yierdis-bench`

bench 也主要依赖协议和参数模块，说明它在设计上更像：

- 外部压测工具
- 而不是直接嵌入 DB 内部的 benchmark harness

## 最重要的边界原则

下面几条是读代码和改代码时最值得记住的边界原则。

### 1. protocol 不等于 command contract

`ExecutionRequest` / `ReplyWriter` 在 `core-contract`，不是在 `protocol-model`。

这意味着：

- protocol DTO 不是命令层的事实来源
- server 最终写回语义仍以 `ReplyWriter` 为准

### 2. `core-command` 不能依赖 `core-db`

命令层只能看 `DbEngine` / `DbReads` / `DbWrites`，不能直接看 `YierdisDb`。

这样做的目的很明确：

- 命令层不和具体存储实现耦死
- DB 能力边界在 API 层稳定下来

### 3. server 才是 protocol -> core 的桥

协议请求适配为 `ExecutionRequest` 的桥只应该留在 server。

这让 protocol 车道和 core 车道可以各自演进，而不至于互相拖住。

### 4. runtime 负责实例生命周期，不负责命令组装

`YierdisInstance` 负责 DB 生命周期、owner-thread 协作和资源 ownership，但不重新承担 command processor 装配职责。

### 5. engine 是命令执行入口

server 不再直接构造 `YierdisFastCommandProcessor`，而是构造 `YierdisEngine`。

这条边界的意义是：

- command processor 的注册、事务、错误转换等细节不继续散落到 bootstrap
- maintenance 也通过同一个 engine 入口回到 runtime/storage
- 后续重构可以把 session/transaction 继续迁入 engine，而不改变 server/executor 的接线模型

如果把一次请求跨模块的过程写成最短路径，大致是：

1. `protocol-netty`
   把网络输入交给 decoder
2. `server`
   把协议请求适配为 `ExecutionRequest`
3. `core-engine`
   作为统一执行入口接收 `ExecutionRequest`
4. `core-command`
   根据 `ExecutionRequest` 和 `CommandContext` 分发命令
5. `core-api`
   通过 `DbReads/DbWrites` 暴露能力边界
6. `core-db`
   执行实际读写
7. `server`
   通过 `ReplyWriter` 把结果编码回协议

初学者如果能把这 7 步记住，读整个仓库时就不容易迷路。

## 构建和测试层面的护栏

这些边界不只是文档约定，仓库里有明确的护栏。

### `core-command` 构建时禁止依赖

`yierdis-core-command/pom.xml` 直接通过 `maven-enforcer-plugin` 禁止：

- `yierdis-core-db`
- `yierdis-protocol-model`

这是硬依赖护栏。

### `ArchitectureBoundaryTest`

这个测试会扫描源码，确保：

- `core-db/core-api/core-command` 不 import `yier.bubu.redis.protocol.*`
- `protocol-codec` 不依赖 `core-contract`
- bootstrap 不重新内联 owner-thread 生命周期逻辑
- request DTO 和 `ExecutionRequest` 的边界不被重新打穿
- server bootstrap 不绕过 `YierdisEngine` 直接接线 command processor 或 maintenance

这类测试的意义，不只是“防止依赖写错”，更是在保护整个阅读模型：

- 协议层看到的对象和命令层看到的对象必须继续分离
- runtime 必须继续只管生命周期
- server 必须继续只做组装，而不是把所有责任拿回去
- engine 必须继续是 command execution 的唯一入口

### `ReplySsoTGuardTest`

这个测试保证：

- server/core 生产代码不把 `ReplyValue` 当回包 authority
- protocol reply model 不重新成为命令层回包语义的真相源

## 读模块架构时推荐的文件

- `README.md`
- `pom.xml`
- `yierdis-core/yierdis-core-command/pom.xml`
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java`
- `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`
- `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`

## 一句话总结

Yierdis 的模块设计重点，不是“按包名分目录”，而是：

- protocol 负责线上协议
- core-contract / core-api 负责执行契约和 DB 能力边界
- core-engine 负责统一命令执行入口
- core-db 负责真实存储
- core-runtime 负责实例生命周期
- server 负责最后的组装

如果你准备真正动手改功能，读完本文后建议继续看 [`development-navigation.md`](./development-navigation.md)。

如果你想继续理解 bytes 抽象为什么单独拆模块，以及它如何连接 protocol/off-heap/Netty 写回，再看 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。

## 面向初学者的模块阅读顺序

如果你想把模块和代码逻辑一起读懂，建议按这个顺序：

1. `yierdis-server`
   先知道项目怎么启动、怎么收请求
2. `yierdis-core-engine`
   再知道 server 最终把请求交给哪个执行入口
3. `yierdis-core-command`
   再知道命令是怎么被解释和分发的
4. `yierdis-core-api`
   再知道命令层到底能向 DB 要什么能力
5. `yierdis-core-db`
   最后再进入实际存储实现
6. `yierdis-core-runtime`
   回过头理解多 DB、owner thread 和实例级生命周期
7. `yierdis-protocol-*`
   最后补协议细节和外部工具链

这样读会更符合“先看主路径，再看细节分层”的学习顺序。
