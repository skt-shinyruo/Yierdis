# 0002: 应用客户端是独立模块，不以官方 Jedis 兼容为目标

日期：2026-10-06
状态：Accepted

Yierdis 已经有给 CLI 用的阻塞传输，server 自己也只实现了 standalone RESP 的一部分命令。应用客户端做成独立模块 `yierdis-client`，只依赖 RESP 编解码，供别的 Java 进程连接一台正在运行的 Yierdis。它按这台 server 的命令集和 RESP2 设计。官方 Jedis 兼容、把 CLI 传输加厚成应用 API，都曾是候选，但前者会在未实现的命令上失败，后者的用户是 CLI 而不是应用。
