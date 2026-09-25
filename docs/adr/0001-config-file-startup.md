# 0001: server 启动改为单一配置文件入口

日期：2026-09-25
状态：Accepted

## 背景

旧形态中 server 的全部配置都是 argv flag（`--port 6378 --maxmemoryBytes 0 ...`，约 40 个键），由
`YierdisServerArgs.parse(String...)` 手写解析。问题：

- 运维侧无法声明式管理：复现一个部署要靠抄一长串命令行；systemd / 容器编排里配置与启动命令耦合。
- 测试侧每个集成测试自己拼 argv 字符串数组，重复的 `--port 0 --maxmemoryBytes 0` 噪音淹没每个用例真正关心的键。
- 手写解析器要维护 `name value` 与 `name=value` 两种形态、flag/键值消歧义，是无业务价值的机制代码。

## 决定

- server 只接受一个启动参数 `--config <path>`（或 `--config=<path>`）；缺省读取 `./yierdis.conf`。配置文件不存在即启动失败，不静默回退默认配置。
- 配置文件格式为 Java `Properties`（UTF-8，`key=value`，`#`/`!` 注释），键名与旧 argv flag 同名（去掉 `--` 前缀），值解析规则不变。
- `YierdisServerFileConfig.fromProperties(...)` 逐个应用键；**未知键直接报错**（typo 不放过）。`maxmemoryBytes` 仍必须显式给出（`0` 表示承认不限制内存）。
- `normalizeAndValidate()` / `YierdisServerRuntimeConfig` 构造器校验链不变，仍是唯一信任边界。
- `YierdisServerArgs` 删除，无兼容 shim：旧 argv 形态若在 `--config` 之外出现任何参数即报错。
- 测试底座同步收敛：`TestServerConfigs`（yierdis-server）、`TestServerConfig`（yierdis-tests）把"基础默认 + 覆盖键"收敛成一处，调用点只写关心的键。

## 影响

- 启动命令从 `java -jar server.jar --port 6378 --maxmemoryBytes 0` 变为先写 `yierdis.conf` 再 `java -jar server.jar`。
- 所有运行文档（README、configuration-and-operations、production-hardening-operations 等)中的启动示例改为配置文件形态。
- cli / bench 的 `--host` / `--port` 等参数不受影响（它们是客户端参数，不是 server 配置）。
