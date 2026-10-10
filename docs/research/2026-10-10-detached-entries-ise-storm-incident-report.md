# 事故报告：DetachedEntries ISE 风暴 → Surefire 孤儿 JVM / G1 GC thrash

| 项 | 值 |
|----|-----|
| 报告日期 | 2026-10-10 |
| 事故窗口 | 2026-10-09 21:23 – 2026-10-10（取证后 kill） |
| 事故 PID | `3381514`（已 kill；勿再 attach） |
| 关联背景 | `lane-errtext` worktree / issue **#182** error-text；模块 `yierdis-client` Surefire fork |
| 修复父票 | **[#190](https://github.com/skt-shinyruo/Yierdis/issues/190)** |
| 性质 | 取证汇总报告（可独立于原 pack 阅读；非 ADR、非产品规格） |

---

## 1. 摘要 / 一句话结论

**占用侧已闭合，失步第一现场仍开放：** 孤儿化的 Surefire fork JVM（PID 3381514）把约 **12 GiB** Old 区钉死，是因为 `DetachedEntries` 计数与 live slots 失步后，`reclaimDetachedEntriesUnchecked` 在 catch 后空转，紧循环抛出约 **1720 万** 同构 `IllegalStateException`（message：`detached key-directory entry count exceeds live slots`）及其 HotSpot backtrace 数组；无 `-Xmx` 放大 thrash 窗口；进程最终被 kill。根因写路径（计数为何漂）与完整 GC root 仍未知；修复已拆为 [#190](https://github.com/skt-shinyruo/Yierdis/issues/190)–[#194](https://github.com/skt-shinyruo/Yierdis/issues/194)。

---

## 2. 事故始末时间线

时间均为 **CST（UTC+8）**，主机 WSL2 / AMD Ryzen 9 9950X / ~47 GiB RAM / JDK 25 / G1。

| 时刻（约） | 事件 |
|------------|------|
| **2026-10-09 21:23** | 在 `.worktrees/lane-errtext` 启动过滤测试（终端 tee → `cli-tests.out`）。Surefire run id：`2026-10-09T21-23-25_057-jvmRun1`。 |
| **21:30:57** | fork JVM **PID 3381514** 启动（`surefirebooter-*.jar`）。 |
| **21:23–22:09** | 反复嵌入式 `TestServer` BOOT/STOP；共享 `/tmp/log/yierdis/server.log` 可见大量 `native memory backend` / `yierdis stopping`。中间曾出现 `ClassNotFoundException: YierdisHllOps$1`（约 21:33）等，**非**本次堆占用直接证据。 |
| **22:09:09** | `server.log` 最后一条业务活动（含 `yierdis started on 127.0.0.1:7275` 一类行来自共享日志混写）；**之后应用日志停更** ≈ mutator 被 GC 掐死。 |
| **测试末尾（写入 `cli-tests.out`）** | Surefire 通信线程 OOM：`surefire-forkedjvm-stream-flusher`、`surefire-forkedjvm-command-thread` 的 `OutOfMemoryError`（UncaughtExceptionHandler）。堆已爆，但进程**未退出**。 |
| **之后数小时** | 进入 **G1 Compaction thrash**：Old≈99.94%，FGC≈1–2/s，FGCT≈墙钟 98%，进程 CPU≈2200–2300%（几乎全是 GC 线程）。Maven 父进程退出 → PPID=`/init`（766198）→ **孤儿化**；worktree `lane-errtext` 与 surefire jar 被删，cwd/fd 显示 `(deleted)`。 |
| **23:10–23:18** | 早期只读 GC 采样（`jstat` / `/proc`）：确认 thrash；`jcmd` attach 失败。 |
| **2026-10-10 00:13–00:17** | 统一事故包首轮打包（`CAPTURE.txt` 等）；RSS 钉死 ≈12.2 GiB。 |
| **00:23 起 / jstat 主窗约 06:38** | pre-kill 最后一轮只读采证（更长 jstat、完整 `/proc`、从已删 surefirebooter 打捞 Class-Path）；**仍未发 kill 信号**。 |
| **08:39–09:10** | 用 **jhsdb / clhsdb（ptrace）** 采 histo / inspect / jstack；`jcmd`/`jmap` attach 仍失败。占用对象与 message 闭合。 |
| **同日（histo 前后）** | 假说复现：`BootstrapLeakProbe`（40 次 start/stop）堆不涨；`main` / `issues-175-182` + 同过滤 + **`-Xmx1g`** → **未复现 thrash**（有功能失败，另论）。 |
| **取证完成后** | **PID 3381514 已 kill**（对话已确认）。机器 GC 打满 CPU 的病灶消除。 |
| **随后** | to-tickets：创建 **#190–#194**；结论写入父票 #190 body。 |

### 2.1 触发命令（已证实）

```bash
cd .../lane-errtext && mvn -q -o -Dmaven.repo.local=/home/feng/.m2/repository \
  -pl yierdis-client,yierdis-cli -am test \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest='*Command*,*Client*,*Cli*,*Connection*'
```

### 2.2 进程画像（thrash 稳态）

| 指标 | 数值 |
|------|------|
| PID | 3381514 |
| 角色 | Maven Surefire fork（`yierdis-client`） |
| RSS | ≈ **12.2 GiB**（钉死；`VmSwap=0`） |
| Old 占用 | **99.94%**（OU ≈ 11.76 GiB） |
| Young | 容量被压到 **0** |
| FGC 频率 | ≈ **1–2 /s**（生命周期均值 ≈1.05 /s） |
| 平均 FGC 停顿 | ≈ **930–941 ms** |
| FGCT / 墙钟 | ≈ **98%** |
| 进程 %CPU | ≈ **2280%**（约 22–23 条 GC 工作线程） |
| 主机 load | ≈ **26–27**（32 核） |
| attach | `jcmd`/`jmap` → `AttachNotSupportedException` |

卡顿主因是 **CPU 被该 JVM 的 GC 线程占满**，不是磁盘满，也不是该进程自身疯狂换页。

---

## 3. 探索流程（问题如何被推进）

本节按实际调查顺序写，便于复盘「下一步该信什么」。

### 3.1 只读取证 → 确认表象

1. 发现高 CPU / 高 RSS 的 Java 进程；确认是 Surefire fork，且 worktree/jar 已删 → **孤儿**。
2. 读 `cli-tests.out` 末尾 → 明确 **OOM 已发生**，但进程仍活。
3. `jstat -gc*` / `-gccause` → Old≈100%、Young=0、`GCC=G1 Compaction Pause`、FGCT≈墙钟 → **GC thrash 死亡螺旋**（OU 不随 FGC 下降）。
4. `/proc` 线程 CPU 排名：前列全是 `GC Thread#N`。
5. **无**原生 `-Xlog:gc*`；有效 GC 证据 = jstat + hsperf + OOM 行。

当时尚无法回答「堆里是什么」：`jcmd` attach 因 thrash 超时失败。早期叙事只能推断「可能是 TestServer/Netty 泄漏」——**该推断后被 histo 作废（占用对象不是笼统 Netty 未释放）**。

### 3.2 假说与复现（负结果）

为区分「单纯 Bootstrap 启停泄漏」vs「特定路径爆炸」：

| 实验 | 做法 | 结果 |
|------|------|------|
| `BootstrapLeakProbe` | 仿 `TestServer`：40 次 start/stop（可选 client PING），`-Xmx512m` | 堆 used 钉在 ~5M，`PROBE_DONE` → **否定「裸启停必泄漏」** |
| 同过滤 + `-Xmx1g`（`main`） | 同 `-Dtest` 过滤 | **未 thrash**；约数分钟结束；有功能失败（如 `ConnectionTest` Errors） |
| 同过滤 + `-Xmx1g`（`issues-175-182`） | 同上 | **未 thrash**；例：`TypedCommandTest.rawMgetIsUnknownAndConnectionStaysUsable` Failure |

结论：当前默认堆/分支组合**不必然**复现 12 GiB ISE 风暴；事故现场仍以 dumps/histo 为准。机制差（为何 lane-errtext 那次 thrash）**未钉死**。

### 3.3 为何无法 jcmd；改用 jhsdb

- thrash 时 HotSpot 无法在超时内响应 attach socket（无可用 `.java_pid`）。
- SIGQUIT 曾尝试：stdout/stderr 是**无读者的孤儿 pipe**，拿不到有用 dump。
- **有效路径**：`jhsdb jmap --histo` / `--heap`、clhsdb `inspect` / `jstack` / `revptrs`（**ptrace**，不依赖 attach）。

### 3.4 histo 点名 → 代码回路

`jhsdbjmap --histo` Top5 ≈ **11.75 GiB**，全部可解释为 ~17.23M `IllegalStateException` + 标准 Throwable backtrace 数组布局（见 §5）。

clhsdb inspect（堆低/中/高地址样本）：`detailMessage` **全部**为

```text
detached key-directory entry count exceeds live slots
```

抛点：`NativeKeyDirectory.DetachedEntries.takeNext()`。

对照源码闭合放大回路：

```text
YierdisDbDataMaintenance.reclaimDetachedEntriesUnchecked
  while (detachedEntryCount() > 0) {
      reclaimDetachedEntry();   // → DetachedEntries.takeNext()
  }
```

- `takeNext()`：slots 已空但 `detachedEntryCount > 0` 时 **先抛 ISE**，**不执行** `detachedEntryCount--`。
- 外层 catch 后 `while` 仍真 → **紧循环分配同构 ISE + backtrace** → 填满堆 → G1 收不回 → thrash。

main 栈（clhsdb）：

```text
TypedCommandTest.connectionCommandsCoverSuccessAndServerErrors
  → TestServer.close
  → YierdisServerBootstrap.close / closeInternal
  → CompletableFuture.join  (WAITING)
```

该用例路径含 **FLUSHDB ASYNC** 等会 detach/reclaim 的操作：main 卡在 close join 时，reclaim 侧已把堆打满。

### 3.5 to-tickets → 进程关闭 → 结论入 issue

1. 将修复链拆为父票 **#190** + 子票 **#191–#194**（见 §7）。
2. 有效结论（histo 表、message、回路、卡点、时间线、负结果、未知项、frontier）写入 **#190** body（issue 不能塞整包大日志）。
3. 取证完成后 **kill PID 3381514**。
4. 原只读 pack 目录保留为可选本地证据；本报告为删 pack 后的**独立叙事**。

---

## 4. 根因：已证实 vs 仍未知

### 4.1 已证实

| 命题 | 证据强度 |
|------|----------|
| ~12 GiB Old ≈ ~1720 万 ISE + backtrace 数组 | histo Top5 与 OU/RSS 对齐；数组:ISE 比值 ≈ 1 / 2 |
| message 唯一且为 DetachedEntries 不变量失败 | clhsdb inspect 多样本，同一 interned String |
| 抛点 `DetachedEntries.takeNext()` | message + 源码 |
| reclaim catch-and-continue + 计数不减 = **无界分配放大器** | 源码路径与 histo 一一对应（HIGH） |
| main 卡在 `TypedCommandTest` → `TestServer.close` → `join` | clhsdb jstack |
| OOM → 未退出 → G1 Compaction thrash → 孤儿化 | cli-tests.out + jstat + `/proc` |
| 无 `-Xmx` 放大 thrash 窗口（非占用对象本身） | Surefire 配置几乎只有版本号 |
| 裸 Bootstrap 启停不必然泄漏 | BootstrapLeakProbe |
| 同过滤 + `-Xmx1g` 对照未 thrash | harness 输出 |

### 4.2 仍未知

| 项 | 状态 |
|----|------|
| `DetachedEntries` 计数与 live slots **失步的第一现场**（哪条写路径让 count 漂） | **未知**；优先审计 detach / reclaim / FLUSHDB（含 ASYNC）/ table release |
| 17M ISE 的完整 GC root / dominator | **未知**；revptrs 样本曾报 `no live references`（可达性不确定，但连续 FGC 后仍占满 Old） |
| 为何 lane-errtext 事故 thrash、同过滤 `-Xmx1g` 对照未 thrash | 对照已做，**机制差未钉死** |

### 4.3 因果链（占用侧已闭合）

```text
DetachedEntries 计数与 live slots 失步（第一现场仍未知）
        │
        ▼
reclaim 紧循环抛 ISE（计数不减；catch 后继续）
        │
        ▼
~17M ISE + backtrace ≈ 11.75 GiB 钉死 Old
        │
        ▼
G1 Young→0；Compaction Pause ~1–2/s；FGCT≈墙钟
        │
        ▼
jcmd 不可用；mutator/日志停更；孤儿 JVM 空转
        │
        ▼
取证（jhsdb）→ kill → 修复票 #190–#194
```

---

## 5. 关键证据数字

### 5.1 Histogram Top5（`jhsdb jmap --histo`）

| # | 类型 | instances | bytes |
|---|------|-----------|-------|
| 1 | `long[]` | 17 229 029 | 4 686 594 440（≈4.69 GiB） |
| 2 | `Object[]` | 34 462 278 | 3 380 221 808（≈3.38 GiB） |
| 3 | `int[]` | 17 230 623 | 2 481 213 824（≈2.48 GiB） |
| 4 | `short[]` | 17 228 921 | 1 378 313 808（≈1.38 GiB） |
| 5 | `IllegalStateException` | 17 228 913 | 689 156 520（≈0.69 GiB） |

- Top5 合计 **≈ 11.75 GiB**（与 jstat OU ≈ 11.76 GiB / `jhsdb --heap` used ≈ 12040 MB 对齐）。
- `long[]`/`int[]`/`short[]` : ISE ≈ **1.000**；`Object[]` : ISE ≈ **2.000** → 标准 HotSpot Throwable backtrace chunks（每异常一套 size-32），**不是**业务大缓冲泄漏。

### 5.2 Message / 方法 / 卡点

| 项 | 值 |
|----|-----|
| ISE message | `detached key-directory entry count exceeds live slots` |
| 抛点 | `NativeKeyDirectory.DetachedEntries.takeNext()`（约 L966） |
| 放大循环 | `YierdisDbDataMaintenance.reclaimDetachedEntriesUnchecked` |
| reclaim 入口 | `NativeKeyDirectory.reclaimDetachedEntry` → `takeNext()` |
| 卡住测试 | `TypedCommandTest.connectionCommandsCoverSuccessAndServerErrors` |
| close 路径 | `TestServer.close` → `YierdisServerBootstrap.closeInternal` → `CompletableFuture.join` |
| 相关命令路径 | **FLUSHDB ASYNC**（该用例会 exercise detach/reclaim） |

### 5.3 GC / 系统（采样窗口代表值）

| 指标 | 数值 |
|------|------|
| MaxHeap / used（jhsdb） | ≈ 12048 / 12040 MB（99.94%，几乎全 Old） |
| O | 99.94% |
| FGC 速率（00:14 窗） | ≈ 1.056/s；avg pause ≈ 941 ms；FGCT/wall ≈ 99.4% |
| 生命周期均值（较早窗） | FGC ≈1.05/s；平均停顿 ≈930 ms；FGCT/存活 ≈97.8% |
| RSS | ≈ 12 788 412 kB |
| 线程 | ~48（大量 GC） |

### 5.4 日志解读注意（避免误判）

共享 `server.log` 全文件计数（多进程混写）：

| 模式 | 约计数 | 含义 |
|------|--------|------|
| BOOT `native memory backend` | ≈985 | `Bootstrap.startInternal()` 开头即打 |
| STOP `yierdis stopping` | ≈981 | `Bootstrap.close()` |
| START `yierdis started` | ≈13 | **仅** `YierdisServer.main` 路径；**TestServer 不打这条** |

因此：**BOOT→STOP 且没有 START，对客户端集成测试是正常生命周期**，不是「启动失败」。`started on :7275` 等行可能来自独立 server 进程与 TestServer 混在同一 logback 文件。

`noCleanup=true` 只把 `cleanupIntervalMillis→0`（maintenance 走 deferred tick），**不是**「close 时不清理」；勿误判为泄漏开关。

---

## 6. 配置放大器（无 `-Xmx`）

事故时 Surefire 配置几乎只有插件版本（无有效 `argLine`）：

- **无** `-Xmx` → 堆可涨到 ~12 GiB，给 ISE 风暴足够空间，再进入长时间 Compaction thrash，而不是尽快 OOM 退出并留下可诊断 dump。
- **无** `-Xlog:gc*` → 无原生日志，只能靠 jstat/hsperf 事后推算。
- **无** 单测超时 / `surefire.exitTimeout` 一类硬截止 → 孤儿 fork 可空转数小时。
- **无** `HeapDumpOnOutOfMemoryError` → OOM 后缺少 hprof。

这些是 **放大器 / 诊断缺口**，不是 histo 上的占用对象本身。加固见 [#192](https://github.com/skt-shinyruo/Yierdis/issues/192)。

---

## 7. 修复切片与 issue 链接

父票跟踪整条链：

**[#190 — DetachedEntries 计数失同步 → reclaim ISE 风暴 / 堆钉死](https://github.com/skt-shinyruo/Yierdis/issues/190)**（OPEN）

| Issue | 标题 | 阻塞 | 角色 |
|-------|------|------|------|
| [#191](https://github.com/skt-shinyruo/Yierdis/issues/191) | Prefactor：reclaim fail-closed，阻断 ISE 风暴 | 无 | 先掐放大器（frontier） |
| [#192](https://github.com/skt-shinyruo/Yierdis/issues/192) | 可选：Surefire `-Xmx` + HeapDumpOnOOM | 无 | 诊断加固，可与 #191 并行（frontier） |
| [#193](https://github.com/skt-shinyruo/Yierdis/issues/193) | 修复 DetachedEntries count ↔ live slots / takeNext | #191 | 真正修失同步根因 |
| [#194](https://github.com/skt-shinyruo/Yierdis/issues/194) | 回归：FLUSHDB ASYNC → reclaim/close 不得风暴 | #191、#193 | 贴近事故路径的回归 |

**当前可开工 frontier：#191、#192。**

建议实施顺序：fail-closed（#191）→ 写路径修不变量（#193）→ 回归（#194）；Surefire 堆上限（#192）正交、可并行。

---

## 8. 附录

### 8.1 原取证 pack 路径（已/将删除）

```text
docs/research/2026-10-09-lane-errtext-surefire-incident/
```

- 性质：只读采集包（jstat、`/proc`、histo dumps、源码副本、复现 harness 等）。
- 处置约定：**可删 / 宜勿整包提交 Git**；用户自删。删后以**本报告** + **#190 body** 为叙事与修复依据。
- 本报告与 pack **平级**，故意不依赖其相对链接。

### 8.2 pack 内主要子树（索引，非嵌入）

| 子路径 | 内容 |
|--------|------|
| `README.md` | pack 入口与 §0 有效结论 |
| `analysis-g1-full-gc-thrash.md` | GC thrash 详细叙事 |
| `analysis-heap-occupancy-ise-storm.md` | ISE 风暴 / histo 专文 |
| `CAPTURE.txt` / `MANIFEST.txt` | 打包元数据与文件字节清单 |
| `proc/` | `/proc` 取证 |
| `jstat/` / `gc-jstat-early/` / `hsperf/` | GC 采样 |
| `logs/` | `cli-tests.out`、共享 `server.log` 副本、终端片段 |
| `code-refs/` | 相关源码副本与 Surefire 配置摘录 |
| `harness/` | BootstrapLeakProbe、`-Xmx1g` 对照输出 |
| `harness/dumps/` | jhsdb/clhsdb 铁证（见下） |
| `pre-kill-capture-20261010-002345/` | kill 前最后一轮只读采证 |

### 8.3 `harness/dumps/` 文件名索引

结论入口：`EVIDENCE-SUMMARY.txt`、`INDEX.txt`。

**优先读**

| 文件 | 说明 |
|------|------|
| `jhsdb-histo-complete.log` | 完整 class histogram；Top5 = ~11.75 GiB |
| `jhsdb-jmap-heap.*.log` / `jhsdb-jmap-heap.retry.log` | MaxHeap / used |
| `main-thread-stack.txt` | main：TypedCommandTest → TestServer.close → join |
| `EVIDENCE-SUMMARY.txt` | 一行版已证实结论 |
| `clhsdb-ise-messages.log` | ISE detailMessage 样本 |
| `clhsdb-inspect-revptrs.log` | inspect + revptrs |
| `clhsdb-jstack-scanoops-ise.log` | jstack + 堆上 ISE 扫描（体积大） |

**次要 / 失败尝试**

| 文件 | 说明 |
|------|------|
| `jcmd-*.log` / `jmap-histo.fail.log` | attach 失败记录 |
| `sigquit-attempt.txt` / `attempt-20261010-083937.txt` | 信号尝试（孤儿 pipe） |
| `jhsdb-histo-LIVE.log` | 另一轮 histo 摘要 |
| `jhsdb-jmap-histo.*.log` | 早期/截断 histo |
| `clhsdb-ise-*.log` / `clhsdb-universe-ise.log` / `clhsdb-ise-midheap.log` | 额外 ISE 采样 |
| `jstat-gcutil.now.log` | 采 histo 时点的 gcutil |

### 8.4 harness 复现产物（文件名）

| 文件 | 说明 |
|------|------|
| `BootstrapLeakProbe.java` | 扔弃式启停探针 |
| `probe-40-client.out` | 40 周期 + client；堆不涨 |
| `incident-filter-1g.out` | main + 同过滤 + `-Xmx1g` |
| `issues-175-182-filter-1g.out` | issues-175-182 + 同过滤 + `-Xmx1g` |
| `incident-filter-gc.log.*` / `issues-175-182-gc.log.*` | 对照跑的 GC 日志分片 |
| `connectiontest-512m.out` / `java-rss-sample.txt` | 辅助采样 |

### 8.5 元数据

- 报告路径：`docs/research/2026-10-10-detached-entries-ise-storm-incident-report.md`
- 撰写约束：不 commit；不改 `.gitignore`；不删除 pack（用户自删）
- 事故 PID：**已 kill**；勿再对 3381514 做 jcmd/jhsdb
- 背景 lane：`lane-errtext` / #182（与本次 DetachedEntries 修复链正交；本报告聚焦 ISE 风暴事故）
