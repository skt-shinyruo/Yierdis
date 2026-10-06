# Degraded 恢复

某个 DB 进入 degraded 之后，进程不会自己把它写回去。stock server 的恢复入口是当前 `SELECT` 选中的库上的 `YDRECONCILE`。内部实现是 `RuntimeDbEngine.reconcileAccounting()`，命令不经过 mutation executor，所以写入门控拦不住它。

配置、容量和停机步骤仍在 [`configuration-and-operations.md`](./configuration-and-operations.md)。本页只放 on-call 恢复。

## 触发条件

degraded 由 `YierdisDbHealth.recordInvariantFailure` 置位，一场事故只保留第一次失败：

- **派生计数下溢**。`expireCount` 只是随 entry 发布、替换、释放更新的派生计数。`reconcileDerivedEntryState` 算出的下一个计数小于 0 时抛 `IllegalStateException("derived expire count underflow")`。抛出点在 `prepared.commit()` 已经开始之后。
- **commit 已经开始之后的其它失败**。executor 做 best-effort promote、settle ledger、release superseded，再由 `postCommitFailure` 标记 degraded。调用方得到 `PostCommitMutationException`（result-unknown）。commit 之后的容量失败同样包成 invariant failure。
- **commit 之前的不变量失败**。`NativeMemoryException` 和 `IllegalStateException`（`isDegradingInvariantFailure`）也会标记 degraded。容量失败（`MemoryLedgerOutOfMemoryException`、`NativeCapacityExceededException`）在 commit 前只返回 OOM，不进入 degraded。
- detached entry 回收自己抛出的 `RuntimeException` 或 `Error` 也会 `recordInvariantFailure` 后再抛出。
- 逻辑账本 `usedBytes()` 和物理快照之间的漂移不会自动 degraded。两边只在这次显式对账里拉齐。

## 可观测表现

- `INFO` 或 `INFO health`：`degraded_databases` 大于 0 时 `ready` 和 `writable` 为 0，并带上最早一场事故的 `first_failure_type` 与 `first_failure_message`。
- 写命令，包括 `DEL`、`FLUSHDB` 和回收类 mutation，回复 `MISCONF DB is in a degraded state; writes are disabled`。`requireWritable` 仍在读取 admission mode 之前执行。
- 未过期的 key 照常读出。已过期的 key 对读命令返回 nil，`EXISTS` 也把它看成不存在。这条读路径不调用 `reclaimExpired`，物理记录和 `keyCount` 都保持原样，直到恢复之后的可写读或维护真正回收。
- 维护节拍先回收 detached entry，因此 `FLUSHDB ASYNC` 留下的旧目录仍能往下清；接着 `requireWritable` 失败。本拍里的过期排空、rehash 和本库 maxmemory enforce 不会跑。异常会中断同一次 tick 的后续 DB、defrag 和 global governor maintenance，调度侧记一条 `maintenance tick failed`。节拍不调用对账。`cleanupIntervalMillis=0` 时只跑 deferred reclamation，这一路只回收 detached entry，不会因为 degraded 抛出上面的 MISCONF。

## 恢复步骤与回复

`YDRECONCILE` 没有参数，只作用于当前选中的 DB，仍在 owner 线程上执行。

1. 读 `INFO health`，确认 `degraded_databases` 和 `first_failure_message`。
2. `SELECT` 到要恢复的库。一次成功只清这一库，其它库保持 degraded。
3. 发送 `YDRECONCILE`。

回复语义：

- `+OK`：物理用量重算成功，`realignUsage` 把逻辑账本对齐到 `max(0, usedBytesForMaxmemory())`，清除 degraded 和这场事故的 first-failure。这个库可以再写。
- `-ERR reconciliation failed`：物理重算抛了 `RuntimeException`。账本保持原样，degraded 保持。失败尝试记在 health 的 last reconciliation 里（`physicalUsedBytes = -1`，`driftBytes = 0`）。这不是成功。

`Error`（例如 `OutOfMemoryError`）不折成 `-ERR reconciliation failed`，继续向外抛。

## 恢复失败之后

`-ERR reconciliation failed` 之后，同一库上的写命令仍是 MISCONF。可以再次执行 `YDRECONCILE`：失败不会改账本，也不会清除 degraded。维护节拍不会代替这次对账。

反复失败表示物理快照重算本身不可用。`DEL` 和 `FLUSHDB` 同样被拒绝，不能用来把库清掉。协议面没有 `SAVE` / `BGSAVE`，进程内数据没有磁盘副本。在重算能够成功之前，把这个进程当作 fail-stop：等底层故障消失后在同一库上重试 `YDRECONCILE`，或者接受重启会丢掉内存数据集。不要把 `-ERR` 当成已经恢复。

## 下溢、degraded 与 YDRECONCILE

派生计数下溢保持 fail-stop。计数已经和 entry 对不上时，把下溢钳成 0 再继续写入，会在错账上接着跑；有了协议恢复入口之后，停写的运维代价是一次 `YDRECONCILE`，而不是改代码或重启。本行为不把下溢改成可降级继续服务。

升级链：

1. commit 内 `expireCount` 下溢，抛 `IllegalStateException("derived expire count underflow")`。
2. commit 已经开始，executor 转入 post-commit settle。DB 标记 degraded，调用方收到 result-unknown（`PostCommitMutationException`）。触发这次下溢的连接被关掉，不返回一个确定的成功或失败。
3. 新连接上的写命令收到 MISCONF。已过期 key 的读返回 nil，物理记录仍在。
4. 在该库上执行 `YDRECONCILE`。成功则 `+OK`，写入恢复；失败则 `-ERR reconciliation failed`，停在第 3 步，按上一节人工处理。
