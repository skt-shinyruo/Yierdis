/**
 * Execution API/SPI audience classification.
 *
 * <p>This package keeps the existing {@code yier.bubu.redis.contract} package
 * name as a migration bridge while the owning Maven module is
 * {@code yierdis-execution-api}.</p>
 *
 * <ul>
 *     <li>ExecutionRequest - API. Audience: protocol adapters, executor, engine, command handlers, runtime, tests.</li>
 *     <li>ByteArrayExecutionRequest - API. Audience: protocol adapters, engine replay, runtime/tests needing heap snapshots.</li>
 *     <li>ExecutionRecord - API. Audience: command transaction/replay logic, runtime change tracking, tests.</li>
 *     <li>ReplySink - API. Audience: command/storage value streaming adapters and reply writer implementations.</li>
 *     <li>ReplyWriter - API. Audience: command handlers, engine, protocol adapters, server reply implementations, tests.</li>
 *     <li>ReplyWriterFactory - API. Audience: executor, server/protocol adapter composition, tests.</li>
 *     <li>Session - API. Audience: executor, engine, command handlers, server session implementations, tests.</li>
 *     <li>ServerSession - API. Audience: engine/session implementations and command handlers needing server-scoped state.</li>
 *     <li>DbIndexProvider - API. Audience: engine routing, command routing, runtime/test routers.</li>
 *     <li>ConnectionStatsProvider - API. Audience: server session implementations and command/server observability.</li>
 *     <li>ConnectionStatsView - API. Audience: server INFO/STATS views, executor/server observability, tests.</li>
 *     <li>TransactionState - API. Audience: command transaction handlers, engine sessions, server sessions, tests.</li>
 *     <li>CommandContext - API. Audience: engine and command handlers; executor/server must not construct it directly.</li>
 *     <li>Command - compatibility/deprecated. Audience: legacy embedders only; new code uses ExecutionRequest.</li>
 * </ul>
 */
package yier.bubu.redis.contract;
