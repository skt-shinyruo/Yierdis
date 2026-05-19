/**
 * Execution API/SPI audience classification.
 *
 * <p>This package is the execution contract surface owned by
 * {@code yierdis-server-api}.</p>
 *
 * <ul>
 *     <li>ExecutionRequest - API. Audience: protocol adapters, executor, engine, command handlers, runtime, tests.</li>
 *     <li>ByteArrayExecutionRequest - API. Audience: protocol adapters, engine replay, runtime/tests needing heap snapshots.</li>
 *     <li>ExecutionRecord - API. Audience: command transaction/replay logic, runtime change tracking, tests.</li>
 *     <li>ReplySink - API. Audience: command/storage value streaming adapters and reply writer implementations.</li>
 *     <li>ReplyWriter - API. Audience: command handlers, engine, protocol adapters, server reply implementations, tests.</li>
 *     <li>ReplyWriterFactory - API. Audience: executor, server/protocol adapter composition, tests.</li>
 *     <li>Session - API. Audience: executor, engine, server session implementations, tests.</li>
 *     <li>DbIndexSession - API. Audience: DB routing and SELECT command handlers.</li>
 *     <li>ClientMetadataSession - API. Audience: CLIENT, AUTH, and server HELLO command handlers.</li>
 *     <li>TransactionSession - API. Audience: transaction command handlers and command processor queueing.</li>
 *     <li>ConnectionStatsSession - API. Audience: server INFO/STATS views.</li>
 *     <li>ProtocolNegotiationSession - API. Audience: protocol reply writers and HELLO command handlers.</li>
 *     <li>ServerSession - API. Audience: compatibility aggregate and engine/session implementations.</li>
 *     <li>DbIndexProvider - compatibility/deprecated. Audience: legacy embedders only; command routing uses DbIndexSession.</li>
 *     <li>ConnectionStatsView - API. Audience: server INFO/STATS views, executor/server observability, tests.</li>
 *     <li>TransactionState - API. Audience: command transaction handlers, engine sessions, server sessions, tests.</li>
 *     <li>CommandContext - API. Audience: engine and command handlers; executor/server must not construct it directly.</li>
 *     <li>Command - compatibility/deprecated. Audience: legacy embedders only; new code uses ExecutionRequest.</li>
 * </ul>
 */
package yier.bubu.redis.execution.api;
