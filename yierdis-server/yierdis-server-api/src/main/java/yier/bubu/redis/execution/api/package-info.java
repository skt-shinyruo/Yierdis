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
 *     <li>RedisReplyWriter - API. Audience: command handlers and server reply implementations that need the explicit Redis command reply model.</li>
 *     <li>RedisReplyWriterFactory - API. Audience: executor, server/protocol adapter composition, tests.</li>
 *     <li>CommandSession - API。受众：executor、engine、server 会话实现与测试。</li>
 *     <li>DbIndexSession - API. Audience: DB routing and SELECT command handlers.</li>
 *     <li>ClientMetadataSession - API. Audience: CLIENT, AUTH, and server HELLO command handlers.</li>
 *     <li>TransactionSession - API. Audience: transaction command handlers and command processor queueing.</li>
 *     <li>ConnectionStatsSession - API. Audience: server INFO/STATS views.</li>
 *     <li>ProtocolNegotiationSession - API. Audience: protocol reply writers and HELLO command handlers.</li>
 *     <li>DbIndexProvider - compatibility/deprecated. Audience: legacy embedders only; command routing uses DbIndexSession.</li>
 *     <li>ConnectionStatsView - API. Audience: server INFO/STATS views, executor/server observability, tests.</li>
 *     <li>TransactionState - API. Audience: command transaction handlers, engine sessions, server sessions, tests.</li>
 *     <li>CommandPreparationContext - API。受众：命令 preparer；仅在回复容量预留前读取完整会话状态。</li>
 *     <li>CommandExecutionContext - API。受众：PreparedCommand；由执行器在回复容量预留后创建，并提供本次请求的 mutation context 与 reply writer。</li>
 *     <li>PreparedCommand - API。受众：engine、executor 与命令实现；封装回复形状、执行前校验和保留资源的关闭责任。</li>
 *     <li>ReplyShape / ReplySizer - API/SPI。受众：命令层、executor 与协议实现；命令层描述语义形状，协议实现计算 wire 大小。</li>
 * </ul>
 */
package yier.bubu.redis.execution.api;
