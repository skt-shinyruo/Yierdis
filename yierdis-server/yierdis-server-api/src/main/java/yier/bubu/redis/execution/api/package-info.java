/**
 * Execution API/SPI audience classification.
 *
 * <p>This package is the execution contract surface owned by
 * {@code yierdis-server-api}.</p>
 *
 * <ul>
 *     <li>ExecutionRequest - API。受众：协议适配器、executor、command dispatcher、事务 replay、runtime 与测试。</li>
 *     <li>ByteArrayExecutionRequest - API。受众：协议适配器、事务 replay，以及需要 heap snapshot 的 runtime 与测试。</li>
 *     <li>ReplySink - API. Audience: command/storage value streaming adapters and reply writer implementations.</li>
 *     <li>RedisReplyWriter - SPI。受众：executor、server 与协议实现；把语义回复和控制错误编码到已预留的 reply sink。</li>
 *     <li>RedisReplyWriterFactory - API. Audience: executor, server/protocol adapter composition, tests.</li>
 *     <li>CommandSession - API。受众：executor、command dispatcher、server 会话实现与测试；直接承载 client metadata、transaction、stats 和 protocol 能力。</li>
 *     <li>DbIndexSession - API. Audience: DB routing and SELECT command handlers.</li>
 *     <li>ConnectionStatsView - API. Audience: server INFO/STATS views, executor/server observability, tests.</li>
 *     <li>TransactionState - API。受众：事务命令 handler、连接 session owner 与测试。</li>
 *     <li>CommandExecutionContext - API。受众：PreparedCommand；由执行器在回复容量预留后创建，提供本次会话。</li>
 *     <li>PreparedCommand - API。受众：command dispatcher、executor 与命令实现；封装预留形状、执行前校验、语义结果和保留资源的关闭责任。</li>
 *     <li>ReplyShape / ReplySizer - API/SPI。受众：命令层、executor 与协议实现；命令层描述语义形状，协议实现计算 wire 大小。</li>
 * </ul>
 */
package yier.bubu.redis.execution.api;
