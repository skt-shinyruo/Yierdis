/**
 * Runtime API/SPI audience classification.
 *
 * <p>This package is the runtime API surface owned by
 * {@code yierdis-server-runtime-api}.</p>
 *
 * <ul>
 *     <li>YierdisInstanceConfig - embedded runtime configuration API. Audience: embedded users, server/application composition, runtime implementation, tests.</li>
 *     <li>YierdisChangeEvent - API. Audience: embedded runtimes, change sinks, replication/AOF-style adapters, tests.</li>
 *     <li>YierdisChangeKind - API. Audience: change sinks that distinguish user commands from synthetic lifecycle deletes.</li>
 *     <li>YierdisChangeSink - API. Audience: runtime composition, embedded users, change-event consumers, tests.</li>
 * </ul>
 */
package yier.bubu.redis.runtime.api;
