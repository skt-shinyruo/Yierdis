/**
 * Runtime API/SPI audience classification.
 *
 * <p>This package keeps the existing {@code yier.bubu.redis.runtime.api}
 * package name as a migration compatibility bridge while the owning Maven
 * module is {@code yierdis-runtime-api}.</p>
 *
 * <ul>
 *     <li>YierdisChangeEvent - API. Audience: embedded runtimes, change sinks, replication/AOF-style adapters, tests.</li>
 *     <li>YierdisChangeSink - API. Audience: runtime composition, embedded users, change-event consumers, tests.</li>
 * </ul>
 */
package yier.bubu.redis.runtime.api;
