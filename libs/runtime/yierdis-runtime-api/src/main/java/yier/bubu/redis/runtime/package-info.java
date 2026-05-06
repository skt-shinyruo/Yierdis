/**
 * Embedded runtime configuration API audience classification.
 *
 * <p>This package keeps the existing {@code yier.bubu.redis.runtime} package
 * name for migration compatibility while the configuration contract is owned
 * by {@code yierdis-runtime-api}. Concrete instance lifecycle and observability
 * classes remain in the current runtime implementation module for this phase.</p>
 *
 * <ul>
 *     <li>YierdisInstanceConfig - embedded runtime configuration API. Audience: embedded users, server/application composition, runtime implementation, tests.</li>
 * </ul>
 */
package yier.bubu.redis.runtime;
