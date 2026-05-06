/**
 * Storage result API audience classification.
 *
 * <p>This package keeps the existing {@code yier.bubu.redis.ops.result}
 * legacy package name as a migration compatibility bridge while the owning
 * Maven module is {@code yierdis-storage-api}.</p>
 *
 * <ul>
 *     <li>BulkStringSink - API. Audience: storage implementations, command reply adapters, tests.</li>
 *     <li>BulkStringSequence - API. Audience: collection read operations and command reply adapters.</li>
 *     <li>BulkStringSequences - API. Audience: convenience factories for collection read results.</li>
 *     <li>BulkStringMapPairs - API. Audience: hash read operations and command reply adapters.</li>
 *     <li>BulkStringMapPairsSupport - API. Audience: convenience factories for hash read results.</li>
 *     <li>BulkStringValue - API. Audience: string read operations and command reply adapters.</li>
 * </ul>
 */
package yier.bubu.redis.ops.result;
