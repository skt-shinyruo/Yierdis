/**
 * Storage API/SPI audience classification owned by {@code yierdis-storage-api}.
 *
 * <p>Factory hooks, maxmemory coordination hooks, and implementation-shaped
 * pressure records are marked SPI for this phase.
 * Observability records that still expose storage-memory accounting details are
 * marked compatibility API. Follow-up: move SPI hooks to an explicit
 * {@code .spi} package, and split public observability views from
 * storage-memory accounting details once the runtime-api and storage-memory
 * phases land.</p>
 *
 * <ul>
 *     <li>DbEngine - API. Audience: command handlers, engine/runtime routing, tests.</li>
 *     <li>DbReads - API. Audience: command read handlers, DB implementations, tests.</li>
 *     <li>DbWrites - API. Audience: command write handlers, DB implementations, tests.</li>
 *     <li>StringReadOps - API. Audience: string command handlers and DB implementations.</li>
 *     <li>StringWriteOps - API. Audience: string command handlers and DB implementations.</li>
 *     <li>HashReadOps - API. Audience: hash command handlers and DB implementations.</li>
 *     <li>HashWriteOps - API. Audience: hash command handlers and DB implementations.</li>
 *     <li>ListReadOps - API. Audience: list command handlers and DB implementations.</li>
 *     <li>ListWriteOps - API. Audience: list command handlers and DB implementations.</li>
 *     <li>SetReadOps - API. Audience: set command handlers and DB implementations.</li>
 *     <li>SetWriteOps - API. Audience: set command handlers and DB implementations.</li>
 *     <li>ZSetReadOps - API. Audience: sorted-set command handlers and DB implementations.</li>
 *     <li>ZSetWriteOps - API. Audience: sorted-set command handlers and DB implementations.</li>
 *     <li>HllReadOps - API. Audience: HyperLogLog command handlers and DB implementations.</li>
 *     <li>HllWriteOps - API. Audience: HyperLogLog command handlers and DB implementations.</li>
 *     <li>KeyspaceReadOps - API. Audience: keyspace command handlers and DB implementations.</li>
 *     <li>KeyspaceWriteOps - API. Audience: keyspace command handlers and DB implementations.</li>
 *     <li>TtlReadOps - API. Audience: TTL command handlers and DB implementations.</li>
 *     <li>TtlWriteOps - API. Audience: TTL command handlers and DB implementations.</li>
 *     <li>MemoryOps - API. Audience: server INFO/STATS adapters, runtime observability, tests.</li>
 *     <li>ExpirationManager - API. Audience: runtime maintenance and DB implementations.</li>
 *     <li>DbLifecycleOps - API. Audience: runtime lifecycle and DB implementations.</li>
 *     <li>RuntimeDbEngine - SPI. Audience: runtime assembly and DB factories.</li>
 *     <li>DbEngineFactory - SPI. Audience: runtime assembly and storage implementations.</li>
 *     <li>MaxmemoryCoordinator - SPI. Audience: runtime maxmemory governor and participants.</li>
 *     <li>MaxmemoryCoordinatorAware - SPI. Audience: storage implementations joining maxmemory coordination.</li>
 *     <li>MaxmemoryParticipant - SPI. Audience: runtime governor and participating storage engines.</li>
 *     <li>MaxmemoryUsageSource - SPI. Audience: runtime maxmemory accounting.</li>
 *     <li>MaxmemoryCandidate - SPI. Audience: maxmemory coordination and tests; key identity uses KeyHandle.</li>
 *     <li>MaxmemoryPolicy - API. Audience: CLI args, runtime config, storage implementations, tests.</li>
 *     <li>MaxmemoryErrors - API. Audience: storage implementations and command error mapping.</li>
 *     <li>DbMemoryConstants - SPI. Audience: storage accounting and focused tests.</li>
 *     <li>YierdisMemoryStats - compatibility observability API. Audience: server/runtime observability and memory views; contains storage-memory accounting details that must split later.</li>
 *     <li>YierdisCommandException - API. Audience: storage implementations and command handlers.</li>
 *     <li>WrongTypeException - API. Audience: storage implementations and command handlers.</li>
 *     <li>ExpireOption - API. Audience: command handlers and storage implementations.</li>
 *     <li>SetMode - API. Audience: command handlers and storage implementations.</li>
 *     <li>ValueType - API. Audience: command handlers, observability, and storage implementations.</li>
 *     <li>ScanCursorV2 - compatibility API. Audience: keyspace/HLL scan command handlers and storage implementations; contains keyspace cursor details that must split later.</li>
 *     <li>KeyHandle - SPI. Audience: pressure-path key identity without heap materialization.</li>
 * </ul>
 */
package yier.bubu.redis.storage.api;
