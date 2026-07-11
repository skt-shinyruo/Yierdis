# Single-Node Production Hardening Program

This program implements the approved design in [the production-hardening specification](../specs/2026-07-10-single-node-production-hardening-design.md). Execute the plans in order; each stage leaves the repository buildable and is the baseline for the next stage.

1. [Stage 1: Allocator and Usage Foundation](2026-07-10-single-node-hardening-stage-1-allocator-usage.md)
2. [Stage 2: Failure-Atomic Mutations and OOM](2026-07-10-single-node-hardening-stage-2-failure-atomic-mutations.md)
3. [Stage 3: Bounded Hash Tables](2026-07-10-single-node-hardening-stage-3-bounded-hash-tables.md)
4. [Stage 4: Maxmemory Convergence](2026-07-10-single-node-hardening-stage-4-maxmemory-convergence.md)
5. [Stage 5: RESP Ingress Admission](2026-07-10-single-node-hardening-stage-5-resp-ingress-admission.md)
6. [Stage 6: Reliable Commit Stream](2026-07-10-single-node-hardening-stage-6-commit-stream-convergence.md)
7. [Stage 7: Bounded Ordered RESP Egress and Final Convergence](2026-07-10-single-node-hardening-stage-7-bounded-ordered-resp-egress.md)

The immutable baseline for throughput comparisons is commit `fb857980^` (the pre-design production code). Preserve raw benchmark output for that commit before executing Stage 1. The final gate compares GET, SET, HSET, and ZADD median operations per second against this baseline and requires every command to retain at least 90% of baseline throughput.

Do not start a later stage while an earlier stage has failing focused tests, architecture tests, or the full JDK 25 Maven suite. Temporary adapters are allowed only where a later plan explicitly removes them.

## Design Traceability

| Approved design requirement | Implementation owner |
| --- | --- |
| Lazy 4096-slot object metadata, retained generations, zero empty-DB metadata | [Stage 1](2026-07-10-single-node-hardening-stage-1-allocator-usage.md), Tasks 2 and 7 |
| Account allocator-owned Java structures and native segments/pages in estimates and snapshots | [Stage 1](2026-07-10-single-node-hardening-stage-1-allocator-usage.md), Tasks 3 and 4; [Stage 4](2026-07-10-single-node-hardening-stage-4-maxmemory-convergence.md), Task 1 |
| Warm-page retention, bounded pressure trim, reusable primitive page IDs, releasable directory segments, and no historical page/span descriptor growth | [Stage 1](2026-07-10-single-node-hardening-stage-1-allocator-usage.md), Tasks 3 and 4 |
| Remove per-object/root handle mirrors and production allocator synchronization | [Stage 1](2026-07-10-single-node-hardening-stage-1-allocator-usage.md), Tasks 4 through 6 |
| Unified expected native-capacity OOM taxonomy and recoverable command reply | [Stage 2](2026-07-10-single-node-hardening-stage-2-failure-atomic-mutations.md), Tasks 1, 2, and 9 |
| Prepared mutations, physical-growth reconciliation, non-allocating commit, no rollback after COMMITTING | [Stage 2](2026-07-10-single-node-hardening-stage-2-failure-atomic-mutations.md), Tasks 3 through 8 |
| Deletion-only RECLAMATION admission commits only non-positive deltas and never recursively invokes cleanup, eviction, or the global governor | [Stage 2](2026-07-10-single-node-hardening-stage-2-failure-atomic-mutations.md), Tasks 3 and 4; [Stage 6](2026-07-10-single-node-hardening-stage-6-commit-stream-convergence.md), Task 6 |
| Failure-atomic String/List/Hash/Set/ZSet/HLL/key/TTL paths | [Stage 2](2026-07-10-single-node-hardening-stage-2-failure-atomic-mutations.md), Tasks 4 through 9 |
| SipHash, shared grow/compact/shrink policy, bounded rehash and maintenance | [Stage 3](2026-07-10-single-node-hardening-stage-3-bounded-hash-tables.md), Tasks 1 through 6 and 8 |
| SCAN actual-slot budget, rehash-shadow coverage, and the explicit fewer-than-2^29-generation no-omission horizon | [Stage 3](2026-07-10-single-node-hardening-stage-3-bounded-hash-tables.md), Task 7 |
| O(1) heap snapshots and maintenance discovery with incremental counters/registries rather than collection walks | [Stage 3](2026-07-10-single-node-hardening-stage-3-bounded-hash-tables.md), Task 8; [Stage 4](2026-07-10-single-node-hardening-stage-4-maxmemory-convergence.md), Task 1 |
| One physical maxmemory model for per-DB/global scopes and complete disabled-limit stats | [Stage 4](2026-07-10-single-node-hardening-stage-4-maxmemory-convergence.md), Tasks 1 through 3 and 7 |
| Trim before eviction/OOM and require demonstrated physical progress | [Stage 4](2026-07-10-single-node-hardening-stage-4-maxmemory-convergence.md), Tasks 4 through 6 |
| Credited manual reads, retained-capacity ByteBuf accounting, admitted consolidation/copy peaks, FIFO waiters, detached leases, and complete MULTI ownership | [Stage 5](2026-07-10-single-node-hardening-stage-5-resp-ingress-admission.md), Tasks 1 through 6 |
| Ingress configuration, separate observability, disconnect/fuzz leak coverage | [Stage 5](2026-07-10-single-node-hardening-stage-5-resp-ingress-admission.md), Tasks 7 and 8 |
| Retainable internally owned records plus non-retainable, worker-thread callback-scoped sink views | [Stage 6](2026-07-10-single-node-hardening-stage-6-commit-stream-convergence.md), Tasks 1 through 3 |
| Fixed bounded ring, prefilled candidate sequence, allocation-free publish, and held fail-after-commit reservations that cannot be silently canceled | [Stage 6](2026-07-10-single-node-hardening-stage-6-commit-stream-convergence.md), Tasks 2 through 4 |
| DB commit authority replaces command-layer mutation inference | [Stage 6](2026-07-10-single-node-hardening-stage-6-commit-stream-convergence.md), Task 5 |
| Ordered USER/EXPIRED/EVICTED delivery and deferred physical expiry/eviction | [Stage 6](2026-07-10-single-node-hardening-stage-6-commit-stream-convergence.md), Task 6 |
| Failure state, metrics, exact shutdown ownership, timeout reporting | [Stage 6](2026-07-10-single-node-hardening-stage-6-commit-stream-convergence.md), Tasks 7 and 8 |
| DB/runtime dependency boundary and removal of temporary/manual adapters | [Stage 6](2026-07-10-single-node-hardening-stage-6-commit-stream-convergence.md), Task 9 |
| Receive-order reply slots shared by commands, BUSY, protocol errors, internal failures, and close-after output | [Stage 7](2026-07-10-single-node-hardening-stage-7-bounded-ordered-resp-egress.md), Tasks 2 and 5 |
| Hard global, per-connection, and single-reply limits covering slot, source, actual buffer capacity, promise, listener, and queue ownership | [Stage 7](2026-07-10-single-node-hardening-stage-7-bounded-ordered-resp-egress.md), Tasks 1 and 3 |
| FAIR rotation and GLOBAL FIFO behavior when reply capacity blocks a connection | [Stage 7](2026-07-10-single-node-hardening-stage-7-bounded-ordered-resp-egress.md), Tasks 4 and 9 |
| Closeable scalar/mutation results and replayable aggregate sources without unbounded result lists or detached value copies | [Stage 2](2026-07-10-single-node-hardening-stage-2-failure-atomic-mutations.md), Tasks 4 and 5; [Stage 3](2026-07-10-single-node-hardening-stage-3-bounded-hash-tables.md), Task 7; [Stage 7](2026-07-10-single-node-hardening-stage-7-bounded-ordered-resp-egress.md), Tasks 6 and 7 |
| Reply preflight before mutation and close-with-result-unknown after post-commit or write ambiguity | [Stage 2](2026-07-10-single-node-hardening-stage-2-failure-atomic-mutations.md), Task 5; [Stage 7](2026-07-10-single-node-hardening-stage-7-bounded-ordered-resp-egress.md), Tasks 5, 6, and 9 |
| Child-channel registry, ordered shutdown drain, truthful timeout failure, and eventual lease cleanup | [Stage 7](2026-07-10-single-node-hardening-stage-7-bounded-ordered-resp-egress.md), Task 8 |
| Operations docs, bounded soak, four-command 90% throughput gate, full JDK 25 acceptance | [Stage 7](2026-07-10-single-node-hardening-stage-7-bounded-ordered-resp-egress.md), Tasks 10 through 13 |
