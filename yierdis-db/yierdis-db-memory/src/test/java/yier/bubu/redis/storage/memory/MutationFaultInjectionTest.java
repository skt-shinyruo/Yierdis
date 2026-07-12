package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisFfmRegion;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.memory.testkit.FailOnAllocationNativeAllocator;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.expire.YierdisTtlOps;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class MutationFaultInjectionTest {
    private static final byte[] PRIMARY_KEY = b("k");

    @Test
    public void stringMutationFamilyIsFailureAtomic() {
        assertFailureAtomic(
                new MutationCase(
                        "new SET",
                        fixture -> {
                        },
                        fixture -> fixture.stringOps.setString(PRIMARY_KEY, b("new"), SetMode.NORMAL, null)
                ),
                new MutationCase(
                        "existing SET",
                        fixture -> fixture.stringOps.setString(PRIMARY_KEY, b("old"), SetMode.NORMAL, null),
                        fixture -> fixture.stringOps.setString(PRIMARY_KEY, b("new"), SetMode.NORMAL, null)
                ),
                new MutationCase(
                        "SET GET",
                        fixture -> fixture.stringOps.setString(PRIMARY_KEY, b("old"), SetMode.NORMAL, null),
                        fixture -> {
                            WriteResult<StringWriteOps.SetStringValue> result = fixture.stringOps.set(
                                    PRIMARY_KEY,
                                    slice("new"),
                                    SetMode.NORMAL,
                                    null,
                                    true
                            );
                            result.value().close();
                        }
                ),
                new MutationCase(
                        "APPEND",
                        fixture -> fixture.stringOps.setString(PRIMARY_KEY, b("old"), SetMode.NORMAL, null),
                        fixture -> fixture.stringOps.append(PRIMARY_KEY, slice("tail"))
                ),
                new MutationCase(
                        "SETBIT growth",
                        fixture -> fixture.stringOps.setString(PRIMARY_KEY, b("x"), SetMode.NORMAL, null),
                        fixture -> fixture.stringOps.setBit(PRIMARY_KEY, 64L, 1)
                ),
                new MutationCase(
                        "INCRBY",
                        fixture -> fixture.stringOps.setString(PRIMARY_KEY, b("1"), SetMode.NORMAL, null),
                        fixture -> fixture.stringOps.incrBy(PRIMARY_KEY, 2L)
                ),
                new MutationCase(
                        "TTL add",
                        fixture -> fixture.stringOps.setString(PRIMARY_KEY, b("old"), SetMode.NORMAL, null),
                        fixture -> fixture.ttlOps.pexpire(view(PRIMARY_KEY), 5000L)
                ),
                new MutationCase(
                        "TTL replace",
                        fixture -> {
                            fixture.stringOps.setString(PRIMARY_KEY, b("old"), SetMode.NORMAL, null);
                            fixture.ttlOps.pexpire(view(PRIMARY_KEY), 5000L);
                        },
                        fixture -> fixture.ttlOps.pexpire(view(PRIMARY_KEY), 10_000L)
                ),
                new MutationCase(
                        "TTL remove",
                        fixture -> {
                            fixture.stringOps.setString(PRIMARY_KEY, b("old"), SetMode.NORMAL, null);
                            fixture.ttlOps.pexpire(view(PRIMARY_KEY), 5000L);
                        },
                        fixture -> fixture.ttlOps.persist(view(PRIMARY_KEY))
                ),
                new MutationCase(
                        "13th key table growth",
                        fixture -> {
                            for (int i = 0; i < 12; i++) {
                                fixture.stringOps.setString(b("k" + i), b("v"), SetMode.NORMAL, null);
                            }
                        },
                        fixture -> fixture.stringOps.setString(PRIMARY_KEY, b("new"), SetMode.NORMAL, null)
                ),
                new MutationCase(
                        "existing SET with GET and TTL",
                        fixture -> fixture.stringOps.setString(PRIMARY_KEY, b("old"), SetMode.NORMAL, null),
                        fixture -> {
                            WriteResult<StringWriteOps.SetStringValue> result = fixture.stringOps.set(
                                    PRIMARY_KEY,
                                    slice("new"),
                                    SetMode.NORMAL,
                                    ExpireOption.px(5000),
                                    true
                            );
                            result.value().close();
                        }
                )
        );
    }

    @Test
    public void ttlRegionAllocationFailuresAreFailureAtomic() {
        for (long failAt = 1; failAt <= 3; failAt++) {
            try (FaultFixture fixture = FaultFixture.open()) {
                fixture.stringOps.setString(PRIMARY_KEY, b("old"), SetMode.NORMAL, null);
                DbStateSnapshot before = fixture.snapshot();
                fixture.regions.failOnAllocation(failAt);
                try {
                    fixture.ttlOps.pexpire(view(PRIMARY_KEY), 5000L);
                    Assert.fail("expected region allocation failure at " + failAt);
                } catch (YierdisCommandException expected) {
                    Assert.assertEquals(MaxmemoryErrors.OOM_ERR, expected.getMessage());
                    fixture.regions.disableFailures();
                    fixture.assertSnapshotEquals(before);
                }
            }
        }

        for (long failAt = 1; failAt <= 3; failAt++) {
            try (FaultFixture fixture = FaultFixture.open()) {
                fixture.stringOps.setString(PRIMARY_KEY, b("old"), SetMode.NORMAL, null);
                DbStateSnapshot before = fixture.snapshot();
                fixture.regions.failOnAllocation(failAt);
                try {
                    WriteResult<StringWriteOps.SetStringValue> result = fixture.stringOps.set(
                            PRIMARY_KEY,
                            slice("new"),
                            SetMode.NORMAL,
                            ExpireOption.px(5000L),
                            true
                    );
                    result.value().close();
                    Assert.fail("expected region allocation failure at " + failAt);
                } catch (YierdisCommandException expected) {
                    Assert.assertEquals(MaxmemoryErrors.OOM_ERR, expected.getMessage());
                    fixture.regions.disableFailures();
                    fixture.assertSnapshotEquals(before);
                }
            }
        }
    }

    private static void assertFailureAtomic(MutationCase... cases) {
        for (MutationCase caseUnderTest : cases) {
            for (long failAt = 1; failAt <= 128; failAt++) {
                try (FaultFixture fixture = FaultFixture.open()) {
                    caseUnderTest.setup.run(fixture);
                    fixture.allocator.resetAttempts();
                    DbStateSnapshot before = fixture.snapshot();
                    fixture.allocator.failOnAllocation(failAt);
                    try {
                        caseUnderTest.mutate.run(fixture);
                        Assert.assertTrue(
                                caseUnderTest.name + " succeeded before allocation point " + failAt,
                                failAt > fixture.allocator.allocationAttempts()
                        );
                        break;
                    } catch (YierdisCommandException expected) {
                        Assert.assertEquals(MaxmemoryErrors.OOM_ERR, expected.getMessage());
                        fixture.allocator.disableFailures();
                        fixture.assertSnapshotEquals(before);
                        Assert.assertArrayEquals(before.primaryValue, fixture.stringOps.getStringBytes(PRIMARY_KEY));
                    }
                }
            }
        }
    }

    private record MutationCase(String name, FixtureAction setup, FixtureAction mutate) {
        private MutationCase {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(setup, "setup");
            Objects.requireNonNull(mutate, "mutate");
        }
    }

    @FunctionalInterface
    private interface FixtureAction {
        void run(FaultFixture fixture);
    }

    private static final class FaultFixture implements AutoCloseable {
        private final YierdisFfmMemoryRuntime runtime;
        private final FailOnRegionAllocator regions;
        private final FailOnAllocationNativeAllocator allocator;
        private final YierdisFfmExpireIndex expires;
        private final EntryTable entries;
        private final NativeKeyDirectory keyDirectory;
        private final StringRoot stringRoot;
        private final ListRoot listRoot;
        private final HashRoot hashRoot;
        private final SetRoot setRoot;
        private final ZSetRoot zsetRoot;
        private final YierdisDbMemoryLedger ledger;
        private final YierdisStringOps stringOps;
        private final YierdisTtlOps ttlOps;
        private final YierdisDbOwnedResources resources;

        private FaultFixture(
                YierdisFfmMemoryRuntime runtime,
                FailOnRegionAllocator regions,
                FailOnAllocationNativeAllocator allocator,
                YierdisFfmExpireIndex expires,
                EntryTable entries,
                NativeKeyDirectory keyDirectory,
                StringRoot stringRoot,
                ListRoot listRoot,
                HashRoot hashRoot,
                SetRoot setRoot,
                ZSetRoot zsetRoot,
                YierdisDbMemoryLedger ledger,
                YierdisStringOps stringOps,
                YierdisTtlOps ttlOps,
                YierdisDbOwnedResources resources
        ) {
            this.runtime = runtime;
            this.regions = regions;
            this.allocator = allocator;
            this.expires = expires;
            this.entries = entries;
            this.keyDirectory = keyDirectory;
            this.stringRoot = stringRoot;
            this.listRoot = listRoot;
            this.hashRoot = hashRoot;
            this.setRoot = setRoot;
            this.zsetRoot = zsetRoot;
            this.ledger = ledger;
            this.stringOps = stringOps;
            this.ttlOps = ttlOps;
            this.resources = resources;
        }

        private static FaultFixture open() {
            YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("mutation-fault");
            FailOnRegionAllocator regions = new FailOnRegionAllocator(runtime);
            FailOnAllocationNativeAllocator allocator = new FailOnAllocationNativeAllocator(
                    new YierdisStableNativeAllocator(runtime, 4096)
            );
            allocator.bindToCurrentThread();
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator, regions::allocateRegion);
            EntryTable entries = new EntryTable(runtime, allocator);
            NativeKeyDirectory keyDirectory = new NativeKeyDirectory(allocator);
            StringRoot stringRoot = new StringRoot(allocator);
            ListRoot listRoot = new ListRoot(allocator);
            HashRoot hashRoot = new HashRoot(allocator);
            SetRoot setRoot = new SetRoot(allocator);
            ZSetRoot zsetRoot = new ZSetRoot(allocator);
            YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                    0L,
                    MaxmemoryPolicy.NOEVICTION,
                    () -> {
                    },
                    ignored -> {
                    },
                    () -> 0L,
                    () -> null
            );
            YierdisDbKeyLifecycle keyLifecycle = new YierdisDbKeyLifecycle(
                    expires,
                    allocator,
                    runtime,
                    entries,
                    keyDirectory,
                    stringRoot,
                    listRoot,
                    hashRoot,
                    setRoot,
                    zsetRoot,
                    () -> 0L,
                    delta -> ledger.commit(null, delta)
            );
            YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(
                    () -> {
                    },
                    ledger,
                    allocator
            );
            YierdisDbRuntimeInternals internals = new YierdisDbRuntimeInternals(
                    () -> {
                    },
                    executor,
                    keyLifecycle,
                    ledger
            );
            YierdisDbOwnedResources resources = new YierdisDbOwnedResources(runtime, allocator, true, true);
            YierdisStringOps stringOps = new YierdisStringOps(internals);
            YierdisTtlOps ttlOps = new YierdisTtlOps(internals);
            return new FaultFixture(
                    runtime,
                    regions,
                    allocator,
                    expires,
                    entries,
                    keyDirectory,
                    stringRoot,
                    listRoot,
                    hashRoot,
                    setRoot,
                    zsetRoot,
                    ledger,
                    stringOps,
                    ttlOps,
                    resources
            );
        }

        private DbStateSnapshot snapshot() {
            return new DbStateSnapshot(
                    allocator.memoryUsage(),
                    runtime.usedBytes(),
                    runtime.liveRegionCount(),
                    ledger.usedBytes(),
                    ledger.reservedBytes(),
                    allocator.stats().liveObjects(),
                    keyDirectory.size(),
                    expires.size(),
                    stringOps.getStringBytes(PRIMARY_KEY),
                    expires.get(PRIMARY_KEY)
            );
        }

        private void assertSnapshotEquals(DbStateSnapshot before) {
            DbStateSnapshot after = snapshot();
            Assert.assertEquals(before.memoryUsage, after.memoryUsage);
            Assert.assertEquals(before.runtimeUsedBytes, after.runtimeUsedBytes);
            Assert.assertEquals(before.liveRegions, after.liveRegions);
            Assert.assertEquals(before.usedBytes, after.usedBytes);
            Assert.assertEquals(before.reservedBytes, after.reservedBytes);
            Assert.assertEquals(before.liveObjects, after.liveObjects);
            Assert.assertEquals(before.keyCount, after.keyCount);
            Assert.assertEquals(before.expireCount, after.expireCount);
            Assert.assertArrayEquals(before.primaryValue, after.primaryValue);
            Assert.assertEquals(before.expireAtMillis, after.expireAtMillis);
        }

        @Override
        public void close() {
            regions.disableFailures();
            allocator.disableFailures();
            resources.releaseAll(expires, entries, keyDirectory, stringRoot, listRoot, hashRoot, setRoot, zsetRoot);
        }
    }

    private static final class FailOnRegionAllocator {
        private final YierdisFfmMemoryRuntime runtime;
        private final AtomicLong attempts = new AtomicLong();
        private volatile long failAt = -1L;

        private FailOnRegionAllocator(YierdisFfmMemoryRuntime runtime) {
            this.runtime = Objects.requireNonNull(runtime, "runtime");
        }

        private void failOnAllocation(long oneBasedIndex) {
            if (oneBasedIndex <= 0L) {
                throw new IllegalArgumentException("oneBasedIndex must be > 0");
            }
            attempts.set(0L);
            failAt = oneBasedIndex;
        }

        private void disableFailures() {
            failAt = -1L;
        }

        private YierdisFfmRegion allocateRegion(String owner, int bytes) {
            long attempt = attempts.incrementAndGet();
            if (attempt == failAt) {
                throw new NativeCapacityExceededException("injected region allocation failure");
            }
            return runtime.allocateRegion(owner, bytes);
        }
    }

    private record DbStateSnapshot(
            MemoryUsageSnapshot memoryUsage,
            long runtimeUsedBytes,
            long liveRegions,
            long usedBytes,
            long reservedBytes,
            long liveObjects,
            int keyCount,
            int expireCount,
            byte[] primaryValue,
            Long expireAtMillis
    ) {
        private DbStateSnapshot {
            primaryValue = primaryValue == null ? null : Arrays.copyOf(primaryValue, primaryValue.length);
        }
    }

    private static BytesSlice slice(String value) {
        return new ArrayBytesSlice(value.getBytes(StandardCharsets.UTF_8));
    }

    private static BytesSlice view(byte[] value) {
        return new ArrayBytesSlice(Arrays.copyOf(value, value.length));
    }

    private static final class ArrayBytesSlice implements BytesSlice {
        private final byte[] bytes;

        private ArrayBytesSlice(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int length() {
            return bytes.length;
        }

        @Override
        public byte getByte(int index) {
            return bytes[index];
        }

        @Override
        public void writeTo(BytesSink out) {
            out.writeBytes(bytes);
        }
    }
}
