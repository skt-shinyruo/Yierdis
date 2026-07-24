package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.memory.testkit.FailOnAllocationStableMemoryBackend;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.ByteMapSource;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;
import yier.bubu.redis.storage.memory.internal.expire.YierdisNativeExpireIndex;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.expire.YierdisTtlOps;
import yier.bubu.redis.storage.memory.internal.value.YierdisHyperLogLog;

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
                            WriteResult<StringWriteOps.SetStringValue> result = TestDbSupport.commitSetWithOldValue(
                                    fixture.stringOps,
                                    PRIMARY_KEY,
                                    slice("new"),
                                    SetMode.NORMAL,
                                    null
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
                            WriteResult<StringWriteOps.SetStringValue> result = TestDbSupport.commitSetWithOldValue(
                                    fixture.stringOps,
                                    PRIMARY_KEY,
                                    slice("new"),
                                    SetMode.NORMAL,
                                    ExpireOption.px(5000)
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
                fixture.allocator.failOnRegionAllocation(failAt);
                try {
                    fixture.ttlOps.pexpire(view(PRIMARY_KEY), 5000L);
                    Assert.fail("expected region allocation failure at " + failAt);
                } catch (YierdisCommandException expected) {
                    Assert.assertEquals(MaxmemoryErrors.OOM_ERR, expected.getMessage());
                    fixture.allocator.disableRegionFailures();
                    fixture.assertSnapshotEquals(before);
                }
            }
        }

        for (long failAt = 1; failAt <= 3; failAt++) {
            try (FaultFixture fixture = FaultFixture.open()) {
                fixture.stringOps.setString(PRIMARY_KEY, b("old"), SetMode.NORMAL, null);
                DbStateSnapshot before = fixture.snapshot();
                fixture.allocator.failOnRegionAllocation(failAt);
                try {
                    WriteResult<StringWriteOps.SetStringValue> result = TestDbSupport.commitSetWithOldValue(
                            fixture.stringOps,
                            PRIMARY_KEY,
                            slice("new"),
                            SetMode.NORMAL,
                            ExpireOption.px(5000L)
                    );
                    result.value().close();
                    Assert.fail("expected region allocation failure at " + failAt);
                } catch (YierdisCommandException expected) {
                    Assert.assertEquals(MaxmemoryErrors.OOM_ERR, expected.getMessage());
                    fixture.allocator.disableRegionFailures();
                    fixture.assertSnapshotEquals(before);
                }
            }
        }
    }

    @Test
    public void listMutationFamilyIsFailureAtomic() {
        assertFailureAtomic(
                new MutationCase(
                        "new LPUSH",
                        fixture -> {
                        },
                        fixture -> fixture.listOps.lpush(PRIMARY_KEY, Arrays.asList(b("b"), b("a"), b("z")))
                ),
                new MutationCase(
                        "existing RPUSH",
                        fixture -> fixture.listOps.rpush(PRIMARY_KEY, Arrays.asList(b("a"), b("b"))),
                        fixture -> fixture.listOps.rpush(PRIMARY_KEY, Arrays.asList(b("c"), b("d"), b("e")))
                ),
                new MutationCase(
                        "existing LPUSH",
                        fixture -> fixture.listOps.rpush(PRIMARY_KEY, Arrays.asList(b("c"), b("d"))),
                        fixture -> fixture.listOps.lpush(PRIMARY_KEY, Arrays.asList(b("b"), b("a")))
                ),
                new MutationCase(
                        "LPOP count",
                        fixture -> fixture.listOps.rpush(PRIMARY_KEY, Arrays.asList(b("a"), b("b"), b("c"), b("d"))),
                        fixture -> {
                            WriteResult<PoppedValueSequence> result = TestDbSupport.commitPop(
                                    fixture.listOps, PRIMARY_KEY, 2, true
                            );
                            result.value().close();
                        }
                ),
                new MutationCase(
                        "RPOP count",
                        fixture -> fixture.listOps.rpush(PRIMARY_KEY, Arrays.asList(b("a"), b("b"), b("c"), b("d"))),
                        fixture -> {
                            WriteResult<PoppedValueSequence> result = TestDbSupport.commitPop(
                                    fixture.listOps, PRIMARY_KEY, 2, false
                            );
                            result.value().close();
                        }
                )
        );
    }

    @Test
    public void hashAndSetMutationFamiliesAreFailureAtomic() {
        assertFailureAtomic(
                new MutationCase(
                        "new HSET many",
                        fixture -> {
                        },
                        fixture -> fixture.hashOps.hset(
                                PRIMARY_KEY,
                                Arrays.asList(b("a"), b("1"), b("b"), b("2"), b("c"), b("3"))
                        )
                ),
                new MutationCase(
                        "existing single HSET packed",
                        fixture -> fixture.hashOps.hset(
                                PRIMARY_KEY,
                                Arrays.asList(b("a"), b("1"), b("b"), b("2"))
                        ),
                        fixture -> fixture.hashOps.hset(PRIMARY_KEY, Arrays.asList(b("c"), b("3")))
                ),
                new MutationCase(
                        "existing HSET replace and add",
                        fixture -> fixture.hashOps.hset(
                                PRIMARY_KEY,
                                Arrays.asList(b("a"), b("old"), b("b"), b("keep"))
                        ),
                        fixture -> fixture.hashOps.hset(
                                PRIMARY_KEY,
                                Arrays.asList(b("a"), b("new"), b("c"), b("3"), b("d"), b("4"))
                        )
                ),
                new MutationCase(
                        "HSET listpack promotion",
                        fixture -> fixture.hashOps.hset(
                                PRIMARY_KEY,
                                Arrays.asList(b("a"), b("1"), b("b"), b("2"))
                        ),
                        fixture -> fixture.hashOps.hset(
                                PRIMARY_KEY,
                                Arrays.asList(b("large"), repeatedBytes('x', 65), b("next"), b("3"))
                        )
                ),
                new MutationCase(
                        "new SADD many",
                        fixture -> {
                        },
                        fixture -> fixture.setOps.sadd(PRIMARY_KEY, Arrays.asList(b("1"), b("2"), b("3")))
                ),
                new MutationCase(
                        "SADD intset promotion",
                        fixture -> fixture.setOps.sadd(PRIMARY_KEY, Arrays.asList(b("1"), b("2"), b("3"))),
                        fixture -> fixture.setOps.sadd(PRIMARY_KEY, Arrays.asList(b("alpha"), b("beta"), b("gamma")))
                ),
                new MutationCase(
                        "SADD hashtable members",
                        fixture -> fixture.setOps.sadd(PRIMARY_KEY, Arrays.asList(b("seed"), b("1"))),
                        fixture -> fixture.setOps.sadd(PRIMARY_KEY, Arrays.asList(b("alpha"), b("beta"), b("gamma")))
                ),
                new MutationCase(
                        "HDEL fields",
                        fixture -> fixture.hashOps.hset(
                                PRIMARY_KEY,
                                Arrays.asList(b("a"), b("1"), b("b"), b("2"), b("c"), b("3"))
                        ),
                        fixture -> fixture.hashOps.hdel(PRIMARY_KEY, Arrays.asList(b("a"), b("c")))
                ),
                new MutationCase(
                        "SREM members",
                        fixture -> fixture.setOps.sadd(PRIMARY_KEY, Arrays.asList(b("alpha"), b("beta"), b("gamma"))),
                        fixture -> fixture.setOps.srem(PRIMARY_KEY, Arrays.asList(b("alpha"), b("gamma")))
                )
        );
    }

    @Test
    public void existingPackedHsetStagesOnlyChangedPayloads() {
        try (FaultFixture fixture = FaultFixture.open()) {
            List<byte[]> initialPairs = new ArrayList<>();
            for (int index = 0; index < 16; index++) {
                initialPairs.add(b("field-" + index));
                initialPairs.add(b("value-" + index));
            }
            fixture.hashOps.hset(PRIMARY_KEY, initialPairs);

            fixture.allocator.resetAttempts();
            fixture.hashOps.hset(PRIMARY_KEY, Arrays.asList(b("next"), b("value")));

            Assert.assertTrue(
                    "an existing packed HSET must allocate only its replacement root and new field/value payloads",
                    fixture.allocator.allocationAttempts() <= 4L
            );
        }
    }

    @Test
    public void zsetAndHllMutationFamiliesAreFailureAtomic() {
        assertFailureAtomic(
                new MutationCase(
                        "new ZADD many",
                        fixture -> {
                        },
                        fixture -> fixture.zsetOps.zadd(
                                PRIMARY_KEY,
                                Arrays.asList(b("1"), b("a"), b("2"), b("b"), b("3"), b("c"))
                        )
                ),
                new MutationCase(
                        "existing ZADD score replacement",
                        fixture -> fixture.zsetOps.zadd(
                                PRIMARY_KEY,
                                Arrays.asList(b("1"), repeatedBytes('a', 65), b("2"), b("b"))
                        ),
                        fixture -> fixture.zsetOps.zadd(
                                PRIMARY_KEY,
                                Arrays.asList(b("4"), repeatedBytes('a', 65))
                        )
                ),
                new MutationCase(
                        "ZADD packed promotion and multiple members",
                        fixture -> fixture.zsetOps.zadd(
                                PRIMARY_KEY,
                                Arrays.asList(b("1"), b("a"), b("2"), b("b"))
                        ),
                        fixture -> fixture.zsetOps.zadd(
                                PRIMARY_KEY,
                                Arrays.asList(
                                        b("3"), repeatedBytes('c', 65),
                                        b("4"), b("d"),
                                        b("5"), b("e")
                                )
                        )
                ),
                new MutationCase(
                        "new PFADD",
                        fixture -> {
                        },
                        fixture -> fixture.hllOps.pfadd(
                                PRIMARY_KEY,
                                Arrays.asList(b("alpha"), b("beta"), b("gamma"))
                        )
                ),
                new MutationCase(
                        "existing PFADD",
                        fixture -> fixture.hllOps.pfadd(PRIMARY_KEY, Arrays.asList(b("alpha"), b("beta"))),
                        fixture -> fixture.hllOps.pfadd(
                                PRIMARY_KEY,
                                Arrays.asList(b("gamma"), b("delta"), b("epsilon"))
                        )
                ),
                new MutationCase(
                        "PFMERGE replaces destination",
                        fixture -> {
                            fixture.hllOps.pfadd(PRIMARY_KEY, List.of(b("dest-seed")));
                            fixture.hllOps.pfadd(b("source-a"), Arrays.asList(b("a"), b("b"), b("c")));
                            fixture.hllOps.pfadd(b("source-b"), Arrays.asList(b("c"), b("d"), b("e")));
                        },
                        fixture -> fixture.hllOps.pfmerge(
                                PRIMARY_KEY,
                                Arrays.asList(b("source-a"), b("source-b"))
                        )
                )
        );
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
                        try {
                            fixture.assertSnapshotEquals(before);
                        } catch (AssertionError failure) {
                            throw new AssertionError(
                                    caseUnderTest.name + " changed state after allocation failure " + failAt,
                                    failure
                            );
                        }
                        Assert.assertArrayEquals(before.primaryValue, fixture.primaryStringValue());
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
        private final TestBackend runtime;
        private final FailOnAllocationStableMemoryBackend allocator;
        private final YierdisNativeExpireIndex expires;
        private final EntryTable entries;
        private final NativeKeyDirectory keyDirectory;
        private final YierdisDbKeyLifecycle keyLifecycle;
        private final StringRoot stringRoot;
        private final ListRoot listRoot;
        private final HashRoot hashRoot;
        private final SetRoot setRoot;
        private final ZSetRoot zsetRoot;
        private final YierdisDbMemoryLedger ledger;
        private final YierdisStringOps stringOps;
        private final YierdisListOps listOps;
        private final YierdisHashOps hashOps;
        private final YierdisSetOps setOps;
        private final YierdisZSetOps zsetOps;
        private final YierdisHllOps hllOps;
        private final YierdisTtlOps ttlOps;
        private final YierdisDbOwnedResources resources;

        private FaultFixture(
                TestBackend runtime,
                FailOnAllocationStableMemoryBackend allocator,
                YierdisNativeExpireIndex expires,
                EntryTable entries,
                NativeKeyDirectory keyDirectory,
                YierdisDbKeyLifecycle keyLifecycle,
                StringRoot stringRoot,
                ListRoot listRoot,
                HashRoot hashRoot,
                SetRoot setRoot,
                ZSetRoot zsetRoot,
                YierdisDbMemoryLedger ledger,
                YierdisStringOps stringOps,
                YierdisListOps listOps,
                YierdisHashOps hashOps,
                YierdisSetOps setOps,
                YierdisZSetOps zsetOps,
                YierdisHllOps hllOps,
                YierdisTtlOps ttlOps,
                YierdisDbOwnedResources resources
        ) {
            this.runtime = runtime;
            this.allocator = allocator;
            this.expires = expires;
            this.entries = entries;
            this.keyDirectory = keyDirectory;
            this.keyLifecycle = keyLifecycle;
            this.stringRoot = stringRoot;
            this.listRoot = listRoot;
            this.hashRoot = hashRoot;
            this.setRoot = setRoot;
            this.zsetRoot = zsetRoot;
            this.ledger = ledger;
            this.stringOps = stringOps;
            this.listOps = listOps;
            this.hashOps = hashOps;
            this.setOps = setOps;
            this.zsetOps = zsetOps;
            this.hllOps = hllOps;
            this.ttlOps = ttlOps;
            this.resources = resources;
        }

        private static FaultFixture open() {
            TestBackend runtime = TestBackend.open("mutation-fault");
            FailOnAllocationStableMemoryBackend allocator = new FailOnAllocationStableMemoryBackend(
                    runtime.backend()
            );
            allocator.bindToCurrentThread();
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, HashSeed.random(), null);
            EntryTable entries = new EntryTable(allocator);
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
            YierdisDbOwnedResources resources = new YierdisDbOwnedResources(allocator);
            YierdisStringOps stringOps = new YierdisStringOps(internals);
            YierdisListOps listOps = new YierdisListOps(internals);
            YierdisHashOps hashOps = new YierdisHashOps(internals);
            YierdisSetOps setOps = new YierdisSetOps(internals);
            YierdisZSetOps zsetOps = new YierdisZSetOps(internals);
            YierdisHllOps hllOps = new YierdisHllOps(internals);
            YierdisTtlOps ttlOps = new YierdisTtlOps(internals);
            return new FaultFixture(
                    runtime,
                    allocator,
                    expires,
                    entries,
                    keyDirectory,
                    keyLifecycle,
                    stringRoot,
                    listRoot,
                    hashRoot,
                    setRoot,
                    zsetRoot,
                    ledger,
                    stringOps,
                    listOps,
                    hashOps,
                    setOps,
                    zsetOps,
                    hllOps,
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
                    allocator.stats().objectCount(NativeObjectKind.LIST_ROOT),
                    allocator.stats().objectCount(NativeObjectKind.LIST_NODE),
                    allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES),
                    allocator.stats().objectCount(NativeObjectKind.HASH_ROOT),
                    allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES),
                    allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES),
                    allocator.stats().objectCount(NativeObjectKind.HASH_TABLE),
                    allocator.stats().objectCount(NativeObjectKind.SET_ROOT),
                    allocator.stats().objectCount(NativeObjectKind.SET_MEMBER_BYTES),
                    allocator.stats().objectCount(NativeObjectKind.SET_TABLE),
                    allocator.stats().objectCount(NativeObjectKind.ZSET_ROOT),
                    allocator.stats().objectCount(NativeObjectKind.ZSET_MEMBER_BYTES),
                    allocator.stats().objectCount(NativeObjectKind.SCORE_BYTES),
                    allocator.stats().objectCount(NativeObjectKind.ZSET_TABLE),
                    allocator.stats().objectCount(NativeObjectKind.ZSET_NODE),
                    allocator.stats().objectCount(NativeObjectKind.STRING_BYTES),
                    keyDirectory.size(),
                    expires.size(),
                    primaryStringValue(),
                    primaryListValues(),
                    primaryHashPairs(),
                    primarySetMembers(),
                    primaryZSetMembersAndScores(),
                    primaryHllCount(),
                    expires.get(PRIMARY_KEY),
                    entrySnapshots(),
                    nativeHandleGraph()
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
            Assert.assertEquals(before.listRoots, after.listRoots);
            Assert.assertEquals(before.listNodes, after.listNodes);
            Assert.assertEquals(before.listpackBytes, after.listpackBytes);
            Assert.assertEquals(before.hashRoots, after.hashRoots);
            Assert.assertEquals(before.hashFields, after.hashFields);
            Assert.assertEquals(before.hashValues, after.hashValues);
            Assert.assertEquals(before.hashTables, after.hashTables);
            Assert.assertEquals(before.setRoots, after.setRoots);
            Assert.assertEquals(before.setMembers, after.setMembers);
            Assert.assertEquals(before.setTables, after.setTables);
            Assert.assertEquals(before.zsetRoots, after.zsetRoots);
            Assert.assertEquals(before.zsetMembers, after.zsetMembers);
            Assert.assertEquals(before.scoreBytes, after.scoreBytes);
            Assert.assertEquals(before.zsetTables, after.zsetTables);
            Assert.assertEquals(before.zsetNodes, after.zsetNodes);
            Assert.assertEquals(before.stringBytes, after.stringBytes);
            Assert.assertEquals(before.keyCount, after.keyCount);
            Assert.assertEquals(before.expireCount, after.expireCount);
            Assert.assertArrayEquals(before.primaryValue, after.primaryValue);
            Assert.assertEquals(before.primaryListValues, after.primaryListValues);
            Assert.assertEquals(before.primaryHashPairs, after.primaryHashPairs);
            Assert.assertEquals(before.primarySetMembers, after.primarySetMembers);
            Assert.assertEquals(before.primaryZSetMembersAndScores, after.primaryZSetMembersAndScores);
            Assert.assertEquals(before.primaryHllCount, after.primaryHllCount);
            Assert.assertEquals(before.expireAtMillis, after.expireAtMillis);
            Assert.assertEquals(before.entrySnapshots, after.entrySnapshots);
            Assert.assertEquals(before.nativeHandleGraph, after.nativeHandleGraph);
        }

        private byte[] primaryStringValue() {
            EntryRecord record = keyLifecycle.entryRecord(PRIMARY_KEY);
            if (record == null || record.type() != ValueType.STRING) {
                return null;
            }
            return stringOps.getStringBytes(PRIMARY_KEY);
        }

        private List<String> primaryListValues() {
            EntryRecord record = keyLifecycle.entryRecord(PRIMARY_KEY);
            if (record == null || record.type() != ValueType.LIST) {
                return null;
            }
            return strings(listOps.lrange(PRIMARY_KEY, 0, -1));
        }

        private List<String> primaryHashPairs() {
            EntryRecord record = keyLifecycle.entryRecord(PRIMARY_KEY);
            if (record == null || record.type() != ValueType.HASH) {
                return null;
            }
            return strings(hashOps.hgetall(PRIMARY_KEY));
        }

        private List<String> primarySetMembers() {
            EntryRecord record = keyLifecycle.entryRecord(PRIMARY_KEY);
            if (record == null || record.type() != ValueType.SET) {
                return null;
            }
            List<String> values = strings(setOps.smembers(PRIMARY_KEY));
            values.sort(String::compareTo);
            return values;
        }

        private List<String> primaryZSetMembersAndScores() {
            EntryRecord record = keyLifecycle.entryRecord(PRIMARY_KEY);
            if (record == null || record.type() != ValueType.ZSET) {
                return null;
            }
            return strings(zsetOps.zrange(PRIMARY_KEY, 0, -1, true));
        }

        private Long primaryHllCount() {
            EntryRecord record = keyLifecycle.entryRecord(PRIMARY_KEY);
            if (record == null || record.type() != ValueType.STRING) {
                return null;
            }
            if (!YierdisHyperLogLog.isHllString(stringRoot, record.valueHandle())) {
                return null;
            }
            return hllOps.pfcount(List.of(PRIMARY_KEY));
        }

        private List<String> entrySnapshots() {
            List<String> snapshots = new ArrayList<>();
            keyDirectory.forEachEntry((keyHandle, entryHandle) -> {
                EntryRecord record = entries.get(entryHandle);
                snapshots.add(entrySnapshot(keyHandle, entryHandle, record));
            });
            snapshots.sort(String::compareTo);
            return snapshots;
        }

        private List<String> nativeHandleGraph() {
            List<String> handles = new ArrayList<>();
            YierdisDbNativeHandleGraph.visitReachable(keyLifecycle, (role, handle, record) ->
                    handles.add(role.name()
                            + "|"
                            + handle.allocatorId()
                            + "|"
                            + handle.localRaw())
            );
            handles.sort(String::compareTo);
            return handles;
        }

        private String entrySnapshot(KeyHandle keyHandle, EntryHandle entryHandle, EntryRecord record) {
            String keyIdentity = nativeHandleIdentity(keyHandle);
            if (record == null) {
                return keyIdentity + "|" + nativeHandleIdentity(entryHandle.nativeHandle()) + "|null";
            }
            return keyIdentity
                    + "|"
                    + nativeHandleIdentity(entryHandle.nativeHandle())
                    + "|"
                    + record
                    + "|"
                    + valueSnapshot(record);
        }

        private String valueSnapshot(EntryRecord record) {
            return switch (record.type()) {
                case STRING -> bytesToBase64(stringRoot.copy(record.valueHandle()));
                case LIST -> bytesListSnapshot(listRoot.range(record.valueHandle(), 0, -1));
                case HASH -> bytesPairsSnapshot(hashRoot.hgetallPairs(record.valueHandle()));
                case SET -> bytesSetSnapshot(setRoot.members(record.valueHandle()));
                case ZSET -> bytesListSnapshot(zsetRoot.zrange(record.valueHandle(), 0, -1, true));
            };
        }

        private static String nativeHandleIdentity(KeyHandle keyHandle) {
            NativeHandle nativeHandle = KeyHandleAccess.allocatorNativeHandleOrNull(keyHandle);
            return nativeHandleIdentity(nativeHandle);
        }

        private static String nativeHandleIdentity(NativeHandle nativeHandle) {
            return nativeHandle == null
                    ? "null"
                    : nativeHandle.allocatorId() + ":" + nativeHandle.localRaw();
        }

        private static String bytesToBase64(byte[] bytes) {
            return bytes == null ? "null" : Base64.getEncoder().encodeToString(bytes);
        }

        private static String bytesListSnapshot(List<byte[]> values) {
            List<String> encoded = new ArrayList<>(values.size());
            for (byte[] value : values) {
                encoded.add(bytesToBase64(value));
            }
            return encoded.toString();
        }

        private static String bytesSetSnapshot(List<byte[]> values) {
            List<String> encoded = new ArrayList<>(values.size());
            for (byte[] value : values) {
                encoded.add(bytesToBase64(value));
            }
            encoded.sort(String::compareTo);
            return encoded.toString();
        }

        private static String bytesPairsSnapshot(List<byte[]> values) {
            List<String> encoded = new ArrayList<>(values.size() / 2);
            for (int i = 0; i < values.size(); i += 2) {
                encoded.add(bytesToBase64(values.get(i)) + "=" + bytesToBase64(values.get(i + 1)));
            }
            encoded.sort(String::compareTo);
            return encoded.toString();
        }

        @Override
        public void close() {
            allocator.disableFailures();
            allocator.disableRegionFailures();
            resources.releaseAll(expires, entries, keyDirectory, stringRoot, listRoot, hashRoot, setRoot, zsetRoot);
        }
    }

    private record DbStateSnapshot(
            MemoryUsageSnapshot memoryUsage,
            long runtimeUsedBytes,
            long liveRegions,
            long usedBytes,
            long reservedBytes,
            long liveObjects,
            long listRoots,
            long listNodes,
            long listpackBytes,
            long hashRoots,
            long hashFields,
            long hashValues,
            long hashTables,
            long setRoots,
            long setMembers,
            long setTables,
            long zsetRoots,
            long zsetMembers,
            long scoreBytes,
            long zsetTables,
            long zsetNodes,
            long stringBytes,
            int keyCount,
            int expireCount,
            byte[] primaryValue,
            List<String> primaryListValues,
            List<String> primaryHashPairs,
            List<String> primarySetMembers,
            List<String> primaryZSetMembersAndScores,
            Long primaryHllCount,
            Long expireAtMillis,
            List<String> entrySnapshots,
            List<String> nativeHandleGraph
    ) {
        private DbStateSnapshot {
            primaryValue = primaryValue == null ? null : Arrays.copyOf(primaryValue, primaryValue.length);
            primaryListValues = primaryListValues == null ? null : List.copyOf(primaryListValues);
            primaryHashPairs = primaryHashPairs == null ? null : List.copyOf(primaryHashPairs);
            primarySetMembers = primarySetMembers == null ? null : List.copyOf(primarySetMembers);
            primaryZSetMembersAndScores =
                    primaryZSetMembersAndScores == null ? null : List.copyOf(primaryZSetMembersAndScores);
            entrySnapshots = entrySnapshots == null ? null : List.copyOf(entrySnapshots);
            nativeHandleGraph = nativeHandleGraph == null ? null : List.copyOf(nativeHandleGraph);
        }
    }

    private static List<String> strings(ByteSequenceSource sequence) {
        RecordingByteValueSink sink = new RecordingByteValueSink();
        sequence.emitTo(sink);
        return sink.values;
    }

    private static List<String> strings(ByteMapSource pairs) {
        RecordingByteValueSink sink = new RecordingByteValueSink();
        pairs.emitPairsTo(sink);
        return sink.values;
    }

    private static final class RecordingByteValueSink implements ByteValueSink {
        private final List<String> values = new java.util.ArrayList<>();

        @Override
        public void value(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public void value(byte[] data, int off, int len) {
            values.add(new String(data, off, len, StandardCharsets.UTF_8));
        }

        @Override
        public void value(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            value(data);
        }

        @Override
        public void longAscii(long value) {
            values.add(Long.toString(value));
        }

        @Override
        public void nullValue() {
            value((byte[]) null);
        }
    }

    private static BytesSlice slice(String value) {
        return new ArrayBytesSlice(value.getBytes(StandardCharsets.UTF_8));
    }

    private static BytesSlice view(byte[] value) {
        return new ArrayBytesSlice(Arrays.copyOf(value, value.length));
    }

    private static byte[] repeatedBytes(char value, int count) {
        byte[] bytes = new byte[count];
        Arrays.fill(bytes, (byte) value);
        return bytes;
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
