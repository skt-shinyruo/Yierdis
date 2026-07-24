package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceResult;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.value.HashValue;
import yier.bubu.redis.storage.memory.internal.value.SetValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.ZSetValue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

public class HashTableMillionOperationChurnTest {
    private static final int OPERATION_COUNT = 1_000_000;
    private static final int WORKING_SET_SIZE = 4_096;
    private static final HashSeed FIXED_SEED = new HashSeed(0x1020304050607080L, 0x8877665544332211L);

    @Test
    public void boundedTablesConvergeAfterOneMillionFixedSeedOperations() {
        HashTableMaintenanceRegistry registry = new HashTableMaintenanceRegistry();
        try (TestBackend runtime = TestBackend.open("hash-table-million-operation-churn");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED, registry);
             HashValue hash = new HashValue(allocator, FIXED_SEED, registry);
             SetValue set = new SetValue(allocator, FIXED_SEED, registry);
             ZSetValue zset = new ZSetValue(allocator, FIXED_SEED, registry)) {
            long baselineLiveObjects = allocator.stats().liveObjects();
            byte[][] keys = new byte[WORKING_SET_SIZE][];
            byte[][] members = new byte[WORKING_SET_SIZE][];
            byte[][] values = new byte[WORKING_SET_SIZE][];
            byte[][] scores = new byte[WORKING_SET_SIZE][];
            @SuppressWarnings("unchecked")
            List<byte[]>[] singletonMembers = new List[WORKING_SET_SIZE];
            @SuppressWarnings("unchecked")
            List<byte[]>[] hashPairs = new List[WORKING_SET_SIZE];
            @SuppressWarnings("unchecked")
            List<byte[]>[] zsetPairs = new List[WORKING_SET_SIZE];
            NativeHandle[] directoryEntries = new NativeHandle[WORKING_SET_SIZE];
            for (int i = 0; i < WORKING_SET_SIZE; i++) {
                keys[i] = bytes("churn-key-" + i);
                members[i] = bytes("churn-member-" + i);
                values[i] = bytes("churn-value-" + i);
                scores[i] = bytes(Integer.toString(i));
                singletonMembers[i] = List.of(members[i]);
                hashPairs[i] = List.of(members[i], values[i]);
                zsetPairs[i] = List.of(scores[i], members[i]);
            }

            try {
                Random random = new Random(0x5eed_1234_abcdL);
                for (int operation = 0; operation < OPERATION_COUNT; operation++) {
                    int index = random.nextInt(WORKING_SET_SIZE);
                    switch (random.nextInt(12)) {
                        case 0, 1 -> upsertDirectory(directory, allocator, keys[index], directoryEntries, index);
                        case 2 -> deleteDirectory(directory, allocator, keys[index], directoryEntries, index);
                        case 3, 4 -> hash.hset(members[index], values[index]);
                        case 5 -> hash.hdel(singletonMembers[index]);
                        case 6, 7 -> set.addAll(singletonMembers[index]);
                        case 8 -> set.removeAll(singletonMembers[index]);
                        case 9, 10 -> zset.zaddMany(zsetPairs[index]);
                        case 11 -> zset.zrem(singletonMembers[index]);
                        default -> throw new AssertionError("unexpected operation");
                    }
                    if ((operation + 1) % 256 == 0) {
                        advanceMaintenance(registry);
                    }
                }

                Assert.assertEquals(ValueEncoding.HASH_HT, hash.encoding());
                Assert.assertEquals(ValueEncoding.SET_HT, set.encoding());
                Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, zset.encoding());

                for (int i = 0; i < WORKING_SET_SIZE; i++) {
                    deleteDirectory(directory, allocator, keys[i], directoryEntries, i);
                    hash.hdel(singletonMembers[i]);
                    set.removeAll(singletonMembers[i]);
                    zset.zrem(singletonMembers[i]);
                }
                drainMaintenance(registry);

                assertEmptyTable(directory.metrics());
                assertEmptyTable(hash.memberTableMetrics());
                assertEmptyTable(set.memberTableMetrics());
                assertEmptyTable(zset.memberTableMetrics());
                Assert.assertFalse(directory.hasMaintenanceDebt());
                Assert.assertFalse(hash.hasMemberTableMaintenanceDebt());
                Assert.assertFalse(set.hasMemberTableMaintenanceDebt());
                Assert.assertFalse(zset.hasMemberTableMaintenanceDebt());
                Assert.assertEquals(0, registry.pendingTableCount());
                Assert.assertEquals(baselineLiveObjects, allocator.stats().liveObjects());
            } finally {
                for (int i = 0; i < WORKING_SET_SIZE; i++) {
                    deleteDirectory(directory, allocator, keys[i], directoryEntries, i);
                }
            }
        }
    }

    private static void upsertDirectory(
            NativeKeyDirectory directory,
            StableMemoryBackend allocator,
            byte[] key,
            NativeHandle[] entries,
            int index
    ) {
        NativeHandle existing = entries[index];
        if (existing != null) {
            EntryHandle expected = new EntryHandle(existing);
            Assert.assertEquals(expected, directory.compute(key, (ignored, previous) -> previous));
            return;
        }

        NativeHandle allocated = allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32);
        boolean inserted = false;
        try {
            EntryHandle next = new EntryHandle(allocated);
            Assert.assertEquals(next, directory.compute(key, (ignored, previous) -> next));
            entries[index] = allocated;
            inserted = true;
        } finally {
            if (!inserted) {
                allocator.free(allocated);
            }
        }
    }

    private static void deleteDirectory(
            NativeKeyDirectory directory,
            StableMemoryBackend allocator,
            byte[] key,
            NativeHandle[] entries,
            int index
    ) {
        NativeHandle existing = entries[index];
        if (existing == null) {
            return;
        }
        Assert.assertEquals(new EntryHandle(existing), directory.remove(key));
        allocator.free(existing);
        entries[index] = null;
    }

    private static void advanceMaintenance(HashTableMaintenanceRegistry registry) {
        registry.advance(HashTableWorkBudget.of(64L, Long.MAX_VALUE), participant -> {
            HashTableMaintenanceRegistry.MaintenancePreparation preparation = participant.prepareMaintenance();
            if (preparation == null) {
                return HashTableMaintenanceRegistry.PreparationResult.NO_CHANGE;
            }
            boolean committed = false;
            try {
                preparation.commit();
                committed = true;
                return HashTableMaintenanceRegistry.PreparationResult.STARTED;
            } finally {
                if (!committed) {
                    preparation.abort();
                }
            }
        });
    }

    private static void drainMaintenance(HashTableMaintenanceRegistry registry) {
        for (int i = 0; registry.pendingTableCount() != 0 && i < 100_000; i++) {
            advanceMaintenance(registry);
        }
        Assert.assertEquals(0, registry.pendingTableCount());
    }

    private static void assertEmptyTable(HashTableMetrics metrics) {
        Assert.assertNotNull(metrics);
        Assert.assertEquals(16, metrics.capacity());
        Assert.assertEquals(0, metrics.size());
        Assert.assertEquals(0, metrics.filledSlots());
        Assert.assertEquals(0, metrics.tombstones());
        Assert.assertFalse(metrics.rehashing());
        Assert.assertEquals(0, metrics.oldCapacity());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
