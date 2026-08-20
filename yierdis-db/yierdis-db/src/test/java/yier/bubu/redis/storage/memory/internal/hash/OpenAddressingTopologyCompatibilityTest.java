package yier.bubu.redis.storage.memory.internal.hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.hash.OpenAddressingTopology.Location;
import yier.bubu.redis.storage.memory.internal.hash.OpenAddressingTopology.SlotState;
import yier.bubu.redis.storage.memory.internal.hash.OpenAddressingTopology.TableSide;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.value.NativeByteMap;
import yier.bubu.redis.storage.memory.internal.value.NativeByteStore;

public class OpenAddressingTopologyCompatibilityTest {
    private static final HashSeed FIXED_SEED = new HashSeed(0x0123456789abcdefL, 0xfedcba9876543210L);

    @Test
    public void representsProbeTombstoneAndIncrementalRehashTransitions() {
        OpenAddressingTopology topology = new OpenAddressingTopology(16);

        OpenAddressingTopology.ProbeResult first = topology.probe(1, ignored -> false);
        Assert.assertFalse(first.found());
        Assert.assertEquals(new Location(TableSide.ACTIVE, 1), first.location());
        topology.occupyActive(first.location().slot(), 1);

        OpenAddressingTopology.ProbeResult second = topology.probe(1, ignored -> false);
        Assert.assertEquals(new Location(TableSide.ACTIVE, 2), second.location());
        Assert.assertEquals(2, second.probes());
        topology.occupyActive(second.location().slot(), 1);

        topology.remove(new Location(TableSide.ACTIVE, 1));
        Assert.assertEquals(SlotState.TOMBSTONE, topology.slotState(TableSide.ACTIVE, 1));
        OpenAddressingTopology.ProbeResult replacement = topology.probe(1, ignored -> false);
        Assert.assertEquals(new Location(TableSide.ACTIVE, 1), replacement.location());
        topology.occupyActive(replacement.location().slot(), 1);

        OpenAddressingTopology stagedActive = new OpenAddressingTopology(32);
        topology.beginRehash(stagedActive);
        AtomicInteger movedFrom = new AtomicInteger(-1);
        AtomicInteger movedTo = new AtomicInteger(-1);
        HashTableWorkResult step = topology.advanceRehash(
                HashTableWorkBudget.of(2L, Long.MAX_VALUE),
                (oldSlot, activeSlot) -> {
                    movedFrom.set(oldSlot);
                    movedTo.set(activeSlot);
                }
        );

        Assert.assertEquals(2L, step.inspectedSlots());
        Assert.assertEquals(1L, step.migratedSlots());
        Assert.assertEquals(HashTableWorkResult.StopReason.SLOT_LIMIT, step.stopReason());
        Assert.assertEquals(1, movedFrom.get());
        Assert.assertEquals(1, movedTo.get());
        Assert.assertEquals(SlotState.MIGRATED_SCAN_SHADOW, topology.slotState(TableSide.OLD, 1));
        Assert.assertEquals(SlotState.FILLED, topology.slotState(TableSide.ACTIVE, 1));
    }

    @Test
    public void resetPublishesAPreallocatedEmptyTopology() {
        OpenAddressingTopology topology = new OpenAddressingTopology(16);
        topology.occupyActive(1, 1);
        topology.beginRehash(new OpenAddressingTopology(32));
        long generation = topology.metrics().generation();

        topology.reset(new OpenAddressingTopology(16));

        Assert.assertEquals(16, topology.metrics().capacity());
        Assert.assertEquals(0, topology.metrics().size());
        Assert.assertFalse(topology.metrics().rehashing());
        Assert.assertEquals(generation + 1L, topology.metrics().generation());
        Assert.assertEquals(0L, topology.metrics().completedRehashes());
        Assert.assertEquals(SlotState.EMPTY, topology.slotState(TableSide.ACTIVE, 1));
    }

    @Test
    public void matchesBothNativeContainersAcrossCollisionsTombstonesAndIncrementalRehash() {
        try (TestBackend runtime = TestBackend.open("topology-compatibility");
             StableMemoryBackend backend = runtime.backend()) {
            NativeByteStore mapKeyStore = new NativeByteStore(backend, NativeObjectKind.SET_MEMBER_BYTES);
            try (NativeByteMap<Integer> map = new NativeByteMap<>(
                    mapKeyStore,
                    NativeObjectKind.SET_MEMBER_BYTES,
                    FIXED_SEED,
                    new HashTableMaintenanceRegistry()
            ); NativeKeyDirectory directory = new NativeKeyDirectory(
                    backend,
                    FIXED_SEED,
                    new HashTableMaintenanceRegistry()
            )) {
                TopologyFixture topology = new TopologyFixture();
                List<byte[]> keys = collidingKeys(13, 1);
                List<byte[]> presentKeys = new ArrayList<>(keys.subList(0, 12));
                List<NativeHandle> entries = new ArrayList<>();
                try {
                    for (int index = 0; index < 12; index++) {
                        put(topology, map, directory, backend, entries, keys.get(index), index);
                    }
                    assertCompatible(topology, map, mapKeyStore, directory, presentKeys);

                    Assert.assertEquals(Integer.valueOf(0), map.remove(keys.get(0)));
                    Assert.assertNotNull(directory.remove(keys.get(0)));
                    topology.remove(string(keys.get(0)), hash(keys.get(0)));
                    presentKeys.removeFirst();
                    Assert.assertNull(topology.get(hash(keys.get(0)), string(keys.get(0))));
                    Assert.assertNull(map.get(keys.get(0)));
                    Assert.assertNull(directory.get(keys.get(0)));
                    Assert.assertEquals(1, topology.metrics().tombstones());
                    assertCompatible(topology, map, mapKeyStore, directory, presentKeys);

                    byte[] replacement = collidingKeys(1, 1, "replacement-").getFirst();
                    put(topology, map, directory, backend, entries, replacement, 12);
                    presentKeys.add(replacement);
                    Assert.assertEquals(0, topology.metrics().tombstones());
                    assertCompatible(topology, map, mapKeyStore, directory, presentKeys);

                    put(topology, map, directory, backend, entries, keys.get(12), 13);
                    presentKeys.add(keys.get(12));
                    Assert.assertTrue(topology.metrics().rehashing());
                    assertCompatible(topology, map, mapKeyStore, directory, presentKeys);

                    HashTableWorkBudget twoSlots = HashTableWorkBudget.of(2L, Long.MAX_VALUE);
                    Assert.assertEquals(topology.advanceRehash(twoSlots), map.advanceRehash(twoSlots));
                    Assert.assertEquals(topology.lastWorkResult(), directory.advanceRehash(twoSlots));
                    Assert.assertEquals(SlotState.MIGRATED_SCAN_SHADOW, topology.slotState(TableSide.OLD, 1));

                    Assert.assertEquals(Integer.valueOf(12), map.remove(replacement));
                    Assert.assertNotNull(directory.remove(replacement));
                    topology.remove(string(replacement), hash(replacement));
                    presentKeys.remove(replacement);
                    Assert.assertEquals(SlotState.TOMBSTONE, topology.slotState(TableSide.OLD, 1));
                    Assert.assertNull(topology.get(hash(replacement), string(replacement)));
                    Assert.assertNull(map.get(replacement));
                    Assert.assertNull(directory.get(replacement));
                    assertCompatible(topology, map, mapKeyStore, directory, presentKeys);

                    while (topology.metrics().rehashing()) {
                        Assert.assertEquals(topology.advanceRehash(twoSlots), map.advanceRehash(twoSlots));
                        Assert.assertEquals(topology.lastWorkResult(), directory.advanceRehash(twoSlots));
                        assertCompatible(topology, map, mapKeyStore, directory, presentKeys);
                    }
                    Assert.assertEquals(1L, topology.metrics().completedRehashes());
                } finally {
                    for (NativeHandle entry : entries) {
                        backend.free(entry);
                    }
                }
            }
        }
    }

    private static void put(
            TopologyFixture topology,
            NativeByteMap<Integer> map,
            NativeKeyDirectory directory,
            StableMemoryBackend backend,
            List<NativeHandle> entries,
            byte[] key,
            int value
    ) {
        Assert.assertNull(map.put(key, value));
        NativeHandle nativeEntry = backend.allocate(NativeObjectKind.ENTRY_RECORD, 1);
        entries.add(nativeEntry);
        try (NativeKeyDirectory.StagedInsert staged = directory.stageInsert(key)) {
            directory.publishStagedInsert(staged, new EntryHandle(nativeEntry));
        }
        topology.put(string(key), hash(key));
    }

    private static void assertCompatible(
            TopologyFixture topology,
            NativeByteMap<Integer> map,
            NativeByteStore mapKeyStore,
            NativeKeyDirectory directory,
            List<byte[]> presentKeys
    ) {
        Assert.assertEquals(topology.metrics(), map.metrics());
        Assert.assertEquals(topology.metrics(), directory.metrics());
        Set<String> expected = new HashSet<>();
        for (byte[] key : presentKeys) {
            String expectedKey = topology.get(hash(key), string(key));
            Assert.assertEquals(string(key), expectedKey);
            expected.add(string(key));
            Assert.assertNotNull(map.get(key));
            Assert.assertNotNull(directory.get(key));
        }
        Assert.assertEquals(expected, topology.visibleKeys());
        Assert.assertEquals(expected, scanKeys(map, mapKeyStore));
        Assert.assertEquals(expected, scanKeys(directory));
    }

    private static Set<String> scanKeys(NativeByteMap<Integer> map, NativeByteStore keyStore) {
        Set<String> seen = new HashSet<>();
        NativeByteMap.ScanResult result = map.scanWithWork(
                ScanCursorV2.start(), Long.MAX_VALUE, (keyHandle, ignored) -> {
                    seen.add(string(keyStore.toByteArray(keyHandle)));
                    return true;
                }
        );
        Assert.assertEquals(0L, result.nextCursor().value());
        return seen;
    }

    private static Set<String> scanKeys(NativeKeyDirectory directory) {
        Set<String> seen = new HashSet<>();
        NativeKeyDirectory.ScanResult result = directory.scanWithWork(
                ScanCursorV2.start(), Long.MAX_VALUE, (keyHandle, ignored) -> {
                    seen.add(string(copy(keyHandle)));
                    return true;
                }
        );
        Assert.assertEquals(0L, result.nextCursor().value());
        return seen;
    }

    private static byte[] copy(AllocatorKeyHandle key) {
        byte[] bytes = new byte[key.length()];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = key.getByte(index);
        }
        return bytes;
    }

    private static List<byte[]> collidingKeys(int count, int bucket) {
        return collidingKeys(count, bucket, "collision-");
    }

    private static List<byte[]> collidingKeys(int count, int bucket, String prefix) {
        List<byte[]> keys = new ArrayList<>(count);
        for (int suffix = 0; keys.size() < count; suffix++) {
            byte[] key = (prefix + suffix).getBytes(StandardCharsets.US_ASCII);
            if ((hash(key) & 15) == bucket) {
                keys.add(key);
            }
        }
        return keys;
    }

    private static int hash(byte[] key) {
        return SipHash24.foldToInt(SipHash24.hash(FIXED_SEED, key));
    }

    private static String string(byte[] key) {
        return new String(key, StandardCharsets.US_ASCII);
    }

    private static final class TopologyFixture {
        private static final HashTableWorkBudget WRITE_REHASH_BUDGET =
                HashTableWorkBudget.of(2L, Long.MAX_VALUE);

        private final OpenAddressingTopology topology = new OpenAddressingTopology(16);
        private String[] active = new String[16];
        private String[] old;
        private HashTableWorkResult lastWorkResult;

        private void put(String key, int hash) {
            advanceOnWrite();
            OpenAddressingTopology.ProbeResult probe = probe(hash, key);
            if (probe.found()) {
                return;
            }
            HashTableMetrics metrics = topology.metrics();
            SlotState previous = topology.slotState(TableSide.ACTIVE, probe.location().slot());
            HashCapacityPolicy.Decision decision = HashCapacityPolicy.nextAction(
                    metrics.capacity(),
                    metrics.size() + 1,
                    metrics.filledSlots() + (previous == SlotState.EMPTY ? 1 : 0),
                    metrics.tombstones() - (previous == SlotState.TOMBSTONE ? 1 : 0)
            );
            if (decision.action() != HashCapacityPolicy.Action.NONE) {
                old = active;
                active = new String[decision.targetCapacity()];
                topology.beginRehash(decision.targetCapacity());
                probe = probe(hash, key);
            }
            active[probe.location().slot()] = key;
            topology.occupyActive(probe.location().slot(), hash);
        }

        private void remove(String key, int hash) {
            advanceOnWrite();
            OpenAddressingTopology.ProbeResult probe = probe(hash, key);
            Assert.assertTrue(probe.found());
            Location location = probe.location();
            if (location.table() == TableSide.ACTIVE) {
                int oldShadow = topology.invalidateOldShadow(hash, candidate -> key.equals(valueAt(candidate)));
                if (oldShadow >= 0) {
                    old[oldShadow] = null;
                }
                active[location.slot()] = null;
            } else {
                old[location.slot()] = null;
            }
            topology.remove(location);
        }

        private String get(int hash, String key) {
            OpenAddressingTopology.ProbeResult result = probe(hash, key);
            return result.found() ? valueAt(result.location()) : null;
        }

        private HashTableWorkResult advanceRehash(HashTableWorkBudget budget) {
            lastWorkResult = topology.advanceRehash(budget, (oldSlot, activeSlot) -> active[activeSlot] = old[oldSlot]);
            if (lastWorkResult.rehashComplete()) {
                old = null;
            }
            return lastWorkResult;
        }

        private HashTableWorkResult lastWorkResult() {
            return lastWorkResult;
        }

        private HashTableMetrics metrics() {
            return topology.metrics();
        }

        private SlotState slotState(TableSide side, int slot) {
            return topology.slotState(side, slot);
        }

        private Set<String> visibleKeys() {
            Set<String> visible = new HashSet<>();
            addVisible(active, TableSide.ACTIVE, visible);
            if (old != null) {
                addVisible(old, TableSide.OLD, visible);
            }
            return visible;
        }

        private void addVisible(String[] values, TableSide side, Set<String> visible) {
            for (int slot = 0; slot < values.length; slot++) {
                SlotState state = topology.slotState(side, slot);
                if (state == SlotState.FILLED || state == SlotState.MIGRATED_SCAN_SHADOW) {
                    visible.add(values[slot]);
                }
            }
        }

        private OpenAddressingTopology.ProbeResult probe(int hash, String key) {
            return topology.probe(hash, location -> key.equals(valueAt(location)));
        }

        private String valueAt(Location location) {
            return (location.table() == TableSide.ACTIVE ? active : old)[location.slot()];
        }

        private void advanceOnWrite() {
            if (topology.metrics().rehashing()) {
                advanceRehash(WRITE_REHASH_BUDGET);
            }
        }
    }
}
