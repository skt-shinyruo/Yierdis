package yier.bubu.redis.memory.foreign;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public class YierdisNativeObjectTableTest {
    @Test
    public void slotExhaustionUsesCapacityHierarchy() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("slot-capacity");
             YierdisNativeObjectTable table = newTable(runtime, 1, 0)) {
            table.allocate(NativeObjectKind.STRING_BYTES, 1, 1, 1, 1, 0, 0);
            Assert.assertThrows(
                    NativeCapacityExceededException.class,
                    () -> table.allocate(NativeObjectKind.STRING_BYTES, 1, 1, 1, 2, 0, 0)
            );
        }
    }

    @Test
    public void metadataBookkeepingUsesSegmentLocalPrimitiveStructures() {
        Assert.assertEquals(36, YierdisNativeObjectTable.META_BYTES);
        Assert.assertEquals(0, YierdisNativeObjectTable.ADDRESS_OFFSET);
        Assert.assertEquals(4, YierdisNativeObjectTable.SIZE_OFFSET);
        Assert.assertEquals(8, YierdisNativeObjectTable.SEGMENT_ID_OFFSET);
        Assert.assertEquals(12, YierdisNativeObjectTable.PACKED_METADATA_OFFSET);
        Assert.assertEquals(16, YierdisNativeObjectTable.ALLOC_EPOCH_OFFSET);
        Assert.assertEquals(24, YierdisNativeObjectTable.FREE_EPOCH_OFFSET);
        Assert.assertEquals(32, YierdisNativeObjectTable.PIN_COUNT_OFFSET);
        Assert.assertThrows(
                NoSuchFieldException.class,
                () -> YierdisNativeObjectTable.class.getDeclaredField("CAPACITY_OFFSET")
        );
        Assert.assertFalse(Arrays.stream(YierdisNativeObjectTable.class.getDeclaredFields())
                .map(Field::getType)
                .anyMatch(ArrayDeque.class::equals));
        Assert.assertFalse(Arrays.stream(YierdisNativeObjectTable.class.getDeclaredFields())
                .map(Field::getType)
                .anyMatch(boolean[].class::equals));
        Assert.assertTrue(Arrays.stream(YierdisNativeObjectSegment.class.getDeclaredFields())
                .map(Field::getType)
                .anyMatch(int[].class::equals));
        Assert.assertTrue(Arrays.stream(YierdisNativeObjectSegment.class.getDeclaredFields())
                .map(Field::getType)
                .anyMatch(long[].class::equals));
    }

    @Test
    public void emptyTableCommitsNoMetadataRegardlessOfMaximumSlots() {
        for (int maxSlots : new int[]{0, 1, 4_096, 262_144}) {
            try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("lazy-table-" + maxSlots);
                 YierdisNativeObjectTable table = newTable(runtime, maxSlots, 0)) {
                Assert.assertEquals(0L, runtime.usedBytes());
                Assert.assertEquals(0L, table.stats().metadataCommittedBytes());
                Assert.assertEquals(0, table.stats().activeSegments());
            }
        }
    }

    @Test
    public void automaticCapacityGrowsMetadataSegmentsLazily() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("automatic-segment-growth");
             YierdisNativeObjectTable table = newTable(runtime, 0, 0)) {
            for (int i = 0; i < 4_097; i++) {
                table.allocate(NativeObjectKind.STRING_BYTES, 1, 1, 1, i + 1L, 0, 0);
            }

            Assert.assertEquals(2, table.stats().activeSegments());
            Assert.assertEquals(4_097L, table.stats().liveSlots());
        }
    }

    @Test
    public void automaticCapacityArithmeticHandlesMaximumIntegerSlot() {
        int finalSegment = Integer.MAX_VALUE / YierdisNativeObjectSegment.SLOTS_PER_SEGMENT;
        int activeSegments = finalSegment + 1;

        Assert.assertEquals(
                Integer.MAX_VALUE,
                YierdisNativeObjectTable.materializedSlotUpperBound(Integer.MAX_VALUE, activeSegments)
        );
        Assert.assertEquals(
                YierdisNativeObjectSegment.SLOTS_PER_SEGMENT - 1,
                YierdisNativeObjectTable.validSlotsInSegment(Integer.MAX_VALUE, finalSegment)
        );
        Assert.assertEquals(
                Integer.MAX_VALUE,
                YierdisNativeObjectTable.slotId(
                        finalSegment,
                        YierdisNativeObjectSegment.SLOTS_PER_SEGMENT - 2
                )
        );
    }

    @Test
    public void negativeCapacityIsRejected() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("negative-capacity")) {
            Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> newTable(runtime, -1, 0)
            );
        }
    }

    @Test
    public void allocationCrossing4096SlotsCommitsExactlyTwoSegments() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("segment-boundary");
             YierdisNativeObjectTable table = newTable(runtime, 4_097, 0)) {
            List<NativeHandle> handles = new ArrayList<>();
            for (int i = 0; i < 4_097; i++) {
                handles.add(table.allocate(NativeObjectKind.STRING_BYTES, 1, 1, 1, i + 1L, 0, 0));
            }

            Assert.assertEquals(2, table.stats().activeSegments());
            Assert.assertEquals(4_097L, table.stats().liveSlots());
            Assert.assertEquals(2L * 4_096L * YierdisNativeObjectTable.META_BYTES,
                    table.stats().metadataCommittedBytes());
        }
    }

    @Test
    public void allocatesGenerationBearingHandlesAndStoresMetadata() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-basic");
             YierdisNativeObjectTable table = newTable(runtime, 4, 77)) {

            NativeHandle handle = table.allocate(NativeObjectKind.STRING_BYTES, 32, 48, 55, 1234L, 2, 9L);
            YierdisNativeObjectMeta meta = table.resolve(handle);

            Assert.assertEquals(NativeObjectKind.STRING_BYTES.domain(), handle.domain());
            Assert.assertEquals(NativeObjectKind.STRING_BYTES.code(), handle.kindCode());
            Assert.assertEquals(1L, handle.slotId());
            Assert.assertEquals(1, handle.generation());
            Assert.assertEquals(32, meta.size());
            Assert.assertEquals(48, meta.capacity());
            Assert.assertEquals(1234L, meta.address());
            Assert.assertEquals(55, meta.segmentId());
            Assert.assertEquals(2, meta.pageClass());
            Assert.assertEquals(77, meta.ownerShardId());
            Assert.assertEquals(9L, meta.allocEpoch());
            Assert.assertEquals(YierdisNativeObjectTable.STATE_ALLOCATED, meta.state());
        }
    }

    @Test
    public void occupiedSlotCursorDoesNotWrapAfterMaximumInteger() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-cursor-wrap");
             YierdisNativeObjectTable table = newTable(runtime, 1, 0)) {
            table.allocate(NativeObjectKind.STRING_BYTES, 1, 1, 1, 0L, 0, 1L);

            Assert.assertEquals(0, table.nextOccupiedSlot(Integer.MAX_VALUE));
        }
    }

    @Test
    public void freeIncrementsGenerationBeforeReuse() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-reuse");
             YierdisNativeObjectTable table = newTable(runtime, 1, 7)) {

            NativeHandle first = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 1, 11L, 1, 1L);
            table.free(first, 2L);

            try {
                table.resolve(first);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            NativeHandle second = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 2, 22L, 1, 3L);
            Assert.assertEquals(first.slotId(), second.slotId());
            Assert.assertEquals(first.generation() + 1, second.generation());
        }
    }

    @Test
    public void generationWrapRetiresSlot() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-wrap");
             YierdisNativeObjectTable table = newTable(runtime, 1, 7)) {

            for (int generation = 1; generation <= 0x0fff; generation++) {
                NativeHandle handle = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 1, generation, 1, generation);
                Assert.assertEquals(generation, handle.generation());
                table.free(handle, generation);
            }

            try {
                table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 1, 1L, 1, 1L);
                Assert.fail("expected retired slot exhaustion");
            } catch (NativeCapacityExceededException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }
            Assert.assertEquals(0L, table.stats().liveSlots());
            Assert.assertEquals(0L, table.stats().freeSlots());
            Assert.assertEquals(1L, table.stats().retiredSlots());
            Assert.assertEquals(1L, table.stats().peakLiveSlots());
        }
    }

    @Test
    public void statsTrackLiveFreeAndPerStateTransitions() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-stats");
             YierdisNativeObjectTable table = newTable(runtime, 2, 7)) {
            NativeHandle first = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 1, 11L, 1, 1L);
            NativeHandle second = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 2, 22L, 1, 1L);
            table.pin(first);
            table.free(first, 2L);

            YierdisNativeObjectTableStats quarantined = table.stats();
            Assert.assertEquals(2L, quarantined.liveSlots());
            Assert.assertEquals(0L, quarantined.freeSlots());
            Assert.assertEquals(2L, quarantined.peakLiveSlots());
            Assert.assertEquals(1L, quarantined.stateCount(YierdisNativeObjectTable.STATE_ALLOCATED));
            Assert.assertEquals(1L, quarantined.stateCount(YierdisNativeObjectTable.STATE_FREED_QUARANTINED));

            table.unpin(first);
            table.free(second, 3L);

            YierdisNativeObjectTableStats released = table.stats();
            Assert.assertEquals(0L, released.liveSlots());
            Assert.assertEquals(2L, released.freeSlots());
            Assert.assertEquals(2L, released.stateCount(YierdisNativeObjectTable.STATE_FREE));
        }
    }

    @Test
    public void pinnedFreeQuarantinesUntilUnpin() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-quarantine");
             YierdisNativeObjectTable table = newTable(runtime, 1, 7)) {

            NativeHandle handle = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 1, 11L, 1, 1L);
            table.pin(handle);
            table.free(handle, 2L);

            YierdisNativeObjectMeta quarantined = table.snapshot(handle, true);
            Assert.assertEquals(YierdisNativeObjectTable.STATE_FREED_QUARANTINED, quarantined.state());
            Assert.assertEquals(1, quarantined.pinCount());
            Assert.assertEquals(2L, quarantined.freeEpoch());

            try {
                table.free(handle, 3L);
                Assert.fail("expected quarantined double-free stale rejection");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("quarantined"));
            }

            try {
                table.resolve(handle);
                Assert.fail("expected quarantined handle rejection");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("quarantined"));
            }

            try {
                table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 2, 22L, 1, 3L);
                Assert.fail("expected slot limit while quarantined");
            } catch (NativeCapacityExceededException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }

            table.unpin(handle);

            NativeHandle second = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 2, 22L, 1, 3L);
            Assert.assertEquals(handle.slotId(), second.slotId());
            Assert.assertEquals(handle.generation() + 1, second.generation());
        }
    }

    @Test
    public void rejectsWrongKindAndDomainAndDoubleFree() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-invalid");
             YierdisNativeObjectTable table = newTable(runtime, 2, 7)) {

            NativeHandle handle = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 1, 11L, 1, 1L);
            NativeHandle wrongKind = NativeHandle.of(handle.domain(), NativeObjectKind.GENERIC, handle.slotId(), handle.generation(), 0);
            NativeHandle wrongDomain = NativeHandle.of(NativeObjectKind.ENTRY_RECORD.domain(), NativeObjectKind.ENTRY_RECORD, handle.slotId(), handle.generation(), 0);

            try {
                table.resolve(wrongKind);
                Assert.fail("expected wrong-kind rejection");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("kind/domain"));
            }

            try {
                table.resolve(wrongDomain);
                Assert.fail("expected wrong-domain rejection");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("kind/domain"));
            }

            table.free(handle, 2L);

            try {
                table.free(handle, 3L);
                Assert.fail("expected double-free stale rejection");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }
        }
    }

    @Test
    public void metadataIsBackedByNativeRuntimeMemory() {
        YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-native");
        YierdisNativeObjectTable table = newTable(runtime, 4, 7);
        try {
            table.allocate(NativeObjectKind.STRING_BYTES, 1, 1, 1, 1L, 0, 0L);
            Assert.assertEquals(4_096L * YierdisNativeObjectTable.META_BYTES, runtime.usedBytes());
        } finally {
            table.close();
            runtime.close();
        }
    }

    @Test
    public void declaredCapacityMustMatchCapacityResolver() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-capacity-mismatch");
             YierdisNativeObjectTable table = newTable(runtime, 1, 0)) {
            Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> table.allocate(NativeObjectKind.STRING_BYTES, 1, 16, 1, 0L, 0, 0L)
            );
            Assert.assertEquals(0L, table.stats().metadataCommittedBytes());
        }
    }

    @Test
    public void locationMutationsValidateCapacityBeforePublishingMetadata() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-location-capacity");
             YierdisNativeObjectTable table = newTable(runtime, 1, 0)) {
            NativeHandle handle = table.allocate(NativeObjectKind.STRING_BYTES, 8, 16, 1, 0L, 1, 1L);
            YierdisNativeObjectMeta original = table.resolve(handle);

            Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> table.updateLocation(handle, 8, 15, 1, 0L, 1)
            );
            Assert.assertEquals(original, table.resolve(handle));

            table.beginMove(handle);
            Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> table.publishMoved(handle, 8, 47, 2, 0L, 2)
            );
            table.abortMove(handle);
            Assert.assertEquals(original, table.resolve(handle));
        }
    }

    private static YierdisNativeObjectTable newTable(
            YierdisFfmMemoryRuntime runtime,
            int maxSlots,
            int ownerShardId
    ) {
        return new YierdisNativeObjectTable(
                runtime,
                maxSlots,
                ownerShardId,
                (pageId, pageOffset, pageClass) -> switch (pageClass) {
                    case 0 -> 1;
                    case 1 -> 16;
                    case 2 -> 48;
                    default -> throw new IllegalStateException("unknown test page class: " + pageClass);
                }
        );
    }
}
