package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HashValueTest {
    @Test
    public void stagedPackedBuildPlansOneFinalBlockAcrossAllocatorSizeClasses() {
        ArrayList<byte[]> pairs = new ArrayList<>();
        for (int index = 0; index < YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES; index++) {
            pairs.add(fixedBytes('f', index, YierdisEncodingThresholds.HASH_MAX_LISTPACK_VALUE_BYTES));
            pairs.add(fixedBytes('v', index, YierdisEncodingThresholds.HASH_MAX_LISTPACK_VALUE_BYTES));
        }
        int encodedBytes = NativeListpack.encodedBytesOf(pairs);
        Assert.assertTrue(encodedBytes > 64 * 1024);
        Assert.assertArrayEquals(new int[]{encodedBytes}, HashValue.preparedBuildNativeAllocationSizes(pairs));

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-packed-final-block");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            HashValue hash = new HashValue(allocator);
            try {
                hash.loadForBuild(pairs);
                Assert.assertEquals(ValueEncoding.HASH_PACKED, hash.encoding());
                Assert.assertEquals(pairs.size() / 2, hash.size());
                Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
                Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
                Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));
            } finally {
                hash.close();
            }
        }
    }

    @Test
    public void hashtableEncodingStoresValueHandlesInPrimitiveArray() throws ReflectiveOperationException {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-primitive-value-handles");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            HashValue hash = new HashValue(allocator);
            try {
                byte[] field = bytes("field");
                byte[] value = new byte[YierdisEncodingThresholds.HASH_MAX_LISTPACK_VALUE_BYTES + 1];
                long heapUpperBound = HashValue.preparedNewHeapUpperBound(List.of(field, value));

                Assert.assertEquals(1, hash.hset(field, value));
                Assert.assertEquals(ValueEncoding.HASH_HT, hash.encoding());
                Assert.assertTrue(nativeMapValueSlots(hash, "map") instanceof long[]);
                Assert.assertTrue(hash.heapEstimatedBytes() <= heapUpperBound);
                Assert.assertArrayEquals(value, hash.hget(field));
            } finally {
                hash.close();
            }
        }
    }

    @Test
    public void hashTableDeltaNoopDoesNotAllocateNativeObjects() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-table-delta-noop");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            HashValue hash = new HashValue(allocator);
            try {
                byte[] field = bytes("field");
                byte[] value = fixedBytes(
                        'v',
                        1,
                        YierdisEncodingThresholds.HASH_MAX_LISTPACK_VALUE_BYTES + 1
                );
                Assert.assertEquals(1, hash.hset(field, value));
                Assert.assertEquals(ValueEncoding.HASH_HT, hash.encoding());

                long fieldObjects = allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES);
                long valueObjects = allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES);
                Set<Long> handles = nativeHandles(hash);

                HashValue.HashTableSetPlan plan = hash.planHashTableSet(List.of(
                        field,
                        Arrays.copyOf(value, value.length)
                ));
                Assert.assertEquals(0, plan.added());
                Assert.assertFalse(plan.changedAny());
                Assert.assertArrayEquals(new int[0], plan.nativeAllocationSizes());
                Assert.assertEquals(0L, plan.stagedHeapBytes());

                try (HashValue.PreparedHashTableSet prepared = hash.prepareHashTableSet(plan)) {
                    Assert.assertFalse(prepared.changedAny());
                    Assert.assertEquals(0L, prepared.stagedHeapBytes());
                    Assert.assertEquals(fieldObjects,
                            allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
                    Assert.assertEquals(valueObjects,
                            allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));

                    prepared.commit();
                    prepared.releaseSuperseded();
                }

                Assert.assertEquals(handles, nativeHandles(hash));
                Assert.assertArrayEquals(value, hash.hget(field));
                Assert.assertEquals(fieldObjects,
                        allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
                Assert.assertEquals(valueObjects,
                        allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));
            } finally {
                hash.close();
            }
        }
    }

    @Test
    public void hashTableDeltaAbortRestoresOriginalHandlesAndContent() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-table-delta-abort");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            HashValue hash = new HashValue(allocator);
            try {
                byte[] firstField = bytes("first");
                byte[] firstValue = fixedBytes(
                        'a',
                        1,
                        YierdisEncodingThresholds.HASH_MAX_LISTPACK_VALUE_BYTES + 1
                );
                byte[] secondField = bytes("second");
                byte[] secondValue = bytes("original-second");
                byte[] newField = bytes("new");
                hash.hset(firstField, firstValue);
                hash.hset(secondField, secondValue);

                Set<Long> originalHandles = nativeHandles(hash);
                long originalFieldObjects = allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES);
                long originalValueObjects = allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES);

                HashValue.HashTableSetPlan plan = hash.planHashTableSet(List.of(
                        firstField, bytes("staged-first"),
                        secondField, bytes("staged-second"),
                        newField, bytes("staged-new")
                ));
                HashValue.PreparedHashTableSet prepared = hash.prepareHashTableSet(plan);
                try {
                    Assert.assertEquals(originalFieldObjects + 1L,
                            allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
                    Assert.assertEquals(originalValueObjects + 3L,
                            allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));
                    Assert.assertEquals(originalHandles, nativeHandles(hash));
                    Assert.assertArrayEquals(firstValue, hash.hget(firstField));
                    Assert.assertArrayEquals(secondValue, hash.hget(secondField));
                    Assert.assertNull(hash.hget(newField));
                } finally {
                    prepared.close();
                }

                Assert.assertEquals(2, hash.size());
                Assert.assertEquals(originalHandles, nativeHandles(hash));
                Assert.assertArrayEquals(firstValue, hash.hget(firstField));
                Assert.assertArrayEquals(secondValue, hash.hget(secondField));
                Assert.assertNull(hash.hget(newField));
                Assert.assertEquals(originalFieldObjects,
                        allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
                Assert.assertEquals(originalValueObjects,
                        allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));
            } finally {
                hash.close();
            }
        }
    }

    @Test
    public void hashTableDeltaCommitUsesLastDuplicateAndReusesExistingFieldHandles() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-table-delta-commit");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            HashValue hash = new HashValue(allocator);
            byte[] existingField = bytes("existing");
            byte[] untouchedField = bytes("untouched");
            byte[] newField = bytes("new");
            byte[] originalValue = fixedBytes(
                    'o',
                    1,
                    YierdisEncodingThresholds.HASH_MAX_LISTPACK_VALUE_BYTES + 1
            );
            byte[] untouchedValue = bytes("keep");
            byte[] finalExistingValue = bytes("existing-final");
            byte[] finalNewValue = bytes("new-final");
            try {
                hash.hset(existingField, originalValue);
                hash.hset(untouchedField, untouchedValue);
                Set<Long> originalFieldHandles = nativeHandles(hash, NativeObjectKind.HASH_FIELD_BYTES);
                Set<Long> originalValueHandles = nativeHandles(hash, NativeObjectKind.HASH_VALUE_BYTES);
                long originalFieldObjects = allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES);
                long originalValueObjects = allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES);

                HashValue.HashTableSetPlan plan = hash.planHashTableSet(List.of(
                        existingField, bytes("existing-first"),
                        newField, bytes("new-first"),
                        existingField, finalExistingValue,
                        newField, finalNewValue,
                        untouchedField, untouchedValue
                ));
                Assert.assertEquals(1, plan.added());
                Assert.assertTrue(plan.changedAny());
                Assert.assertArrayEquals(
                        new int[]{finalExistingValue.length, finalNewValue.length, newField.length},
                        plan.nativeAllocationSizes()
                );

                HashValue.PreparedHashTableSet prepared = hash.prepareHashTableSet(plan);
                try {
                    Assert.assertEquals(1, prepared.added());
                    Assert.assertTrue(prepared.changedAny());
                    prepared.commit();

                    Assert.assertEquals(3, hash.size());
                    Assert.assertArrayEquals(finalExistingValue, hash.hget(existingField));
                    Assert.assertArrayEquals(finalNewValue, hash.hget(newField));
                    Assert.assertArrayEquals(untouchedValue, hash.hget(untouchedField));
                    Set<Long> committedFieldHandles = nativeHandles(hash, NativeObjectKind.HASH_FIELD_BYTES);
                    Assert.assertTrue(committedFieldHandles.containsAll(originalFieldHandles));
                    Assert.assertEquals(originalFieldHandles.size() + 1, committedFieldHandles.size());
                    Set<Long> committedValueHandles = nativeHandles(hash, NativeObjectKind.HASH_VALUE_BYTES);
                    Assert.assertEquals(3, committedValueHandles.size());
                    Assert.assertEquals(1, intersectionSize(originalValueHandles, committedValueHandles));
                    Assert.assertEquals(originalFieldObjects + 1L,
                            allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
                    Assert.assertEquals(originalValueObjects + 2L,
                            allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));

                    prepared.releaseSuperseded();
                } finally {
                    prepared.close();
                }

                Assert.assertEquals(originalFieldObjects + 1L,
                        allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
                Assert.assertEquals(originalValueObjects + 1L,
                        allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));
            } finally {
                hash.close();
            }
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));
        }
    }

    @Test
    public void rootCreatedPackedHashStoresFieldsAndValuesAsNativeBytesAndStreamsNativeSlices() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-native-packed-bytes");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             HashRoot root = new HashRoot(allocator)) {
            ValueHandle handle = root.create();

            root.hsetMany(handle, List.of(bytes("field"), bytes("value")));

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));
            RecordingBulkStringSink out = new RecordingBulkStringSink();
            root.hgetallPairsInto(handle, out);
            Assert.assertTrue(out.sawNativeBytesSlice());
            Assert.assertEquals(List.of("field", "value"), out.strings());
        }
    }

    @Test
    public void packedHgetValueReturnsOnlyTheValueSlice() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-native-value-slice");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            HashValue hash = new HashValue(allocator);
            try {
                hash.hset(bytes("field"), bytes("value"));
                try (BulkStringValue value = hash.hgetValue(bytes("field"))) {
                    RecordingBulkStringSink out = new RecordingBulkStringSink();
                    value.writeTo(out);
                    Assert.assertEquals(List.of("value"), out.strings());
                    Assert.assertEquals(bytes("value").length, value.payloadLength());
                }
            } finally {
                hash.close();
            }
        }
    }

    @Test
    public void packedHashSupportsUpdateAndDeleteWithRepacking() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            HashValue hv = new HashValue(allocator);
            try {
                Assert.assertEquals(ValueEncoding.HASH_PACKED, hv.encoding());

                byte[] f1 = new byte[]{0, 'f', 1};
                byte[] v1 = new byte[]{'v'};
                byte[] f2 = new byte[]{'k'};
                byte[] v2 = new byte[]{'\n', 0, (byte) 0xFF};

                Assert.assertEquals(1, hv.hset(f1, v1));
                Assert.assertEquals(1, hv.hset(f2, v2));
                Assert.assertEquals(2, hv.size());

                Assert.assertArrayEquals(v1, hv.hget(f1));
                Assert.assertArrayEquals(v2, hv.hget(f2));

                byte[] v1Longer = new byte[]{'v', '0', '-', 'n', 'e', 'w'};
                Assert.assertEquals(0, hv.hset(f1, v1Longer));
                Assert.assertArrayEquals(v1Longer, hv.hget(f1));
                Assert.assertArrayEquals(v2, hv.hget(f2));

                Assert.assertEquals(1, hv.hdel(List.of(f2)));
                Assert.assertEquals(1, hv.size());
                Assert.assertNull(hv.hget(f2));
                Assert.assertArrayEquals(v1Longer, hv.hget(f1));

                Assert.assertEquals(1, hv.hdel(Arrays.asList(f1, f1)));
                Assert.assertEquals(0, hv.size());
                Assert.assertNull(hv.hget(f1));
            } finally {
                hv.close();
            }
        }
    }

    @Test
    public void hashConvertsToHashTableAfterTooManyFields() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            HashValue hv = new HashValue(allocator);
            try {
                int added = 0;
                for (int i = 0; i < 600; i++) {
                    byte[] f = ("f" + i).getBytes(StandardCharsets.US_ASCII);
                    byte[] v = ("v" + i).getBytes(StandardCharsets.US_ASCII);
                    added += hv.hset(f, v);
                }
                Assert.assertEquals(600, added);
                Assert.assertEquals(600, hv.size());
                Assert.assertEquals(ValueEncoding.HASH_HT, hv.encoding());
                Assert.assertArrayEquals("v0".getBytes(StandardCharsets.US_ASCII), hv.hget("f0".getBytes(StandardCharsets.US_ASCII)));
                Assert.assertArrayEquals("v599".getBytes(StandardCharsets.US_ASCII), hv.hget("f599".getBytes(StandardCharsets.US_ASCII)));
            } finally {
                hv.close();
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] fixedBytes(char prefix, int index, int length) {
        byte[] out = new byte[length];
        Arrays.fill(out, (byte) prefix);
        byte[] suffix = Integer.toString(index).getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(suffix, 0, out, out.length - suffix.length, suffix.length);
        return out;
    }

    private static Object nativeMapValueSlots(Object owner, String mapFieldName) throws ReflectiveOperationException {
        var mapField = owner.getClass().getDeclaredField(mapFieldName);
        mapField.setAccessible(true);
        Object map = mapField.get(owner);
        var activeField = NativeByteMap.class.getDeclaredField("active");
        activeField.setAccessible(true);
        Object table = activeField.get(map);
        var valueSlotsField = table.getClass().getDeclaredField("valueSlots");
        valueSlotsField.setAccessible(true);
        return valueSlotsField.get(table);
    }

    private static Set<Long> nativeHandles(HashValue hash) {
        Set<Long> handles = new HashSet<>();
        hash.forEachNativeHandle(handle -> handles.add(handle.raw()));
        return handles;
    }

    private static Set<Long> nativeHandles(HashValue hash, NativeObjectKind kind) {
        Set<Long> handles = new HashSet<>();
        hash.forEachNativeHandle(handle -> {
            if (handle.domain() == kind.domain() && handle.kindCode() == kind.code()) {
                handles.add(handle.raw());
            }
        });
        return handles;
    }

    private static int intersectionSize(Set<Long> left, Set<Long> right) {
        Set<Long> intersection = new HashSet<>(left);
        intersection.retainAll(right);
        return intersection.size();
    }

    private static final class RecordingBulkStringSink implements BulkStringSink {
        private final List<String> values = new ArrayList<>();
        private boolean sawNativeBytesSlice;

        @Override
        public void bulkString(byte[] data) {
            sawNativeBytesSlice = false;
            values.add(data == null ? null : new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            sawNativeBytesSlice = false;
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            sawNativeBytesSlice = slice instanceof NativeBytesSlice;
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            values.add(new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            sawNativeBytesSlice = false;
            values.add(Long.toString(value));
        }

        private boolean sawNativeBytesSlice() {
            return sawNativeBytesSlice;
        }

        private List<String> strings() {
            return values;
        }
    }
}
