package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class HashValue implements YierdisValue, NativeHandleOwner, HeapTrackedValue {
    private static final long FIXED_HEAP_BYTES = 88L;
    private final NativeAllocator allocator;
    private final NativeByteStore fieldStore;
    private final NativeByteStore valueStore;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;

    private NativeListpack packed;
    private NativeByteMap<NativeHandle> map;
    private HashValue borrowedPackedSource;
    private Runnable heapChangeListener = () -> {
    };

    public HashValue(NativeAllocator allocator) {
        this(allocator, HashSeed.random());
    }

    public HashValue(NativeAllocator allocator, HashSeed hashSeed) {
        this(allocator, hashSeed, null);
    }

    public HashValue(
            NativeAllocator allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        NativeAllocator nativeAllocator = Objects.requireNonNull(allocator, "allocator");
        this.allocator = nativeAllocator;
        this.fieldStore = new NativeByteStore(nativeAllocator, NativeObjectKind.HASH_FIELD_BYTES);
        this.valueStore = new NativeByteStore(nativeAllocator, NativeObjectKind.HASH_VALUE_BYTES);
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.maintenanceRegistry = maintenanceRegistry;
        this.packed = new NativeListpack(fieldStore, NativeObjectKind.HASH_FIELD_BYTES);
    }

    @Override
    public ValueType type() {
        return ValueType.HASH;
    }

    @Override
    public ValueEncoding encoding() {
        return map != null ? ValueEncoding.HASH_HT : ValueEncoding.HASH_PACKED;
    }

    public int size() {
        if (map != null) {
            return map.size();
        }
        return packed.size() / 2;
    }

    public long preparedCopyHeapUpperBound(List<byte[]> fieldValuePairs) {
        return heapUpperBoundForEntryCount(addSaturating(size(), pairCount(fieldValuePairs)));
    }

    public static long preparedNewHeapUpperBound(List<byte[]> fieldValuePairs) {
        return heapUpperBoundForEntryCount(pairCount(fieldValuePairs));
    }

    public HashTableMetrics memberTableMetrics() {
        return map == null ? null : map.metrics();
    }

    public boolean hasMemberTableMaintenanceDebt() {
        return map != null && map.hasMaintenanceDebt();
    }

    public int hset(byte[] field, byte[] value) {
        Objects.requireNonNull(field, "field");
        if (map != null) {
            NativeHandle nextValue = value == null ? null : valueStore.store(value);
            boolean existing = map.containsKey(field);
            boolean ok = false;
            try {
                NativeHandle old = map.put(field, nextValue);
                ok = true;
                if (!existing) {
                    return 1;
                }
                if (old != null) {
                    valueStore.release(old);
                }
                return 0;
            } finally {
                if (!ok && nextValue != null) {
                    valueStore.release(nextValue);
                }
            }
        }

        int pairIndex = indexOfFieldPair(field);
        if (pairIndex >= 0) {
            if (isOversize(value)) {
                convertToHashMap();
                return hset(field, value);
            }
            packed.set(pairIndex + 1, value, NativeObjectKind.HASH_VALUE_BYTES);
            return 0;
        }

        if (packed.size() / 2 >= YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES
                || isOversize(field)
                || isOversize(value)) {
            convertToHashMap();
            return hset(field, value);
        }

        packed.addLast(field, NativeObjectKind.HASH_FIELD_BYTES);
        packed.addLast(value, NativeObjectKind.HASH_VALUE_BYTES);

        if (packed.size() / 2 > YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES) {
            convertToHashMap();
        }
        return 1;
    }

    public PreparedPackedHset preparePackedHset(byte[] field, byte[] value) {
        Objects.requireNonNull(field, "field");
        if (map != null || packed == null) {
            return null;
        }

        int pairIndex = indexOfFieldPair(field);
        if (pairIndex >= 0) {
            if (isOversize(value)) {
                return null;
            }
        } else if (packed.size() / 2 >= YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES
                || isOversize(field)
                || isOversize(value)) {
            return null;
        }

        HashValue replacement = new HashValue(allocator, hashSeed, maintenanceRegistry);
        replacement.borrowedPackedSource = this;
        try {
            for (int index = 0; index < packed.size(); index++) {
                replacement.packed.addBorrowed(packed.entryRefAt(index));
            }
            if (pairIndex >= 0) {
                replacement.packed.replaceBorrowedAt(pairIndex + 1, value, NativeObjectKind.HASH_VALUE_BYTES);
                return new PreparedPackedHset(replacement, 0);
            }
            replacement.packed.addLast(field, NativeObjectKind.HASH_FIELD_BYTES);
            replacement.packed.addLast(value, NativeObjectKind.HASH_VALUE_BYTES);
            return new PreparedPackedHset(replacement, 1);
        } catch (RuntimeException | Error failure) {
            try {
                replacement.close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    public int hsetMany(List<byte[]> fieldValuePairs) {
        int added = 0;
        for (int i = 0; i < fieldValuePairs.size(); i += 2) {
            byte[] field = fieldValuePairs.get(i);
            byte[] value = fieldValuePairs.get(i + 1);
            added += hset(field, value);
        }
        return added;
    }

    public byte[] hget(byte[] field) {
        Objects.requireNonNull(field, "field");
        if (map != null) {
            NativeHandle ref = map.get(field);
            return ref == null ? null : valueStore.toByteArray(ref);
        }
        int pairIndex = indexOfFieldPair(field);
        if (pairIndex < 0) {
            return null;
        }
        return packed.get(pairIndex + 1);
    }

    public BulkStringValue hgetValue(byte[] field) {
        Objects.requireNonNull(field, "field");
        if (map != null) {
            NativeHandle ref = map.get(field);
            return ref == null ? BulkStringValue.nullValue() : valueStore.retainedValue(ref);
        }
        int pairIndex = indexOfFieldPair(field);
        if (pairIndex < 0) {
            return BulkStringValue.nullValue();
        }
        NativeListEntryRef ref = packed.entryRefAt(pairIndex + 1);
        return ref.handle() == null ? BulkStringValue.nullValue() : fieldStore.retainedValue(ref.handle());
    }

    public int hdel(List<byte[]> fields) {
        int removed = 0;
        if (map != null) {
            for (byte[] field : fields) {
                Objects.requireNonNull(field, "field");
                boolean existing = map.containsKey(field);
                NativeHandle old = map.remove(field);
                if (!existing) {
                    continue;
                }
                if (old != null) {
                    valueStore.release(old);
                }
                removed++;
            }
            return removed;
        }

        for (byte[] field : fields) {
            Objects.requireNonNull(field, "field");
            int pairIndex = indexOfFieldPair(field);
            if (pairIndex < 0) {
                continue;
            }
            packed.removeAtDiscard(pairIndex + 1);
            packed.removeAtDiscard(pairIndex);
            removed++;
        }
        return removed;
    }

    public int countExistingFields(List<byte[]> fields) {
        Objects.requireNonNull(fields, "fields");
        int count = 0;
        for (int i = 0; i < fields.size(); i++) {
            byte[] field = fields.get(i);
            Objects.requireNonNull(field, "field");
            if (containsDuplicateBefore(fields, i, field)) {
                continue;
            }
            if (containsField(field)) {
                count++;
            }
        }
        return count;
    }

    public List<byte[]> hgetallPairs() {
        if (map != null) {
            List<byte[]> out = new ArrayList<>(map.size() * 2);
            map.forEach((fieldRef, valueRef) -> {
                out.add(fieldStore.toByteArray(fieldRef));
                out.add(valueRef == null ? null : valueStore.toByteArray(valueRef));
            });
            return out;
        }

        int pairs = packed.size() / 2;
        List<byte[]> out = new ArrayList<>(pairs * 2);
        for (int i = 0; i < packed.size(); i++) {
            out.add(packed.get(i));
        }
        return out;
    }

    public int[] nativePayloadSizes() {
        List<Integer> sizes = new ArrayList<>();
        if (map != null) {
            map.forEach((fieldRef, valueRef) -> {
                sizes.add(fieldStore.allocatedBytes(fieldRef));
                if (valueRef != null) {
                    sizes.add(valueStore.allocatedBytes(valueRef));
                }
            });
        } else if (packed != null) {
            int[] packedSizes = new int[packed.nativePayloadCount()];
            packed.copyNativePayloadSizes(packedSizes, 0);
            for (int size : packedSizes) {
                sizes.add(size);
            }
        }

        int[] out = new int[sizes.size()];
        for (int i = 0; i < sizes.size(); i++) {
            out[i] = sizes.get(i);
        }
        return out;
    }

    public int hgetallCount() {
        return size() * 2;
    }

    public void hgetallPairsInto(BulkStringSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        if (map != null) {
            map.forEach((fieldRef, valueRef) -> {
                out.bulkString(fieldStore.slice(fieldRef));
                if (valueRef == null) {
                    out.bulkStringNull();
                } else {
                    out.bulkString(valueStore.slice(valueRef));
                }
            });
            return;
        }

        NativeListpack.Cursor c = packed.cursor();
        while (c.next()) {
            c.writeTo(out);
        }
    }

    public long estimatedBytes() {
        if (map != null) {
            return fieldStore.nativeBytes() + valueStore.nativeBytes();
        }
        return packed.estimatedBytes();
    }

    @Override
    public long heapEstimatedBytes() {
        long representationBytes = map != null
                ? map.heapEstimatedBytes()
                : packed == null ? 0L : packed.heapEstimatedBytes();
        return FIXED_HEAP_BYTES + representationBytes;
    }

    public void releaseExcept(HashValue retained) {
        Objects.requireNonNull(retained, "retained");
        if (map != null || packed == null || retained.map != null || retained.packed == null) {
            throw new IllegalStateException("packed hash ownership transfer requires packed values");
        }
        packed.closeExcept(retained.packed);
        packed = null;
        borrowedPackedSource = null;
    }

    public void activateBorrowedPackedOwnership(HashValue source) {
        if (borrowedPackedSource != source) {
            throw new IllegalStateException("packed hash ownership source does not match");
        }
        borrowedPackedSource = null;
    }

    @Override
    public void setHeapChangeListener(Runnable listener) {
        heapChangeListener = Objects.requireNonNull(listener, "listener");
    }

    @Override
    public void forEachNativeHandle(Consumer<NativeHandle> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (map != null) {
            map.forEach((fieldRef, valueRef) -> {
                consumer.accept(fieldRef);
                if (valueRef != null) {
                    consumer.accept(valueRef);
                }
            });
            return;
        }
        packed.forEachNativeHandle(consumer);
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        if (map != null) {
            try {
                map.forEach((fieldRef, valueRef) -> {
                    if (valueRef != null) {
                        valueStore.release(valueRef);
                    }
                });
                map.close();
            } catch (RuntimeException e) {
                failure = e;
            } finally {
                map = null;
            }
        }
        if (packed != null) {
            try {
                if (borrowedPackedSource == null || borrowedPackedSource.packed == null) {
                    packed.close();
                } else {
                    packed.closeExcept(borrowedPackedSource.packed);
                }
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                packed = null;
                borrowedPackedSource = null;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private int indexOfFieldPair(byte[] field) {
        int idx = 0;
        NativeListpack.Cursor c = packed.cursor();
        while (c.next()) {
            if ((idx & 1) == 0 && c.equalsBytes(field)) {
                return idx;
            }
            idx++;
        }
        return -1;
    }

    private boolean containsField(byte[] field) {
        return map != null ? map.containsKey(field) : indexOfFieldPair(field) >= 0;
    }

    private void convertToHashMap() {
        if (map != null) {
            return;
        }
        NativeByteMap<NativeHandle> out = new NativeByteMap<>(
                fieldStore,
                NativeObjectKind.HASH_FIELD_BYTES,
                hashSeed,
                maintenanceRegistry,
                this::notifyHeapChanged
        );
        boolean ok = false;
        try {
            for (int i = 0; i + 1 < packed.size(); i += 2) {
                byte[] field = packed.get(i);
                byte[] value = packed.get(i + 1);
                NativeHandle nextValue = value == null ? null : valueStore.store(value);
                boolean inserted = false;
                try {
                    out.put(field, nextValue);
                    inserted = true;
                } finally {
                    if (!inserted && nextValue != null) {
                        valueStore.release(nextValue);
                    }
                }
            }
            ok = true;
        } finally {
            if (!ok) {
                out.forEach((fieldRef, valueRef) -> {
                    if (valueRef != null) {
                        valueStore.release(valueRef);
                    }
                });
                out.close();
            }
        }

        packed.close();
        packed = null;
        map = out;
        notifyHeapChanged();
    }

    private static boolean isOversize(byte[] b) {
        return b != null && b.length > YierdisEncodingThresholds.HASH_MAX_LISTPACK_VALUE_BYTES;
    }

    private static long heapUpperBoundForEntryCount(long expectedEntries) {
        if (expectedEntries < 0L) {
            return Long.MAX_VALUE;
        }
        long packedEntries = multiplySaturating(expectedEntries, 2L);
        long packedBytes = addSaturating(FIXED_HEAP_BYTES, NativeListpack.heapUpperBoundForEntries(packedEntries));
        long mapBytes = addSaturating(FIXED_HEAP_BYTES, NativeByteMap.heapUpperBoundForEntries(expectedEntries));
        return Math.max(packedBytes, mapBytes);
    }

    private static long pairCount(List<byte[]> fieldValuePairs) {
        return fieldValuePairs == null ? 0L : fieldValuePairs.size() / 2L;
    }

    private static long addSaturating(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static long multiplySaturating(long left, long right) {
        if (left == 0L || right == 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private void notifyHeapChanged() {
        heapChangeListener.run();
    }

    private static boolean containsDuplicateBefore(List<byte[]> values, int endExclusive, byte[] candidate) {
        for (int i = 0; i < endExclusive; i++) {
            if (Arrays.equals(values.get(i), candidate)) {
                return true;
            }
        }
        return false;
    }

    public record PreparedPackedHset(HashValue replacement, int added) {
        public PreparedPackedHset {
            Objects.requireNonNull(replacement, "replacement");
            if (added < 0 || added > 1) {
                throw new IllegalArgumentException("added must be 0 or 1");
            }
        }
    }
}
