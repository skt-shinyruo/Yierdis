package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.memory.MaterializedCollectionScanWindow;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;
import yier.bubu.redis.storage.memory.internal.keyspace.YierdisGlobMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class HashValue implements YierdisValue, NativeHandleOwner, HeapTrackedValue {
    private static final long FIXED_HEAP_BYTES = 88L;
    private final StableMemoryBackend allocator;
    private final NativeByteStore fieldStore;
    private final NativeByteStore valueStore;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;

    private NativeListpack packed;
    private NativeByteMap<NativeHandle> map;
    private HashValue borrowedPackedSource;
    private Runnable heapChangeListener = () -> {
    };

    public HashValue(
            StableMemoryBackend allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        StableMemoryBackend stableMemoryBackend = Objects.requireNonNull(allocator, "allocator");
        this.allocator = stableMemoryBackend;
        this.fieldStore = new NativeByteStore(stableMemoryBackend, NativeObjectKind.HASH_FIELD_BYTES);
        this.valueStore = new NativeByteStore(stableMemoryBackend, NativeObjectKind.HASH_VALUE_BYTES);
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

    public static int[] preparedBuildNativeAllocationSizes(List<byte[]> finalFieldValuePairs) {
        validateFieldValuePairs(finalFieldValuePairs);
        if (usesPackedEncoding(finalFieldValuePairs)) {
            int encodedBytes = NativeListpack.encodedBytesOf(finalFieldValuePairs);
            return encodedBytes == 0 ? new int[0] : new int[]{encodedBytes};
        }

        int allocationCount = finalFieldValuePairs.size() / 2;
        for (int index = 1; index < finalFieldValuePairs.size(); index += 2) {
            if (finalFieldValuePairs.get(index) != null) {
                allocationCount++;
            }
        }
        int[] sizes = new int[allocationCount];
        int next = 0;
        for (int index = 0; index < finalFieldValuePairs.size(); index += 2) {
            sizes[next++] = Math.max(1, finalFieldValuePairs.get(index).length);
            byte[] value = finalFieldValuePairs.get(index + 1);
            if (value != null) {
                sizes[next++] = Math.max(1, value.length);
            }
        }
        return sizes;
    }

    public void loadForBuild(List<byte[]> finalFieldValuePairs) {
        validateFieldValuePairs(finalFieldValuePairs);
        if (size() != 0 || map != null || packed == null || !packed.isEmpty()) {
            throw new IllegalStateException("staged hash build requires an empty value");
        }
        if (!usesPackedEncoding(finalFieldValuePairs)) {
            convertToHashMap();
            hsetMany(finalFieldValuePairs);
            return;
        }

        int encodedBytes = NativeListpack.encodedBytesOf(finalFieldValuePairs);
        if (encodedBytes > 0) {
            packed.reserveForBuild(finalFieldValuePairs.size(), encodedBytes);
        }
        for (int index = 0; index < finalFieldValuePairs.size(); index += 2) {
            packed.addLast(finalFieldValuePairs.get(index), NativeObjectKind.HASH_FIELD_BYTES);
            packed.addLast(finalFieldValuePairs.get(index + 1), NativeObjectKind.HASH_VALUE_BYTES);
        }
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
            int finalEntryCount = packed.size() + (pairIndex >= 0 ? 0 : 2);
            int finalEncodedBytes = packed.encodedBytes();
            if (pairIndex >= 0) {
                finalEncodedBytes = Math.addExact(
                        finalEncodedBytes - packed.encodedEntryBytesAt(pairIndex + 1),
                        NativeListpack.entryEncodedBytes(value)
                );
            } else {
                finalEncodedBytes = Math.addExact(
                        finalEncodedBytes,
                        Math.addExact(
                                NativeListpack.entryEncodedBytes(field),
                                NativeListpack.entryEncodedBytes(value)
                        )
                );
            }
            replacement.packed.reserveForBuild(finalEntryCount, finalEncodedBytes);
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

    public HashTableSetPlan planHashTableSet(List<byte[]> fieldValuePairs) {
        validateFieldValuePairs(fieldValuePairs);
        if (map == null) {
            throw new IllegalStateException("hash-table set planning requires HASH_HT encoding");
        }

        ArrayList<byte[]> canonicalPairs = canonicalFieldValuePairs(fieldValuePairs);
        HashTableSetEntry[] entries = new HashTableSetEntry[canonicalPairs.size() / 2];
        int added = 0;
        int changed = 0;
        int valueAllocationCount = 0;
        int fieldAllocationCount = 0;
        for (int pairIndex = 0; pairIndex < canonicalPairs.size(); pairIndex += 2) {
            byte[] field = canonicalPairs.get(pairIndex);
            byte[] nextValue = canonicalPairs.get(pairIndex + 1);
            NativeHandle previousValue = map.get(field);
            boolean present = previousValue != null || map.containsKey(field);
            boolean valueChanged = !present || !storedValueEquals(previousValue, nextValue);
            entries[pairIndex / 2] = new HashTableSetEntry(
                    field,
                    nextValue,
                    present,
                    previousValue,
                    valueChanged
            );
            if (!present) {
                added++;
            }
            if (!valueChanged) {
                continue;
            }
            changed++;
            if (!present) {
                fieldAllocationCount++;
            }
            if (nextValue != null) {
                valueAllocationCount++;
            }
        }

        int[] allocationSizes = new int[valueAllocationCount + fieldAllocationCount];
        int nextSize = 0;
        for (HashTableSetEntry entry : entries) {
            if (entry.changed && entry.nextValue != null) {
                allocationSizes[nextSize++] = Math.max(1, entry.nextValue.length);
            }
        }
        for (HashTableSetEntry entry : entries) {
            if (entry.changed && !entry.present) {
                allocationSizes[nextSize++] = Math.max(1, entry.field.length);
            }
        }
        long stagedHeapBytes = changed == 0
                ? 0L
                : map.estimatedPreparedPutHeapGrowthBytes(changed, added);
        return new HashTableSetPlan(
                this,
                map,
                entries,
                added,
                changed,
                allocationSizes,
                stagedHeapBytes
        );
    }

    public PreparedHashTableSet prepareHashTableSet(HashTableSetPlan plan) {
        Objects.requireNonNull(plan, "plan");
        plan.validateFor(this, map);
        if (!plan.changedAny()) {
            return new PreparedHashTableSet(plan, null, new NativeHandle[0]);
        }

        NativeHandle[] nextValueHandles = new NativeHandle[plan.changedCount];
        ArrayList<NativeByteMap.StagedPut<NativeHandle>> puts = new ArrayList<>(plan.changedCount);
        NativeByteMap.PreparedMutation<NativeHandle> preparedMap = null;
        int nextChanged = 0;
        try {
            for (HashTableSetEntry entry : plan.entries) {
                if (!entry.changed) {
                    continue;
                }
                NativeHandle nextValue = entry.nextValue == null ? null : valueStore.store(entry.nextValue);
                nextValueHandles[nextChanged++] = nextValue;
                puts.add(new NativeByteMap.StagedPut<>(entry.field, nextValue, entry.present));
            }
            preparedMap = map.preparePuts(puts);
            return new PreparedHashTableSet(plan, preparedMap, nextValueHandles);
        } catch (RuntimeException | Error failure) {
            if (preparedMap != null) {
                try {
                    preparedMap.close();
                } catch (RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            releaseHandles(nextValueHandles, failure);
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

    public ByteValue hgetValue(byte[] field) {
        Objects.requireNonNull(field, "field");
        if (map != null) {
            NativeHandle ref = map.get(field);
            return ref == null ? ByteValue.nullValue() : valueStore.retainedValue(ref);
        }
        int pairIndex = indexOfFieldPair(field);
        if (pairIndex < 0) {
            return ByteValue.nullValue();
        }
        NativeListEntryRef ref = packed.entryRefAt(pairIndex + 1);
        return ref.handle() == null
                ? ByteValue.nullValue()
                : fieldStore.retainedValue(ref.handle(), ref.payloadOffset(), ref.payloadLength());
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

    public CollectionScanWindow hscan(
            ScanCursorV2 cursor,
            byte[] globPattern,
            int count,
            boolean noValues
    ) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }
        ScanCursorV2 current = cursor == null ? ScanCursorV2.start() : cursor;
        if (map != null) {
            int boundedCount = NativeCollectionScanWindow.boundedMatchCount(count);
            int[] matched = {0};
            int expectedElements = boundedCount * (noValues ? 1 : 2);
            try (NativeCollectionScanWindow.Builder builder =
                         NativeCollectionScanWindow.builder(fieldStore.backend(), expectedElements)) {
                NativeByteMap.ScanResult result = map.scanWithWork(
                        current,
                        NativeCollectionScanWindow.slotBudget(boundedCount),
                        (fieldRef, valueRef) -> {
                            var fieldSlice = fieldStore.slice(fieldRef);
                            if (globPattern != null && !YierdisGlobMatcher.matches(globPattern, fieldSlice)) {
                                return true;
                            }
                            builder.addNative(fieldRef, fieldSlice.length());
                            if (!noValues) {
                                if (valueRef == null) {
                                    builder.addNull();
                                } else {
                                    builder.addNative(valueRef, valueStore.length(valueRef));
                                }
                            }
                            matched[0]++;
                            return matched[0] < boundedCount;
                        }
                );
                return builder.build(result.nextCursor());
            }
        }

        // Redis 的 compact hash 在一次调用中完整返回；这样数组位置变化不会破坏跨调用完整迭代语义。
        if (current.value() != 0L) {
            return new MaterializedCollectionScanWindow(ScanCursorV2.start(), List.of());
        }
        int pairCount = packed.size() / 2;
        List<byte[]> out = new ArrayList<>(pairCount * (noValues ? 1 : 2));
        for (int pairIndex = 0; pairIndex < pairCount; pairIndex++) {
            byte[] field = packed.get(pairIndex * 2);
            if (globPattern != null && !YierdisGlobMatcher.matches(globPattern, field)) {
                continue;
            }
            out.add(field);
            if (!noValues) {
                out.add(packed.get(pairIndex * 2 + 1));
            }
        }
        return new MaterializedCollectionScanWindow(ScanCursorV2.start(), out);
    }

    public void hgetallPairsInto(ByteValueSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        if (map != null) {
            map.forEach((fieldRef, valueRef) -> {
                out.value(fieldStore.slice(fieldRef));
                if (valueRef == null) {
                    out.nullValue();
                } else {
                    out.value(valueStore.slice(valueRef));
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
        NativeByteMap<NativeHandle> out = NativeByteMap.nativeHandleValues(
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

    private static boolean usesPackedEncoding(List<byte[]> fieldValuePairs) {
        if (fieldValuePairs.size() / 2 > YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES) {
            return false;
        }
        for (int index = 0; index < fieldValuePairs.size(); index += 2) {
            if (isOversize(fieldValuePairs.get(index)) || isOversize(fieldValuePairs.get(index + 1))) {
                return false;
            }
        }
        return true;
    }

    private static void validateFieldValuePairs(List<byte[]> fieldValuePairs) {
        Objects.requireNonNull(fieldValuePairs, "fieldValuePairs");
        if ((fieldValuePairs.size() & 1) != 0) {
            throw new IllegalArgumentException("fieldValuePairs must contain field/value pairs");
        }
        for (int index = 0; index < fieldValuePairs.size(); index += 2) {
            Objects.requireNonNull(fieldValuePairs.get(index), "hash field");
        }
    }

    private static long heapUpperBoundForEntryCount(long expectedEntries) {
        if (expectedEntries < 0L) {
            return Long.MAX_VALUE;
        }
        long packedEntries = multiplySaturating(expectedEntries, 2L);
        long packedBytes = addSaturating(FIXED_HEAP_BYTES, NativeListpack.heapUpperBoundForEntries(packedEntries));
        long mapBytes = addSaturating(
                FIXED_HEAP_BYTES,
                NativeByteMap.heapUpperBoundForNativeHandleValues(expectedEntries)
        );
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

    private ArrayList<byte[]> canonicalFieldValuePairs(List<byte[]> fieldValuePairs) {
        ArrayList<byte[]> canonical = new ArrayList<>(fieldValuePairs.size());
        CanonicalFieldIndex index = new CanonicalFieldIndex(
                canonical,
                fieldValuePairs.size() / 2,
                hashSeed
        );
        for (int pairIndex = 0; pairIndex < fieldValuePairs.size(); pairIndex += 2) {
            byte[] field = fieldValuePairs.get(pairIndex);
            byte[] value = fieldValuePairs.get(pairIndex + 1);
            int existingPairIndex = index.find(field);
            if (existingPairIndex >= 0) {
                canonical.set(existingPairIndex + 1, value);
                continue;
            }
            int canonicalPairIndex = canonical.size();
            canonical.add(field);
            canonical.add(value);
            index.add(canonicalPairIndex);
        }
        return canonical;
    }

    private boolean storedValueEquals(NativeHandle previousValue, byte[] nextValue) {
        if (previousValue == null || nextValue == null) {
            return previousValue == null && nextValue == null;
        }
        return valueStore.equalsBytes(previousValue, nextValue);
    }

    private void releaseHandles(NativeHandle[] handles, Throwable failure) {
        for (int index = 0; index < handles.length; index++) {
            NativeHandle handle = handles[index];
            if (handle == null) {
                continue;
            }
            try {
                valueStore.release(handle);
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            } finally {
                handles[index] = null;
            }
        }
    }

    public static final class HashTableSetPlan {
        private final HashValue owner;
        private final NativeByteMap<NativeHandle> sourceMap;
        private final HashTableSetEntry[] entries;
        private final int added;
        private final int changedCount;
        private final int[] nativeAllocationSizes;
        private final long stagedHeapBytes;

        private HashTableSetPlan(
                HashValue owner,
                NativeByteMap<NativeHandle> sourceMap,
                HashTableSetEntry[] entries,
                int added,
                int changedCount,
                int[] nativeAllocationSizes,
                long stagedHeapBytes
        ) {
            this.owner = owner;
            this.sourceMap = sourceMap;
            this.entries = entries;
            this.added = added;
            this.changedCount = changedCount;
            this.nativeAllocationSizes = nativeAllocationSizes;
            this.stagedHeapBytes = stagedHeapBytes;
        }

        public int added() {
            return added;
        }

        public boolean changedAny() {
            return changedCount != 0;
        }

        public int[] nativeAllocationSizes() {
            return nativeAllocationSizes.clone();
        }

        public long stagedHeapBytes() {
            return stagedHeapBytes;
        }

        private void validateFor(HashValue expectedOwner, NativeByteMap<NativeHandle> currentMap) {
            if (owner != expectedOwner || sourceMap != currentMap || currentMap == null) {
                throw new IllegalStateException("hash-table set plan no longer matches its source");
            }
            for (HashTableSetEntry entry : entries) {
                NativeHandle currentValue = currentMap.get(entry.field);
                boolean currentPresent = currentValue != null || currentMap.containsKey(entry.field);
                if (currentPresent != entry.present || !Objects.equals(currentValue, entry.previousValue)) {
                    throw new IllegalStateException("hash-table set plan source entry changed");
                }
            }
        }
    }

    public final class PreparedHashTableSet implements AutoCloseable {
        private final HashTableSetPlan plan;
        private NativeByteMap.PreparedMutation<NativeHandle> preparedMap;
        private final NativeHandle[] nextValueHandles;
        private boolean committed;
        private boolean released;

        private PreparedHashTableSet(
                HashTableSetPlan plan,
                NativeByteMap.PreparedMutation<NativeHandle> preparedMap,
                NativeHandle[] nextValueHandles
        ) {
            this.plan = Objects.requireNonNull(plan, "plan");
            this.preparedMap = preparedMap;
            this.nextValueHandles = Objects.requireNonNull(nextValueHandles, "nextValueHandles");
        }

        public int added() {
            return plan.added();
        }

        public boolean changedAny() {
            return plan.changedAny();
        }

        public long stagedHeapBytes() {
            return preparedMap == null ? 0L : preparedMap.stagedHeapBytes();
        }

        public void commit() {
            if (committed || released) {
                throw new IllegalStateException("prepared hash-table set is closed");
            }
            plan.validateFor(HashValue.this, map);
            if (preparedMap != null) {
                preparedMap.validateForCommit();
                preparedMap.commitValidated();
            }
            committed = true;
        }

        public void releaseSuperseded() {
            if (!committed || released) {
                throw new IllegalStateException("prepared hash-table set is not committed");
            }
            RuntimeException cleanupFailure = new RuntimeException(
                    "committed hash-table set cleanup failed"
            );
            if (preparedMap != null) {
                try {
                    preparedMap.releaseSuperseded();
                } catch (RuntimeException | Error releaseFailure) {
                    cleanupFailure.addSuppressed(releaseFailure);
                }
            }
            for (HashTableSetEntry entry : plan.entries) {
                if (!entry.changed || entry.previousValue == null) {
                    continue;
                }
                try {
                    valueStore.release(entry.previousValue);
                } catch (RuntimeException | Error releaseFailure) {
                    cleanupFailure.addSuppressed(releaseFailure);
                } finally {
                    entry.previousValue = null;
                }
            }
            if (cleanupFailure.getSuppressed().length != 0) {
                throw cleanupFailure;
            }
            preparedMap = null;
            released = true;
        }

        @Override
        public void close() {
            if (committed || released) {
                return;
            }
            released = true;
            RuntimeException cleanupFailure = new RuntimeException(
                    "prepared hash-table set abort failed"
            );
            if (preparedMap != null) {
                try {
                    preparedMap.close();
                } catch (RuntimeException | Error closeFailure) {
                    cleanupFailure.addSuppressed(closeFailure);
                } finally {
                    preparedMap = null;
                }
            }
            releaseHandles(nextValueHandles, cleanupFailure);
            if (cleanupFailure.getSuppressed().length != 0) {
                throw cleanupFailure;
            }
        }
    }

    private static final class HashTableSetEntry {
        private final byte[] field;
        private final byte[] nextValue;
        private final boolean present;
        private NativeHandle previousValue;
        private final boolean changed;

        private HashTableSetEntry(
                byte[] field,
                byte[] nextValue,
                boolean present,
                NativeHandle previousValue,
                boolean changed
        ) {
            this.field = field;
            this.nextValue = nextValue;
            this.present = present;
            this.previousValue = previousValue;
            this.changed = changed;
        }
    }

    private static final class CanonicalFieldIndex {
        private static final int MAX_CAPACITY = 1 << 30;

        private final List<byte[]> pairs;
        private final HashSeed hashSeed;
        private final int[] pairIndexes;

        private CanonicalFieldIndex(List<byte[]> pairs, int expectedPairs, HashSeed hashSeed) {
            this.pairs = pairs;
            this.hashSeed = hashSeed;
            this.pairIndexes = new int[indexCapacity(expectedPairs)];
        }

        private int find(byte[] field) {
            int slot = slot(field);
            int mask = pairIndexes.length - 1;
            while (true) {
                int encodedPairIndex = pairIndexes[slot];
                if (encodedPairIndex == 0) {
                    return -1;
                }
                int pairIndex = encodedPairIndex - 1;
                if (Arrays.equals(pairs.get(pairIndex), field)) {
                    return pairIndex;
                }
                slot = (slot + 1) & mask;
            }
        }

        private void add(int pairIndex) {
            int slot = slot(pairs.get(pairIndex));
            int mask = pairIndexes.length - 1;
            while (pairIndexes[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            pairIndexes[slot] = pairIndex + 1;
        }

        private int slot(byte[] field) {
            int hash = SipHash24.foldToInt(SipHash24.hash(hashSeed, field));
            return (hash ^ (hash >>> 16)) & (pairIndexes.length - 1);
        }

        private static int indexCapacity(int expectedPairs) {
            long required = Math.max(16L, (long) expectedPairs * 2L);
            if (required > MAX_CAPACITY) {
                throw new IllegalArgumentException("too many hash fields to plan");
            }
            int capacity = 16;
            while (capacity < required) {
                capacity <<= 1;
            }
            return capacity;
        }
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
