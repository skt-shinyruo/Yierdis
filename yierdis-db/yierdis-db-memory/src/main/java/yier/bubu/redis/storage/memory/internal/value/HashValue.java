package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class HashValue implements YierdisValue {
    private final NativeByteStore fieldStore;
    private final NativeByteStore valueStore;

    private NativeListpack packed;
    private NativeByteMap<NativeHandle> map;

    public HashValue(NativeAllocator allocator) {
        NativeAllocator nativeAllocator = Objects.requireNonNull(allocator, "allocator");
        this.fieldStore = new NativeByteStore(nativeAllocator, NativeObjectKind.HASH_FIELD_BYTES);
        this.valueStore = new NativeByteStore(nativeAllocator, NativeObjectKind.HASH_VALUE_BYTES);
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
            packed.removeAt(pairIndex + 1);
            packed.removeAt(pairIndex);
            removed++;
        }
        return removed;
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
                packed.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                packed = null;
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

    private void convertToHashMap() {
        if (map != null) {
            return;
        }
        NativeByteMap<NativeHandle> out = new NativeByteMap<>(fieldStore, NativeObjectKind.HASH_FIELD_BYTES);
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
    }

    private static boolean isOversize(byte[] b) {
        return b != null && b.length > YierdisEncodingThresholds.HASH_MAX_LISTPACK_VALUE_BYTES;
    }
}
