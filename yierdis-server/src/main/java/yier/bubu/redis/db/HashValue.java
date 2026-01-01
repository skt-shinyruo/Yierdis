package yier.bubu.redis.db;

import io.netty.util.internal.PlatformDependent;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapDictLong;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapListpack;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapRawSlice;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapSds;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;

import java.util.ArrayList;
import java.util.List;

final class HashValue implements YierdisValue {
    // Redis stores small hashes in a compact encoding (listpack) and upgrades to hashtable as needed.
    // We approximate that behavior by starting with small parallel arrays (packed) and upgrading to a hash map.

    private final YierdisUnsafeOffHeapAllocator offHeapAllocator;

    // Packed form uses a listpack-like contiguous buffer containing [field][value] pairs.
    // This preserves binary-safe semantics while avoiding per-entry byte[] objects.
    private YierdisListpack packed;
    private YierdisUnsafeOffHeapListpack packedOffHeap;

    private ByteArrayHashMap<byte[]> map;
    private YierdisUnsafeOffHeapDictLong dict;
    private long rawBytes;

    HashValue() {
        this.offHeapAllocator = null;
        this.packed = new YierdisListpack();
    }

    HashValue(YierdisUnsafeOffHeapAllocator allocator) {
        this.offHeapAllocator = allocator;
        this.dict = new YierdisUnsafeOffHeapDictLong(allocator);
    }

    @Override
    public ValueType type() {
        return ValueType.HASH;
    }

    @Override
    public ValueEncoding encoding() {
        if (offHeapAllocator != null) {
            return dict != null ? ValueEncoding.HASH_HT : ValueEncoding.HASH_PACKED;
        }
        return map != null ? ValueEncoding.HASH_HT : ValueEncoding.HASH_PACKED;
    }

    int size() {
        if (offHeapAllocator != null) {
            if (dict != null) {
                return dict.size();
            }
            return packedOffHeap.size() / 2;
        }
        if (map != null) {
            return map.size();
        }
        return packed.size() / 2;
    }

    int hset(byte[] field, byte[] value) {
        if (offHeapAllocator != null) {
            if (dict != null) {
                long nextValue = value == null ? 0L : YierdisUnsafeOffHeapSds.allocate(offHeapAllocator, value, 0, value.length);
                long old = dict.put(field, nextValue);
                if (old == 0L) {
                    rawBytes += (long) field.length + (value == null ? 0 : value.length);
                    return 1;
                }
                int oldLen = YierdisUnsafeOffHeapSds.len(old);
                rawBytes += (value == null ? 0 : value.length) - oldLen;
                YierdisUnsafeOffHeapSds.free(offHeapAllocator, old);
                return 0;
            }

            if (shouldConvertToHashMap(field, value)) {
                convertToDict();
                return hset(field, value);
            }

            int pairIndex = indexOfFieldPairOffHeap(field);
            if (pairIndex >= 0) {
                packedOffHeap.set(pairIndex + 1, value);
                return 0;
            }

            packedOffHeap.addLast(field);
            packedOffHeap.addLast(value);

            if (packedOffHeap.size() / 2 > YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES) {
                convertToDict();
            }
            return 1;
        }

        if (map != null) {
            byte[] old = map.put(field, value);
            if (old == null) {
                rawBytes += (long) field.length + value.length;
                return 1;
            }
            rawBytes += value.length - old.length;
            return 0;
        }

        if (shouldConvertToHashMap(field, value)) {
            convertToHashMap();
            return hset(field, value);
        }

        int pairIndex = indexOfFieldPair(field);
        if (pairIndex >= 0) {
            // Replace value at (pairIndex + 1).
            packed.set(pairIndex + 1, value);
            return 0;
        }

        packed.addLast(field);
        packed.addLast(value);

        if (packed.size() / 2 > YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES) {
            convertToHashMap();
        }
        return 1;
    }

    int hsetMany(List<byte[]> fieldValuePairs) {
        int added = 0;
        for (int i = 0; i < fieldValuePairs.size(); i += 2) {
            byte[] field = fieldValuePairs.get(i);
            byte[] value = fieldValuePairs.get(i + 1);
            added += hset(field, value);
        }
        return added;
    }

    byte[] hget(byte[] field) {
        if (offHeapAllocator != null) {
            if (dict != null) {
                long addr = dict.get(field);
                if (addr == 0L) {
                    return null;
                }
                int len = YierdisUnsafeOffHeapSds.len(addr);
                byte[] out = new byte[len];
                YierdisUnsafeOffHeapSds.getBytes(addr, out, 0, len);
                return out;
            }

            int pairIndex = indexOfFieldPairOffHeap(field);
            if (pairIndex < 0) {
                return null;
            }
            return packedOffHeap.get(pairIndex + 1);
        }
        if (map != null) {
            return map.get(field);
        }
        int pairIndex = indexOfFieldPair(field);
        if (pairIndex < 0) {
            return null;
        }
        return packed.get(pairIndex + 1);
    }

    int hdel(List<byte[]> fields) {
        int removed = 0;
        if (offHeapAllocator != null) {
            if (dict != null) {
                for (byte[] f : fields) {
                    long old = dict.remove(f);
                    if (old != 0L) {
                        int oldLen = YierdisUnsafeOffHeapSds.len(old);
                        rawBytes -= (long) f.length + oldLen;
                        YierdisUnsafeOffHeapSds.free(offHeapAllocator, old);
                        removed++;
                    }
                }
                return removed;
            }

            for (byte[] f : fields) {
                int pairIndex = indexOfFieldPairOffHeap(f);
                if (pairIndex < 0) {
                    continue;
                }
                packedOffHeap.removeAt(pairIndex + 1);
                packedOffHeap.removeAt(pairIndex);
                removed++;
            }
            return removed;
        }

        if (map != null) {
            for (byte[] f : fields) {
                byte[] old = map.remove(f);
                if (old != null) {
                    rawBytes -= (long) f.length + old.length;
                    removed++;
                }
            }
            return removed;
        }

        for (byte[] f : fields) {
            int pairIndex = indexOfFieldPair(f);
            if (pairIndex < 0) {
                continue;
            }
            // Remove value then field to keep indices stable.
            packed.removeAt(pairIndex + 1);
            packed.removeAt(pairIndex);
            removed++;
        }
        return removed;
    }

    List<byte[]> hgetallPairs() {
        if (offHeapAllocator != null) {
            if (dict != null) {
                List<byte[]> out = new ArrayList<>(dict.size() * 2);
                dict.forEach((keyPtr, keyLen, valueAddr) -> {
                    byte[] k = new byte[keyLen];
                    PlatformDependent.copyMemory(keyPtr, k, 0, keyLen);
                    out.add(k);

                    if (valueAddr == 0L) {
                        out.add(null);
                    } else {
                        int len = YierdisUnsafeOffHeapSds.len(valueAddr);
                        byte[] v = new byte[len];
                        YierdisUnsafeOffHeapSds.getBytes(valueAddr, v, 0, len);
                        out.add(v);
                    }
                });
                return out;
            }

            int pairs = packedOffHeap.size() / 2;
            List<byte[]> out = new ArrayList<>(pairs * 2);
            for (int i = 0; i < packedOffHeap.size(); i++) {
                out.add(packedOffHeap.get(i));
            }
            return out;
        }

        if (map != null) {
            List<byte[]> out = new ArrayList<>(map.size() * 2);
            map.forEach((k, v) -> {
                out.add(k);
                out.add(v);
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

    int hgetallCount() {
        return size() * 2;
    }

    void hgetallPairsInto(YierdisBulkStringOutput out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        if (offHeapAllocator != null) {
            if (dict != null) {
                dict.forEach((keyPtr, keyLen, valueAddr) -> {
                    out.bulkString(new YierdisUnsafeOffHeapRawSlice(keyPtr, keyLen));
                    if (valueAddr == 0L) {
                        out.bulkStringNull();
                    } else {
                        YierdisOffHeapSlice slice = YierdisUnsafeOffHeapSds.slice(valueAddr);
                        out.bulkString(slice);
                    }
                });
                return;
            }

            YierdisUnsafeOffHeapListpack.Cursor c = packedOffHeap.cursor();
            while (c.next()) {
                c.writeTo(out);
            }
            return;
        }

        if (map != null) {
            map.forEach((k, v) -> {
                out.bulkString(k, 0, k.length);
                if (v == null) {
                    out.bulkStringNull();
                } else {
                    out.bulkString(v, 0, v.length);
                }
            });
            return;
        }

        YierdisListpack.Cursor c = packed.cursor();
        while (c.next()) {
            c.writeTo(out);
        }
    }

    private static boolean isOversize(byte[] b) {
        return b != null && b.length > YierdisEncodingThresholds.HASH_MAX_LISTPACK_VALUE_BYTES;
    }

    private int indexOfFieldPair(byte[] field) {
        // Packed contains pairs: [field0][value0][field1][value1]...
        int idx = 0;
        YierdisListpack.Cursor c = packed.cursor();
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
        int pairs = packed.size() / 2;
        ByteArrayHashMap<byte[]> out = new ByteArrayHashMap<>(Math.max(16, pairs));
        for (int i = 0; i + 1 < packed.size(); i += 2) {
            out.put(packed.get(i), packed.get(i + 1));
        }
        rawBytes = packed.rawBytesSize();
        packed.clear();
        this.map = out;
    }

    long estimatedBytes() {
        if (offHeapAllocator != null) {
            return 0;
        }
        if (map != null) {
            return rawBytes + map.estimatedBytes();
        }
        return packed.allocatedBytes();
    }

    @Override
    public void close() {
        if (offHeapAllocator == null) {
            return;
        }
        if (dict != null) {
            dict.forEach((keyPtr, keyLen, valueAddr) -> YierdisUnsafeOffHeapSds.free(offHeapAllocator, valueAddr));
            dict.close();
            dict = null;
        }
        if (packedOffHeap != null) {
            packedOffHeap.close();
            packedOffHeap = null;
        }
    }

    private boolean shouldConvertToHashMap(byte[] field, byte[] value) {
        if (packedOffHeap != null) {
            if (packedOffHeap.size() / 2 >= YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES) {
                return true;
            }
            return isOversize(field) || isOversize(value);
        }
        if (packed.size() / 2 >= YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES) {
            return true;
        }
        return isOversize(field) || isOversize(value);
    }

    private int indexOfFieldPairOffHeap(byte[] field) {
        int idx = 0;
        YierdisUnsafeOffHeapListpack.Cursor c = packedOffHeap.cursor();
        while (c.next()) {
            if ((idx & 1) == 0 && c.equalsBytes(field)) {
                return idx;
            }
            idx++;
        }
        return -1;
    }

    private void convertToDict() {
        if (dict != null) {
            return;
        }
        YierdisUnsafeOffHeapDictLong out = new YierdisUnsafeOffHeapDictLong(offHeapAllocator);
        for (int i = 0; i + 1 < packedOffHeap.size(); i += 2) {
            byte[] field = packedOffHeap.get(i);
            byte[] value = packedOffHeap.get(i + 1);
            long valueAddr = value == null ? 0L : YierdisUnsafeOffHeapSds.allocate(offHeapAllocator, value, 0, value.length);
            out.put(field, valueAddr);
        }
        packedOffHeap.close();
        packedOffHeap = null;
        rawBytes = 0;
        out.forEach((keyPtr, keyLen, valueAddr) -> {
            int vlen = valueAddr == 0L ? 0 : YierdisUnsafeOffHeapSds.len(valueAddr);
            rawBytes += (long) keyLen + vlen;
        });
        dict = out;
    }
}
