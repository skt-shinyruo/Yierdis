package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.List;

final class HashValue implements YierdisValue {
    // Redis stores small hashes in a compact encoding (listpack) and upgrades to hashtable as needed.
    // We approximate that behavior by starting with small parallel arrays (packed) and upgrading to a hash map.

    // Packed form uses a listpack-like contiguous buffer containing [field][value] pairs.
    // This preserves binary-safe semantics while avoiding per-entry byte[] objects.
    private final YierdisListpack packed = new YierdisListpack();

    private ByteArrayHashMap<byte[]> map;
    private long rawBytes;

    @Override
    public ValueType type() {
        return ValueType.HASH;
    }

    @Override
    public ValueEncoding encoding() {
        return map != null ? ValueEncoding.HASH_HT : ValueEncoding.HASH_PACKED;
    }

    int size() {
        if (map != null) {
            return map.size();
        }
        return packed.size() / 2;
    }

    int hset(byte[] field, byte[] value) {
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

    private boolean shouldConvertToHashMap(byte[] field, byte[] value) {
        if (packed.size() / 2 >= YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES) {
            return true;
        }
        return isOversize(field) || isOversize(value);
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
        if (map != null) {
            return rawBytes + map.estimatedBytes();
        }
        return packed.allocatedBytes();
    }
}
