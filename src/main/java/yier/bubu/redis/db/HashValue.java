package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class HashValue implements YierdisValue {
    // Redis stores small hashes in a compact encoding (listpack) and upgrades to hashtable as needed.
    // We approximate that behavior by starting with small parallel arrays (packed) and upgrading to a hash map.
    private static final int LISTPACK_MAX_ENTRIES = 512;
    private static final int LISTPACK_MAX_ELEMENT_BYTES = 64;

    private byte[][] packedFields = new byte[0][];
    private byte[][] packedValues = new byte[0][];
    private int packedSize = 0;

    private ByteArrayHashMap<byte[]> map;

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
        return packedSize;
    }

    int hset(byte[] field, byte[] value) {
        if (map != null) {
            boolean isNew = !map.containsKey(field);
            map.put(field, value);
            return isNew ? 1 : 0;
        }

        if (shouldConvertToHashMap(field, value)) {
            convertToHashMap();
            return hset(field, value);
        }

        int entryIndex = indexOfField(field);
        if (entryIndex >= 0) {
            packedValues[entryIndex] = value;
            return 0;
        }

        ensurePackedCapacity(packedSize + 1);
        packedFields[packedSize] = field;
        packedValues[packedSize] = value;
        packedSize++;

        if (packedSize > LISTPACK_MAX_ENTRIES) {
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
        int entryIndex = indexOfField(field);
        if (entryIndex < 0) {
            return null;
        }
        return packedValues[entryIndex];
    }

    int hdel(List<byte[]> fields) {
        int removed = 0;
        if (map != null) {
            for (byte[] f : fields) {
                if (map.removeKey(f)) {
                    removed++;
                }
            }
            return removed;
        }

        for (byte[] f : fields) {
            int entryIndex = indexOfField(f);
            if (entryIndex < 0) {
                continue;
            }
            removeAt(entryIndex);
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

        List<byte[]> out = new ArrayList<>(packedSize * 2);
        for (int i = 0; i < packedSize; i++) {
            out.add(packedFields[i]);
            out.add(packedValues[i]);
        }
        return out;
    }

    private boolean shouldConvertToHashMap(byte[] field, byte[] value) {
        if (packedSize >= LISTPACK_MAX_ENTRIES) {
            return true;
        }
        return isOversize(field) || isOversize(value);
    }

    private static boolean isOversize(byte[] b) {
        return b != null && b.length > LISTPACK_MAX_ELEMENT_BYTES;
    }

    private int indexOfField(byte[] field) {
        for (int i = 0; i < packedSize; i++) {
            if (Arrays.equals(packedFields[i], field)) {
                return i;
            }
        }
        return -1;
    }

    private void removeAt(int index) {
        int last = packedSize - 1;
        if (index < 0 || index > last) {
            throw new IndexOutOfBoundsException();
        }
        if (index != last) {
            packedFields[index] = packedFields[last];
            packedValues[index] = packedValues[last];
        }
        packedFields[last] = null;
        packedValues[last] = null;
        packedSize--;
    }

    private void ensurePackedCapacity(int desired) {
        if (packedFields.length >= desired) {
            return;
        }
        int next = Math.max(8, packedFields.length);
        while (next < desired) {
            next <<= 1;
        }
        packedFields = Arrays.copyOf(packedFields, next);
        packedValues = Arrays.copyOf(packedValues, next);
    }

    private void convertToHashMap() {
        if (map != null) {
            return;
        }
        ByteArrayHashMap<byte[]> out = new ByteArrayHashMap<>(Math.max(16, packedSize));
        for (int i = 0; i < packedSize; i++) {
            out.put(packedFields[i], packedValues[i]);
        }
        this.packedFields = null;
        this.packedValues = null;
        this.packedSize = 0;
        this.map = out;
    }
}
