package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class HashValue implements YierdisValue {
    // Redis stores small hashes in a compact encoding (listpack) and upgrades to hashtable as needed.
    // We approximate that behavior by starting with an array-of-pairs and upgrading to HashMap.
    private static final int LISTPACK_MAX_ENTRIES = 512;
    private static final int LISTPACK_MAX_ELEMENT_BYTES = 64;

    private List<byte[]> listpackPairs = new ArrayList<>();
    private Map<ByteArrayKey, byte[]> map;

    @Override
    public ValueType type() {
        return ValueType.HASH;
    }

    int size() {
        if (map != null) {
            return map.size();
        }
        return listpackPairs.size() / 2;
    }

    int hset(byte[] field, byte[] value) {
        if (map != null) {
            ByteArrayKey k = new ByteArrayKey(field);
            boolean isNew = !map.containsKey(k);
            map.put(k, value);
            return isNew ? 1 : 0;
        }

        if (shouldConvertToHashMap(field, value)) {
            convertToHashMap();
            return hset(field, value);
        }

        int idx = indexOfField(field);
        if (idx >= 0) {
            listpackPairs.set(idx + 1, value);
            return 0;
        }

        listpackPairs.add(field);
        listpackPairs.add(value);
        if (size() > LISTPACK_MAX_ENTRIES) {
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
            return map.get(new ByteArrayKey(field));
        }
        int idx = indexOfField(field);
        if (idx < 0) {
            return null;
        }
        return listpackPairs.get(idx + 1);
    }

    int hdel(List<byte[]> fields) {
        int removed = 0;
        if (map != null) {
            for (byte[] f : fields) {
                if (map.remove(new ByteArrayKey(f)) != null) {
                    removed++;
                }
            }
            return removed;
        }

        for (byte[] f : fields) {
            int idx = indexOfField(f);
            if (idx < 0) {
                continue;
            }
            listpackPairs.remove(idx + 1);
            listpackPairs.remove(idx);
            removed++;
        }
        return removed;
    }

    List<byte[]> hgetallPairs() {
        if (map != null) {
            List<byte[]> out = new ArrayList<>(map.size() * 2);
            for (Map.Entry<ByteArrayKey, byte[]> e : map.entrySet()) {
                out.add(e.getKey().bytes());
                out.add(e.getValue());
            }
            return out;
        }
        return new ArrayList<>(listpackPairs);
    }

    private boolean shouldConvertToHashMap(byte[] field, byte[] value) {
        if (size() >= LISTPACK_MAX_ENTRIES) {
            return true;
        }
        return isOversize(field) || isOversize(value);
    }

    private static boolean isOversize(byte[] b) {
        return b != null && b.length > LISTPACK_MAX_ELEMENT_BYTES;
    }

    private int indexOfField(byte[] field) {
        for (int i = 0; i < listpackPairs.size(); i += 2) {
            if (ByteArrayKey.compareLex(field, listpackPairs.get(i)) == 0) {
                return i;
            }
        }
        return -1;
    }

    private void convertToHashMap() {
        if (map != null) {
            return;
        }
        Map<ByteArrayKey, byte[]> out = new HashMap<>(Math.max(16, size() * 2));
        for (int i = 0; i < listpackPairs.size(); i += 2) {
            out.put(new ByteArrayKey(listpackPairs.get(i)), listpackPairs.get(i + 1));
        }
        this.listpackPairs = null;
        this.map = out;
    }
}
