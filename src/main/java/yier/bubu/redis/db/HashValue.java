package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class HashValue implements YierdisValue {
    // Redis stores small hashes in a compact encoding (listpack) and upgrades to hashtable as needed.
    // We approximate that behavior by starting with an array-of-pairs and upgrading to HashMap.
    private static final int LISTPACK_MAX_ENTRIES = 512;
    private static final int LISTPACK_MAX_ELEMENT_CHARS = 64;

    private List<String> listpackPairs = new ArrayList<>();
    private Map<String, String> map;

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

    int hset(String field, String value) {
        if (map != null) {
            boolean isNew = !map.containsKey(field);
            map.put(field, value);
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

    int hsetMany(List<String> fieldValuePairs) {
        int added = 0;
        for (int i = 0; i < fieldValuePairs.size(); i += 2) {
            String field = fieldValuePairs.get(i);
            String value = fieldValuePairs.get(i + 1);
            added += hset(field, value);
        }
        return added;
    }

    String hget(String field) {
        if (map != null) {
            return map.get(field);
        }
        int idx = indexOfField(field);
        if (idx < 0) {
            return null;
        }
        return listpackPairs.get(idx + 1);
    }

    int hdel(List<String> fields) {
        int removed = 0;
        if (map != null) {
            for (String f : fields) {
                if (map.remove(f) != null) {
                    removed++;
                }
            }
            return removed;
        }

        for (String f : fields) {
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

    List<String> hgetallPairs() {
        if (map != null) {
            List<String> out = new ArrayList<>(map.size() * 2);
            for (Map.Entry<String, String> e : map.entrySet()) {
                out.add(e.getKey());
                out.add(e.getValue());
            }
            return out;
        }
        return new ArrayList<>(listpackPairs);
    }

    private boolean shouldConvertToHashMap(String field, String value) {
        if (size() >= LISTPACK_MAX_ENTRIES) {
            return true;
        }
        return isOversize(field) || isOversize(value);
    }

    private static boolean isOversize(String s) {
        return s != null && s.length() > LISTPACK_MAX_ELEMENT_CHARS;
    }

    private int indexOfField(String field) {
        for (int i = 0; i < listpackPairs.size(); i += 2) {
            if (field.equals(listpackPairs.get(i))) {
                return i;
            }
        }
        return -1;
    }

    private void convertToHashMap() {
        if (map != null) {
            return;
        }
        Map<String, String> out = new HashMap<>(Math.max(16, size() * 2));
        for (int i = 0; i < listpackPairs.size(); i += 2) {
            out.put(listpackPairs.get(i), listpackPairs.get(i + 1));
        }
        this.listpackPairs = null;
        this.map = out;
    }
}
