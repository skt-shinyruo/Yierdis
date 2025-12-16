package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class HashValue implements YierdisValue {
    private final Map<String, String> map = new HashMap<>();

    @Override
    public ValueType type() {
        return ValueType.HASH;
    }

    int size() {
        return map.size();
    }

    int hset(String field, String value) {
        boolean isNew = !map.containsKey(field);
        map.put(field, value);
        return isNew ? 1 : 0;
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
        return map.get(field);
    }

    int hdel(List<String> fields) {
        int removed = 0;
        for (String f : fields) {
            if (map.remove(f) != null) {
                removed++;
            }
        }
        return removed;
    }

    List<String> hgetallPairs() {
        List<String> out = new ArrayList<>(map.size() * 2);
        for (Map.Entry<String, String> e : map.entrySet()) {
            out.add(e.getKey());
            out.add(e.getValue());
        }
        return out;
    }
}
