package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class SetValue implements YierdisValue {
    private final Set<String> set = new HashSet<>();

    @Override
    public ValueType type() {
        return ValueType.SET;
    }

    int size() {
        return set.size();
    }

    int addAll(List<String> members) {
        int added = 0;
        for (String m : members) {
            if (set.add(m)) {
                added++;
            }
        }
        return added;
    }

    boolean contains(String member) {
        return set.contains(member);
    }

    List<String> members() {
        return new ArrayList<>(set);
    }
}
