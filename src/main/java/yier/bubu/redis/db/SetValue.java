package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

final class SetValue implements YierdisValue {
    // Redis uses intset for small integer-only sets and upgrades to hashtable as needed.
    // We approximate that behavior by starting with a sorted long[] and upgrading to HashSet<String>.
    private static final int INTSET_MAX_ENTRIES = 512;

    private long[] intset = new long[0];
    private int intsetSize = 0;
    private Set<String> hashset;

    @Override
    public ValueType type() {
        return ValueType.SET;
    }

    int size() {
        if (hashset != null) {
            return hashset.size();
        }
        return intsetSize;
    }

    int addAll(List<String> members) {
        int added = 0;
        for (String m : members) {
            if (hashset != null) {
                if (hashset.add(m)) {
                    added++;
                }
                continue;
            }

            Long parsed = parseLongStrict(m);
            if (parsed == null) {
                convertToHashSet();
                if (hashset.add(m)) {
                    added++;
                }
                continue;
            }

            if (intsetAdd(parsed)) {
                added++;
            }
            if (intsetSize > INTSET_MAX_ENTRIES) {
                convertToHashSet();
            }
        }
        return added;
    }

    int removeAll(List<String> members) {
        int removed = 0;
        for (String m : members) {
            if (hashset != null) {
                if (hashset.remove(m)) {
                    removed++;
                }
                continue;
            }

            Long parsed = parseLongStrict(m);
            if (parsed == null) {
                continue;
            }
            if (intsetRemove(parsed)) {
                removed++;
            }
        }
        return removed;
    }

    boolean contains(String member) {
        if (hashset != null) {
            return hashset.contains(member);
        }
        Long parsed = parseLongStrict(member);
        if (parsed == null) {
            return false;
        }
        return intsetContains(parsed);
    }

    List<String> members() {
        if (hashset != null) {
            return new ArrayList<>(hashset);
        }
        List<String> out = new ArrayList<>(intsetSize);
        for (int i = 0; i < intsetSize; i++) {
            out.add(Long.toString(intset[i]));
        }
        return out;
    }

    private void convertToHashSet() {
        if (hashset != null) {
            return;
        }
        Set<String> out = new HashSet<>(Math.max(16, intsetSize * 2));
        for (int i = 0; i < intsetSize; i++) {
            out.add(Long.toString(intset[i]));
        }
        this.hashset = out;
        this.intset = null;
        this.intsetSize = 0;
    }

    private static Long parseLongStrict(String s) {
        if (s == null || s.isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private boolean intsetContains(long v) {
        return intsetIndexOf(v) >= 0;
    }

    private boolean intsetAdd(long v) {
        int idx = intsetIndexOf(v);
        if (idx >= 0) {
            return false;
        }

        int insertAt = -(idx + 1);
        ensureIntsetCapacity(intsetSize + 1);
        if (insertAt < intsetSize) {
            System.arraycopy(intset, insertAt, intset, insertAt + 1, intsetSize - insertAt);
        }
        intset[insertAt] = v;
        intsetSize++;
        return true;
    }

    private boolean intsetRemove(long v) {
        int idx = intsetIndexOf(v);
        if (idx < 0) {
            return false;
        }
        int remaining = intsetSize - idx - 1;
        if (remaining > 0) {
            System.arraycopy(intset, idx + 1, intset, idx, remaining);
        }
        intsetSize--;
        return true;
    }

    private int intsetIndexOf(long v) {
        int low = 0;
        int high = intsetSize - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long mv = intset[mid];
            if (mv < v) {
                low = mid + 1;
            } else if (mv > v) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    private void ensureIntsetCapacity(int desired) {
        if (intset.length >= desired) {
            return;
        }
        int next = Math.max(4, intset.length);
        while (next < desired) {
            next *= 2;
        }
        long[] out = new long[next];
        if (intsetSize > 0) {
            System.arraycopy(intset, 0, out, 0, intsetSize);
        }
        intset = out;
    }
}
