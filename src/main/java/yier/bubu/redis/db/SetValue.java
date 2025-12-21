package yier.bubu.redis.db;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class SetValue implements YierdisValue {
    // Redis uses intset for small integer-only sets and upgrades to hashtable as needed.
    // We approximate that behavior by starting with a sorted long[] and upgrading to HashSet<String>.
    private static final int INTSET_MAX_ENTRIES = 512;

    private long[] intset = new long[0];
    private int intsetSize = 0;
    private ByteArrayHashSet hashset;

    @Override
    public ValueType type() {
        return ValueType.SET;
    }

    @Override
    public ValueEncoding encoding() {
        return hashset != null ? ValueEncoding.SET_HT : ValueEncoding.SET_INTSET;
    }

    int size() {
        if (hashset != null) {
            return hashset.size();
        }
        return intsetSize;
    }

    int addAll(List<byte[]> members) {
        int added = 0;
        for (byte[] m : members) {
            if (hashset != null) {
                if (hashset.add(m)) {
                    added++;
                }
                continue;
            }

            Long parsed = parseLongStrictAscii(m);
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

    int removeAll(List<byte[]> members) {
        int removed = 0;
        for (byte[] m : members) {
            if (hashset != null) {
                if (hashset.remove(m)) {
                    removed++;
                }
                continue;
            }

            Long parsed = parseLongStrictAscii(m);
            if (parsed == null) {
                continue;
            }
            if (intsetRemove(parsed)) {
                removed++;
            }
        }
        return removed;
    }

    boolean contains(byte[] member) {
        if (hashset != null) {
            return hashset.contains(member);
        }
        Long parsed = parseLongStrictAscii(member);
        if (parsed == null) {
            return false;
        }
        return intsetContains(parsed);
    }

    List<byte[]> members() {
        if (hashset != null) {
            List<byte[]> out = new ArrayList<>(hashset.size());
            hashset.forEach(out::add);
            return out;
        }
        List<byte[]> out = new ArrayList<>(intsetSize);
        for (int i = 0; i < intsetSize; i++) {
            out.add(Long.toString(intset[i]).getBytes(StandardCharsets.US_ASCII));
        }
        return out;
    }

    private void convertToHashSet() {
        if (hashset != null) {
            return;
        }
        ByteArrayHashSet out = new ByteArrayHashSet(Math.max(16, intsetSize));
        for (int i = 0; i < intsetSize; i++) {
            out.add(Long.toString(intset[i]).getBytes(StandardCharsets.US_ASCII));
        }
        this.hashset = out;
        this.intset = null;
        this.intsetSize = 0;
    }

    private static Long parseLongStrictAscii(byte[] s) {
        if (s == null || s.length == 0) {
            return null;
        }

        int i = 0;
        boolean negative = false;
        byte first = s[0];
        if (first == '-' || first == '+') {
            negative = first == '-';
            i = 1;
            if (i == s.length) {
                return null;
            }
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < s.length) {
            int digit = s[i++] - '0';
            if (digit < 0 || digit > 9) {
                return null;
            }
            if (result < multMin) {
                return null;
            }
            result *= 10;
            if (result < limit + digit) {
                return null;
            }
            result -= digit;
        }

        long value = negative ? result : -result;

        // Only treat canonical integer representations as integers. This preserves binary-safe
        // semantics for members like "01" (distinct from "1"), "+1", and "-0" (distinct from "0").
        String canonical = Long.toString(value);
        if (canonical.length() != s.length) {
            return null;
        }
        for (int j = 0; j < s.length; j++) {
            if ((byte) canonical.charAt(j) != s[j]) {
                return null;
            }
        }
        return value;
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
