package yier.bubu.redis.db;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class SetValue implements YierdisValue {
    // Redis uses intset for small integer-only sets, listpack for small non-integer sets,
    // and upgrades to hashtable as needed.
    private static final int INTSET_MAX_ENTRIES = 512;
    private static final int LISTPACK_MAX_ENTRIES = 128;
    private static final int LISTPACK_MAX_ELEMENT_BYTES = 64;
    private static final byte[] LONG_MIN_VALUE_BYTES = "-9223372036854775808".getBytes(StandardCharsets.US_ASCII);

    private short[] intset16 = new short[0];
    private int[] intset32;
    private long[] intset64;
    private int intsetEncodingBytes = Short.BYTES;
    private int intsetSize = 0;

    private byte[][] listpack;
    private int listpackSize = 0;

    private ByteArrayHashSet hashset;

    @Override
    public ValueType type() {
        return ValueType.SET;
    }

    @Override
    public ValueEncoding encoding() {
        if (hashset != null) {
            return ValueEncoding.SET_HT;
        }
        if (listpack != null) {
            return ValueEncoding.SET_LISTPACK;
        }
        return ValueEncoding.SET_INTSET;
    }

    int size() {
        if (hashset != null) {
            return hashset.size();
        }
        if (listpack != null) {
            return listpackSize;
        }
        return intsetSize;
    }

    int addAll(List<byte[]> members) {
        int added = 0;
        for (byte[] member : members) {
            if (addOne(member)) {
                added++;
            }
        }
        return added;
    }

    int removeAll(List<byte[]> members) {
        int removed = 0;
        for (byte[] member : members) {
            if (removeOne(member)) {
                removed++;
            }
        }
        return removed;
    }

    boolean contains(byte[] member) {
        if (hashset != null) {
            return hashset.contains(member);
        }
        if (listpack != null) {
            return listpackContains(member);
        }

        long parsed = parseCanonicalLongOrSentinel(member);
        boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
        if (!isInt) {
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
        if (listpack != null) {
            List<byte[]> out = new ArrayList<>(listpackSize);
            for (int i = 0; i < listpackSize; i++) {
                out.add(listpack[i]);
            }
            return out;
        }

        List<byte[]> out = new ArrayList<>(intsetSize);
        for (int i = 0; i < intsetSize; i++) {
            out.add(Long.toString(intsetLongAt(i)).getBytes(StandardCharsets.US_ASCII));
        }
        return out;
    }

    private boolean addOne(byte[] member) {
        if (hashset != null) {
            return hashset.add(member);
        }
        if (listpack != null) {
            if (listpackContains(member)) {
                return false;
            }
            if (shouldConvertListpackToHashSet(member)) {
                convertToHashSet();
                return hashset.add(member);
            }
            ensureListpackCapacity(listpackSize + 1);
            listpack[listpackSize++] = member;
            return true;
        }

        long parsed = parseCanonicalLongOrSentinel(member);
        boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
        if (!isInt) {
            if (shouldConvertIntsetToListpackFor(member)) {
                convertIntsetToListpack();
                return addOne(member);
            }
            convertToHashSet();
            return hashset.add(member);
        }

        boolean added = intsetAdd(parsed);
        if (added && intsetSize > INTSET_MAX_ENTRIES) {
            convertToHashSet();
        }
        return added;
    }

    private boolean removeOne(byte[] member) {
        if (hashset != null) {
            return hashset.remove(member);
        }
        if (listpack != null) {
            int idx = listpackIndexOf(member);
            if (idx < 0) {
                return false;
            }
            int last = listpackSize - 1;
            if (idx != last) {
                listpack[idx] = listpack[last];
            }
            listpack[last] = null;
            listpackSize--;
            return true;
        }

        long parsed = parseCanonicalLongOrSentinel(member);
        boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
        if (!isInt) {
            return false;
        }
        return intsetRemove(parsed);
    }

    private boolean shouldConvertListpackToHashSet(byte[] member) {
        if (listpackSize >= LISTPACK_MAX_ENTRIES) {
            return true;
        }
        return member != null && member.length > LISTPACK_MAX_ELEMENT_BYTES;
    }

    private boolean listpackContains(byte[] member) {
        return listpackIndexOf(member) >= 0;
    }

    private int listpackIndexOf(byte[] member) {
        for (int i = 0; i < listpackSize; i++) {
            if (Arrays.equals(listpack[i], member)) {
                return i;
            }
        }
        return -1;
    }

    private void ensureListpackCapacity(int desired) {
        if (desired <= 0) {
            throw new IllegalArgumentException("desired must be > 0");
        }
        if (listpack == null) {
            listpack = new byte[Math.max(8, desired)][];
            return;
        }
        if (listpack.length >= desired) {
            return;
        }
        int next = Math.max(8, listpack.length);
        while (next < desired) {
            next <<= 1;
        }
        listpack = Arrays.copyOf(listpack, next);
    }

    private boolean shouldConvertIntsetToListpackFor(byte[] newMember) {
        if (intsetSize + 1 > LISTPACK_MAX_ENTRIES) {
            return false;
        }
        if (newMember != null && newMember.length > LISTPACK_MAX_ELEMENT_BYTES) {
            return false;
        }
        for (int i = 0; i < intsetSize; i++) {
            if (longStringByteLength(intsetLongAt(i)) > LISTPACK_MAX_ELEMENT_BYTES) {
                return false;
            }
        }
        return true;
    }

    private void convertIntsetToListpack() {
        if (listpack != null) {
            return;
        }
        byte[][] out = new byte[Math.max(8, intsetSize)][];
        for (int i = 0; i < intsetSize; i++) {
            out[i] = Long.toString(intsetLongAt(i)).getBytes(StandardCharsets.US_ASCII);
        }
        listpack = out;
        listpackSize = intsetSize;

        intset16 = null;
        intset32 = null;
        intset64 = null;
        intsetEncodingBytes = 0;
        intsetSize = 0;
    }

    private void convertToHashSet() {
        if (hashset != null) {
            return;
        }
        ByteArrayHashSet out = new ByteArrayHashSet(Math.max(16, size()));

        if (listpack != null) {
            for (int i = 0; i < listpackSize; i++) {
                out.add(listpack[i]);
            }
        } else {
            for (int i = 0; i < intsetSize; i++) {
                out.add(Long.toString(intsetLongAt(i)).getBytes(StandardCharsets.US_ASCII));
            }
        }

        this.hashset = out;
        this.listpack = null;
        this.listpackSize = 0;
        this.intset16 = null;
        this.intset32 = null;
        this.intset64 = null;
        this.intsetEncodingBytes = 0;
        this.intsetSize = 0;
    }

    private boolean intsetContains(long v) {
        return intsetIndexOf(v) >= 0;
    }

    private boolean intsetAdd(long v) {
        ensureIntsetEncoding(v);

        int idx = intsetIndexOf(v);
        if (idx >= 0) {
            return false;
        }

        int insertAt = -(idx + 1);
        ensureIntsetCapacity(intsetSize + 1);

        int move = intsetSize - insertAt;
        if (move > 0) {
            switch (intsetEncodingBytes) {
                case Short.BYTES:
                    System.arraycopy(intset16, insertAt, intset16, insertAt + 1, move);
                    intset16[insertAt] = (short) v;
                    break;
                case Integer.BYTES:
                    System.arraycopy(intset32, insertAt, intset32, insertAt + 1, move);
                    intset32[insertAt] = (int) v;
                    break;
                default:
                    System.arraycopy(intset64, insertAt, intset64, insertAt + 1, move);
                    intset64[insertAt] = v;
            }
        } else {
            switch (intsetEncodingBytes) {
                case Short.BYTES:
                    intset16[insertAt] = (short) v;
                    break;
                case Integer.BYTES:
                    intset32[insertAt] = (int) v;
                    break;
                default:
                    intset64[insertAt] = v;
            }
        }

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
            switch (intsetEncodingBytes) {
                case Short.BYTES:
                    System.arraycopy(intset16, idx + 1, intset16, idx, remaining);
                    break;
                case Integer.BYTES:
                    System.arraycopy(intset32, idx + 1, intset32, idx, remaining);
                    break;
                default:
                    System.arraycopy(intset64, idx + 1, intset64, idx, remaining);
            }
        }

        intsetSize--;
        return true;
    }

    private int intsetIndexOf(long v) {
        int low = 0;
        int high = intsetSize - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long mv = intsetLongAt(mid);
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

    private long intsetLongAt(int index) {
        switch (intsetEncodingBytes) {
            case Short.BYTES:
                return intset16[index];
            case Integer.BYTES:
                return intset32[index];
            default:
                return intset64[index];
        }
    }

    private void ensureIntsetEncoding(long v) {
        if (intsetEncodingBytes == Long.BYTES) {
            return;
        }

        if (intsetEncodingBytes == Short.BYTES) {
            if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                    upgrade16To64();
                } else {
                    upgrade16To32();
                }
            }
            return;
        }

        if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
            upgrade32To64();
        }
    }

    private void upgrade16To32() {
        int[] out = new int[Math.max(4, intset16.length)];
        for (int i = 0; i < intsetSize; i++) {
            out[i] = intset16[i];
        }
        intset16 = null;
        intset32 = out;
        intsetEncodingBytes = Integer.BYTES;
    }

    private void upgrade16To64() {
        long[] out = new long[Math.max(4, intset16.length)];
        for (int i = 0; i < intsetSize; i++) {
            out[i] = intset16[i];
        }
        intset16 = null;
        intset32 = null;
        intset64 = out;
        intsetEncodingBytes = Long.BYTES;
    }

    private void upgrade32To64() {
        long[] out = new long[Math.max(4, intset32.length)];
        for (int i = 0; i < intsetSize; i++) {
            out[i] = intset32[i];
        }
        intset16 = null;
        intset32 = null;
        intset64 = out;
        intsetEncodingBytes = Long.BYTES;
    }

    private void ensureIntsetCapacity(int desired) {
        switch (intsetEncodingBytes) {
            case Short.BYTES:
                if (intset16.length >= desired) {
                    return;
                }
                intset16 = growShortArray(intset16, desired);
                return;
            case Integer.BYTES:
                if (intset32 == null) {
                    intset32 = new int[Math.max(4, desired)];
                    return;
                }
                if (intset32.length >= desired) {
                    return;
                }
                intset32 = growIntArray(intset32, desired);
                return;
            default:
                if (intset64 == null) {
                    intset64 = new long[Math.max(4, desired)];
                    return;
                }
                if (intset64.length >= desired) {
                    return;
                }
                intset64 = growLongArray(intset64, desired);
        }
    }

    private static short[] growShortArray(short[] in, int desired) {
        int next = Math.max(4, in.length);
        while (next < desired) {
            next *= 2;
        }
        short[] out = new short[next];
        if (in.length > 0) {
            System.arraycopy(in, 0, out, 0, in.length);
        }
        return out;
    }

    private static int[] growIntArray(int[] in, int desired) {
        int next = Math.max(4, in.length);
        while (next < desired) {
            next *= 2;
        }
        int[] out = new int[next];
        if (in.length > 0) {
            System.arraycopy(in, 0, out, 0, in.length);
        }
        return out;
    }

    private static long[] growLongArray(long[] in, int desired) {
        int next = Math.max(4, in.length);
        while (next < desired) {
            next *= 2;
        }
        long[] out = new long[next];
        if (in.length > 0) {
            System.arraycopy(in, 0, out, 0, in.length);
        }
        return out;
    }

    private static boolean isLongMinValueBytes(byte[] s) {
        if (s == null || s.length != LONG_MIN_VALUE_BYTES.length) {
            return false;
        }
        for (int i = 0; i < LONG_MIN_VALUE_BYTES.length; i++) {
            if (s[i] != LONG_MIN_VALUE_BYTES[i]) {
                return false;
            }
        }
        return true;
    }

    private static int longStringByteLength(long v) {
        if (v == Long.MIN_VALUE) {
            return LONG_MIN_VALUE_BYTES.length;
        }
        long x = v < 0 ? -v : v;
        int digits = 1;
        while (x >= 10) {
            x /= 10;
            digits++;
        }
        return v < 0 ? digits + 1 : digits;
    }

    /**
     * Parses a canonical ASCII long. Returns {@link Long#MIN_VALUE} as a sentinel on failure.
     * <p>
     * This rejects "+1", "01", and "-0" to preserve binary-safe set semantics.
     */
    private static long parseCanonicalLongOrSentinel(byte[] s) {
        if (s == null || s.length == 0) {
            return Long.MIN_VALUE;
        }

        int i = 0;
        boolean negative = false;
        byte first = s[0];
        if (first == '+') {
            return Long.MIN_VALUE;
        }
        if (first == '-') {
            negative = true;
            i = 1;
            if (i == s.length) {
                return Long.MIN_VALUE;
            }
        }

        if (s[i] == '0') {
            if (i == s.length - 1) {
                return negative ? Long.MIN_VALUE : 0L;
            }
            return Long.MIN_VALUE;
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < s.length) {
            int digit = s[i++] - '0';
            if (digit < 0 || digit > 9) {
                return Long.MIN_VALUE;
            }
            if (result < multMin) {
                return Long.MIN_VALUE;
            }
            result *= 10;
            if (result < limit + digit) {
                return Long.MIN_VALUE;
            }
            result -= digit;
        }

        return negative ? result : -result;
    }
}
