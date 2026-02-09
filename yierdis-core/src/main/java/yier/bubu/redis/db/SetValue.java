package yier.bubu.redis.db;

import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapDictLong;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapRawSlice;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAddressAllocator;
import yier.bubu.redis.protocol.ReplySink;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class SetValue implements YierdisValue {
    // Redis uses intset for small integer-only sets and upgrades to hashtable as needed.
    private static final byte[] LONG_MIN_VALUE_BYTES = "-9223372036854775808".getBytes(StandardCharsets.US_ASCII);
    private static final int LONG_BYTES = Long.BYTES;

    private final YierdisOffHeapAddressAllocator offHeapAllocator;

    private short[] intset16 = new short[0];
    private int[] intset32;
    private long[] intset64;
    private int intsetEncodingBytes = Short.BYTES;
    private int intsetSize = 0;

    private ByteArrayHashSet hashset;
    private long rawBytes;

    // Unsafe off-heap mode:
    // - intset stores sorted longs in a single off-heap long[]
    // - hashtable stores members as dict keys (values are dummy 1L)
    private long intsetAddr;
    private int intsetCapOffHeap;
    private YierdisUnsafeOffHeapDictLong hashsetOffHeap;

    SetValue() {
        this.offHeapAllocator = null;
    }

    SetValue(YierdisOffHeapAddressAllocator allocator) {
        this.offHeapAllocator = allocator;
    }

    @Override
    public ValueType type() {
        return ValueType.SET;
    }

    @Override
    public ValueEncoding encoding() {
        if (offHeapAllocator != null) {
            return hashsetOffHeap != null ? ValueEncoding.SET_HT : ValueEncoding.SET_INTSET;
        }
        if (hashset != null) {
            return ValueEncoding.SET_HT;
        }
        return ValueEncoding.SET_INTSET;
    }

    int size() {
        if (offHeapAllocator != null) {
            return hashsetOffHeap != null ? hashsetOffHeap.size() : intsetSize;
        }
        if (hashset != null) {
            return hashset.size();
        }
        return intsetSize;
    }

    long estimatedBytes() {
        if (offHeapAllocator != null) {
            return 0;
        }
        if (hashset != null) {
            return rawBytes + hashset.estimatedBytes();
        }

        if (intsetEncodingBytes == Short.BYTES) {
            return (long) (intset16 == null ? 0 : intset16.length) * Short.BYTES;
        }
        if (intsetEncodingBytes == Integer.BYTES) {
            return (long) (intset32 == null ? 0 : intset32.length) * Integer.BYTES;
        }
        if (intsetEncodingBytes == Long.BYTES) {
            return (long) (intset64 == null ? 0 : intset64.length) * Long.BYTES;
        }
        return 0;
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
        if (offHeapAllocator != null) {
            if (hashsetOffHeap != null) {
                return hashsetOffHeap.get(member) != 0L;
            }

            long parsed = parseCanonicalLongOrSentinel(member);
            boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
            if (!isInt) {
                return false;
            }
            return intsetContainsOffHeap(parsed);
        }

        if (hashset != null) {
            return hashset.contains(member);
        }

        long parsed = parseCanonicalLongOrSentinel(member);
        boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
        if (!isInt) {
            return false;
        }
        return intsetContains(parsed);
    }

    List<byte[]> members() {
        if (offHeapAllocator != null) {
            if (hashsetOffHeap != null) {
                List<byte[]> out = new ArrayList<>(hashsetOffHeap.size());
                hashsetOffHeap.forEach((keyPtr, keyLen, value) -> {
                    byte[] k = new byte[keyLen];
                    offHeapAllocator.copyMemory(keyPtr, k, 0, keyLen);
                    out.add(k);
                });
                return out;
            }

            List<byte[]> out = new ArrayList<>(intsetSize);
            for (int i = 0; i < intsetSize; i++) {
                out.add(Long.toString(intsetLongAtOffHeap(i)).getBytes(StandardCharsets.US_ASCII));
            }
            return out;
        }

        if (hashset != null) {
            List<byte[]> out = new ArrayList<>(hashset.size());
            hashset.forEach(out::add);
            return out;
        }

        List<byte[]> out = new ArrayList<>(intsetSize);
        for (int i = 0; i < intsetSize; i++) {
            out.add(Long.toString(intsetLongAt(i)).getBytes(StandardCharsets.US_ASCII));
        }
        return out;
    }

    void membersInto(ReplySink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        if (offHeapAllocator != null) {
            if (hashsetOffHeap != null) {
                hashsetOffHeap.forEach((keyPtr, keyLen, value) ->
                        out.bulkString(new YierdisUnsafeOffHeapRawSlice(offHeapAllocator, keyPtr, keyLen)));
                return;
            }
            for (int i = 0; i < intsetSize; i++) {
                out.bulkStringLongAscii(intsetLongAtOffHeap(i));
            }
            return;
        }

        if (hashset != null) {
            hashset.forEach(k -> out.bulkString(k, 0, k.length));
            return;
        }

        for (int i = 0; i < intsetSize; i++) {
            out.bulkStringLongAscii(intsetLongAt(i));
        }
    }

    private boolean addOne(byte[] member) {
        if (member == null) {
            throw new IllegalArgumentException("member must not be null");
        }

        if (offHeapAllocator != null) {
            if (hashsetOffHeap != null) {
                return hashsetOffHeap.put(member, 1L) == 0L;
            }

            long parsed = parseCanonicalLongOrSentinel(member);
            boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
            if (!isInt) {
                convertToHashSetOffHeap();
                return hashsetOffHeap.put(member, 1L) == 0L;
            }

            boolean added = intsetAddOffHeap(parsed);
            if (added && intsetSize > YierdisEncodingThresholds.SET_MAX_INTSET_ENTRIES) {
                convertToHashSetOffHeap();
            }
            return added;
        }

        if (hashset != null) {
            boolean added = hashset.add(member);
            if (added) {
                rawBytes += member.length;
            }
            return added;
        }

        long parsed = parseCanonicalLongOrSentinel(member);
        boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
        if (!isInt) {
            convertToHashSet();
            return hashset.add(member);
        }

        boolean added = intsetAdd(parsed);
        if (added && intsetSize > YierdisEncodingThresholds.SET_MAX_INTSET_ENTRIES) {
            convertToHashSet();
        }
        return added;
    }

    private boolean removeOne(byte[] member) {
        if (member == null) {
            return false;
        }

        if (offHeapAllocator != null) {
            if (hashsetOffHeap != null) {
                return hashsetOffHeap.remove(member) != 0L;
            }

            long parsed = parseCanonicalLongOrSentinel(member);
            boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
            if (!isInt) {
                return false;
            }
            return intsetRemoveOffHeap(parsed);
        }

        if (hashset != null) {
            boolean removed = hashset.remove(member);
            if (removed) {
                rawBytes -= member.length;
            }
            return removed;
        }

        long parsed = parseCanonicalLongOrSentinel(member);
        boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
        if (!isInt) {
            return false;
        }
        return intsetRemove(parsed);
    }

    private void convertToHashSet() {
        if (hashset != null) {
            return;
        }
        ByteArrayHashSet out = new ByteArrayHashSet(Math.max(16, size()));
        long bytes = 0;
        for (int i = 0; i < intsetSize; i++) {
            byte[] member = Long.toString(intsetLongAt(i)).getBytes(StandardCharsets.US_ASCII);
            bytes += member.length;
            out.add(member);
        }

        this.hashset = out;
        this.rawBytes = bytes;
        this.intset16 = null;
        this.intset32 = null;
        this.intset64 = null;
        this.intsetEncodingBytes = 0;
        this.intsetSize = 0;
    }

    private void convertToHashSetOffHeap() {
        if (hashsetOffHeap != null) {
            return;
        }
        if (offHeapAllocator == null) {
            throw new IllegalStateException("offHeapAllocator must not be null");
        }

        YierdisUnsafeOffHeapDictLong out = new YierdisUnsafeOffHeapDictLong(offHeapAllocator);
        for (int i = 0; i < intsetSize; i++) {
            byte[] member = Long.toString(intsetLongAtOffHeap(i)).getBytes(StandardCharsets.US_ASCII);
            out.put(member, 1L);
        }

        if (intsetAddr != 0 && intsetCapOffHeap > 0) {
            offHeapAllocator.freeAddress(intsetAddr, Math.max(8, intsetCapOffHeap * LONG_BYTES));
        }
        intsetAddr = 0;
        intsetCapOffHeap = 0;
        intsetSize = 0;

        this.hashsetOffHeap = out;
    }

    private boolean intsetContains(long v) {
        return intsetIndexOf(v) >= 0;
    }

    private boolean intsetContainsOffHeap(long v) {
        return intsetIndexOfOffHeap(v) >= 0;
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

    private boolean intsetAddOffHeap(long v) {
        ensureIntsetCapacityOffHeap(intsetSize + 1);

        int idx = intsetIndexOfOffHeap(v);
        if (idx >= 0) {
            return false;
        }

        int insertAt = -(idx + 1);
        for (int i = intsetSize; i > insertAt; i--) {
            putLong(intsetAddr, i, getLong(intsetAddr, i - 1));
        }
        putLong(intsetAddr, insertAt, v);
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

    private boolean intsetRemoveOffHeap(long v) {
        int idx = intsetIndexOfOffHeap(v);
        if (idx < 0) {
            return false;
        }
        for (int i = idx; i + 1 < intsetSize; i++) {
            putLong(intsetAddr, i, getLong(intsetAddr, i + 1));
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

    private int intsetIndexOfOffHeap(long v) {
        int low = 0;
        int high = intsetSize - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long mv = intsetLongAtOffHeap(mid);
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

    private long intsetLongAtOffHeap(int index) {
        if (intsetAddr == 0) {
            throw new IllegalStateException("off-heap intset not allocated");
        }
        if (index < 0 || index >= intsetSize) {
            throw new IndexOutOfBoundsException();
        }
        return getLong(intsetAddr, index);
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

    private void ensureIntsetCapacityOffHeap(int desired) {
        if (desired <= 0) {
            throw new IllegalArgumentException("desired must be > 0");
        }
        if (intsetCapOffHeap >= desired && intsetAddr != 0) {
            return;
        }

        int next = Math.max(4, intsetCapOffHeap);
        while (next < desired) {
            next <<= 1;
        }

        long nextAddr = offHeapAllocator.allocateAddress(Math.max(8, next * LONG_BYTES));
        if (intsetAddr != 0 && intsetSize > 0) {
            offHeapAllocator.copyMemory(intsetAddr, nextAddr, (long) intsetSize * LONG_BYTES);
        }
        if (intsetAddr != 0 && intsetCapOffHeap > 0) {
            offHeapAllocator.freeAddress(intsetAddr, Math.max(8, intsetCapOffHeap * LONG_BYTES));
        }

        intsetAddr = nextAddr;
        intsetCapOffHeap = next;
    }

    private long getLong(long base, int index) {
        long addr = base + (long) index * LONG_BYTES;
        long b0 = offHeapAllocator.getByte(addr) & 0xffL;
        long b1 = offHeapAllocator.getByte(addr + 1) & 0xffL;
        long b2 = offHeapAllocator.getByte(addr + 2) & 0xffL;
        long b3 = offHeapAllocator.getByte(addr + 3) & 0xffL;
        long b4 = offHeapAllocator.getByte(addr + 4) & 0xffL;
        long b5 = offHeapAllocator.getByte(addr + 5) & 0xffL;
        long b6 = offHeapAllocator.getByte(addr + 6) & 0xffL;
        long b7 = offHeapAllocator.getByte(addr + 7) & 0xffL;
        return b0
                | (b1 << 8)
                | (b2 << 16)
                | (b3 << 24)
                | (b4 << 32)
                | (b5 << 40)
                | (b6 << 48)
                | (b7 << 56);
    }

    private void putLong(long base, int index, long value) {
        long addr = base + (long) index * LONG_BYTES;
        offHeapAllocator.putByte(addr, (byte) value);
        offHeapAllocator.putByte(addr + 1, (byte) (value >>> 8));
        offHeapAllocator.putByte(addr + 2, (byte) (value >>> 16));
        offHeapAllocator.putByte(addr + 3, (byte) (value >>> 24));
        offHeapAllocator.putByte(addr + 4, (byte) (value >>> 32));
        offHeapAllocator.putByte(addr + 5, (byte) (value >>> 40));
        offHeapAllocator.putByte(addr + 6, (byte) (value >>> 48));
        offHeapAllocator.putByte(addr + 7, (byte) (value >>> 56));
    }

    @Override
    public void close() {
        if (offHeapAllocator == null) {
            return;
        }
        if (hashsetOffHeap != null) {
            hashsetOffHeap.close();
            hashsetOffHeap = null;
        }
        if (intsetAddr != 0 && intsetCapOffHeap > 0) {
            offHeapAllocator.freeAddress(intsetAddr, Math.max(8, intsetCapOffHeap * LONG_BYTES));
            intsetAddr = 0;
            intsetCapOffHeap = 0;
            intsetSize = 0;
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
