package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmByteMap;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBytesRefSlice;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmIntSet;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class SetValue implements YierdisValue {
    // Redis uses intset for small integer-only sets and upgrades to hashtable as needed.
    private static final byte[] LONG_MIN_VALUE_BYTES = "-9223372036854775808".getBytes(StandardCharsets.US_ASCII);
    private static final int LONG_BYTES = Long.BYTES;

    private static final Object PRESENT = new Object();

    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final YierdisFfmBlobStore ffmBlobStore;

    private short[] intset16 = new short[0];
    private int[] intset32;
    private long[] intset64;
    private int intsetEncodingBytes = Short.BYTES;
    private int intsetSize = 0;

    private ByteArrayHashSet hashset;
    private long rawBytes;

    private YierdisFfmIntSet intsetFfm;
    private YierdisFfmByteMap<Object> hashsetFfm;

    public SetValue() {
        this.memoryRuntime = null;
        this.ffmBlobStore = null;
    }

    public SetValue(YierdisFfmMemoryRuntime memoryRuntime) {
        this.memoryRuntime = memoryRuntime;
        this.ffmBlobStore = new YierdisFfmBlobStore(memoryRuntime, "set");
        this.intsetFfm = new YierdisFfmIntSet(memoryRuntime);
    }

    @Override
    public ValueType type() {
        return ValueType.SET;
    }

    @Override
    public ValueEncoding encoding() {
        if (memoryRuntime != null) {
            return hashsetFfm != null ? ValueEncoding.SET_HT : ValueEncoding.SET_INTSET;
        }
        if (hashset != null) {
            return ValueEncoding.SET_HT;
        }
        return ValueEncoding.SET_INTSET;
    }

    public int size() {
        if (memoryRuntime != null) {
            return hashsetFfm != null ? hashsetFfm.size() : intsetFfm.size();
        }
        if (hashset != null) {
            return hashset.size();
        }
        return intsetSize;
    }

    public long estimatedBytes() {
        if (memoryRuntime != null) {
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

    public int addAll(List<byte[]> members) {
        int added = 0;
        for (byte[] member : members) {
            if (addOne(member)) {
                added++;
            }
        }
        return added;
    }

    public int removeAll(List<byte[]> members) {
        int removed = 0;
        for (byte[] member : members) {
            if (removeOne(member)) {
                removed++;
            }
        }
        return removed;
    }

    public boolean contains(byte[] member) {
        if (memoryRuntime != null) {
            if (hashsetFfm != null) {
                return hashsetFfm.get(member) != null;
            }

            long parsed = parseCanonicalLongOrSentinel(member);
            boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
            return isInt && intsetFfm.contains(parsed);
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

    public List<byte[]> members() {
        if (memoryRuntime != null) {
            if (hashsetFfm != null) {
                List<byte[]> out = new ArrayList<>(hashsetFfm.size());
                hashsetFfm.forEach((keyRef, ignored) -> out.add(ffmBlobStore.toByteArray(keyRef)));
                return out;
            }

            List<byte[]> out = new ArrayList<>(intsetFfm.size());
            for (int i = 0; i < intsetFfm.size(); i++) {
                out.add(Long.toString(intsetFfm.get(i)).getBytes(StandardCharsets.US_ASCII));
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

    public void membersInto(BulkStringSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        if (memoryRuntime != null) {
            if (hashsetFfm != null) {
                hashsetFfm.forEach((keyRef, ignored) -> out.bulkString(new YierdisFfmBytesRefSlice(keyRef)));
                return;
            }
            for (int i = 0; i < intsetFfm.size(); i++) {
                out.bulkStringLongAscii(intsetFfm.get(i));
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

        if (memoryRuntime != null) {
            if (hashsetFfm != null) {
                boolean added = hashsetFfm.put(member, PRESENT) == null;
                if (added) {
                    rawBytes += member.length;
                }
                return added;
            }

            long parsed = parseCanonicalLongOrSentinel(member);
            boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
            if (!isInt) {
                convertToHashSetFfm();
                boolean added = hashsetFfm.put(member, PRESENT) == null;
                if (added) {
                    rawBytes += member.length;
                }
                return added;
            }

            boolean added = intsetFfm.add(parsed);
            if (added && intsetFfm.size() > YierdisEncodingThresholds.SET_MAX_INTSET_ENTRIES) {
                convertToHashSetFfm();
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

        if (memoryRuntime != null) {
            if (hashsetFfm != null) {
                boolean removed = hashsetFfm.remove(member) != null;
                if (removed) {
                    rawBytes -= member.length;
                }
                return removed;
            }

            long parsed = parseCanonicalLongOrSentinel(member);
            boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
            return isInt && intsetFfm.remove(parsed);
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

    private void convertToHashSetFfm() {
        if (hashsetFfm != null) {
            return;
        }
        YierdisFfmByteMap<Object> out = new YierdisFfmByteMap<>(ffmBlobStore);
        long bytes = 0L;
        for (int i = 0; i < intsetFfm.size(); i++) {
            byte[] member = Long.toString(intsetFfm.get(i)).getBytes(StandardCharsets.US_ASCII);
            out.put(member, PRESENT);
            bytes += member.length;
        }
        intsetFfm.close();
        intsetFfm = null;
        hashsetFfm = out;
        rawBytes = bytes;
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

    @Override
    public void close() {
        if (memoryRuntime != null) {
            if (hashsetFfm != null) {
                hashsetFfm.close();
                hashsetFfm = null;
            }
            if (intsetFfm != null) {
                intsetFfm.close();
                intsetFfm = null;
            }
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
