package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class SetValue implements YierdisValue, NativeHandleOwner, HeapTrackedValue {
    // Redis uses intset for small integer-only sets and upgrades to hashtable as needed.
    private static final byte[] LONG_MIN_VALUE_BYTES = "-9223372036854775808".getBytes(StandardCharsets.US_ASCII);
    private static final int LONG_BYTES = Long.BYTES;
    private static final long FIXED_HEAP_BYTES = 80L;
    private static final long ARRAY_HEADER_BYTES = 16L;

    private static final Object PRESENT = new Object();

    private final NativeByteStore memberStore;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;

    private short[] intset16 = new short[0];
    private int[] intset32;
    private long[] intset64;
    private int intsetEncodingBytes = Short.BYTES;
    private int intsetSize = 0;

    private NativeByteMap<Object> members;
    private Runnable heapChangeListener = () -> {
    };

    public SetValue(NativeAllocator allocator) {
        this(allocator, HashSeed.random());
    }

    public SetValue(NativeAllocator allocator, HashSeed hashSeed) {
        this(allocator, hashSeed, null);
    }

    public SetValue(
            NativeAllocator allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        this.memberStore = new NativeByteStore(Objects.requireNonNull(allocator, "allocator"), NativeObjectKind.SET_MEMBER_BYTES);
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.maintenanceRegistry = maintenanceRegistry;
    }

    @Override
    public ValueType type() {
        return ValueType.SET;
    }

    @Override
    public ValueEncoding encoding() {
        return members != null ? ValueEncoding.SET_HT : ValueEncoding.SET_INTSET;
    }

    public int size() {
        if (members != null) {
            return members.size();
        }
        return intsetSize;
    }

    public long preparedCopyHeapUpperBound(List<byte[]> candidates) {
        return heapUpperBoundForEntryCount(addSaturating(size(), candidateCount(candidates)));
    }

    public static long preparedNewHeapUpperBound(List<byte[]> candidates) {
        return heapUpperBoundForEntryCount(candidateCount(candidates));
    }

    public HashTableMetrics memberTableMetrics() {
        return members == null ? null : members.metrics();
    }

    public boolean hasMemberTableMaintenanceDebt() {
        return members != null && members.hasMaintenanceDebt();
    }

    public long estimatedBytes() {
        if (members != null) {
            return memberStore.nativeBytes();
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

    @Override
    public long heapEstimatedBytes() {
        if (members != null) {
            return FIXED_HEAP_BYTES + members.heapEstimatedBytes();
        }
        if (intsetEncodingBytes == Short.BYTES) {
            return FIXED_HEAP_BYTES + primitiveArrayHeapBytes(intset16 == null ? 0 : intset16.length, Short.BYTES);
        }
        if (intsetEncodingBytes == Integer.BYTES) {
            return FIXED_HEAP_BYTES + primitiveArrayHeapBytes(intset32 == null ? 0 : intset32.length, Integer.BYTES);
        }
        return FIXED_HEAP_BYTES + primitiveArrayHeapBytes(intset64 == null ? 0 : intset64.length, Long.BYTES);
    }

    @Override
    public void setHeapChangeListener(Runnable listener) {
        heapChangeListener = Objects.requireNonNull(listener, "listener");
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
        if (members != null) {
            return members.get(member) != null;
        }

        long parsed = parseCanonicalLongOrSentinel(member);
        boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
        if (!isInt) {
            return false;
        }
        return intsetContains(parsed);
    }

    public List<byte[]> members() {
        if (members != null) {
            List<byte[]> out = new ArrayList<>(members.size());
            members.forEach((memberRef, ignored) -> out.add(memberStore.toByteArray(memberRef)));
            return out;
        }

        List<byte[]> out = new ArrayList<>(intsetSize);
        for (int i = 0; i < intsetSize; i++) {
            out.add(Long.toString(intsetLongAt(i)).getBytes(StandardCharsets.US_ASCII));
        }
        return out;
    }

    public int[] nativePayloadSizes() {
        if (members != null) {
            if (members.size() == 0) {
                return new int[0];
            }
            int[] out = new int[members.size()];
            final int[] next = {0};
            members.forEach((memberRef, ignored) -> out[next[0]++] = memberStore.allocatedBytes(memberRef));
            return out;
        }

        int[] out = new int[intsetSize];
        for (int i = 0; i < intsetSize; i++) {
            out[i] = Math.max(1, longStringByteLength(intsetLongAt(i)));
        }
        return out;
    }

    public int countAdditions(List<byte[]> candidates) {
        Objects.requireNonNull(candidates, "candidates");
        int count = 0;
        for (int i = 0; i < candidates.size(); i++) {
            byte[] member = candidates.get(i);
            Objects.requireNonNull(member, "member");
            if (containsDuplicateBefore(candidates, i, member)) {
                continue;
            }
            if (!contains(member)) {
                count++;
            }
        }
        return count;
    }

    public int countExistingMembers(List<byte[]> candidates) {
        Objects.requireNonNull(candidates, "candidates");
        int count = 0;
        for (int i = 0; i < candidates.size(); i++) {
            byte[] member = candidates.get(i);
            if (member == null || containsDuplicateBefore(candidates, i, member)) {
                continue;
            }
            if (contains(member)) {
                count++;
            }
        }
        return count;
    }

    public void membersInto(BulkStringSink out) {
        Objects.requireNonNull(out, "out");

        if (members != null) {
            members.forEach((memberRef, ignored) -> out.bulkString(memberStore.slice(memberRef)));
            return;
        }

        for (int i = 0; i < intsetSize; i++) {
            out.bulkStringLongAscii(intsetLongAt(i));
        }
    }

    @Override
    public void forEachNativeHandle(Consumer<NativeHandle> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (members != null) {
            members.forEach((memberRef, ignored) -> consumer.accept(memberRef));
        }
    }

    private boolean addOne(byte[] member) {
        if (member == null) {
            throw new IllegalArgumentException("member must not be null");
        }

        if (members != null) {
            return members.put(member, PRESENT) == null;
        }

        long parsed = parseCanonicalLongOrSentinel(member);
        boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
        if (!isInt) {
            convertToHashSet();
            return members.put(member, PRESENT) == null;
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

        if (members != null) {
            return members.remove(member) != null;
        }

        long parsed = parseCanonicalLongOrSentinel(member);
        boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
        if (!isInt) {
            return false;
        }
        return intsetRemove(parsed);
    }

    private void convertToHashSet() {
        if (members != null) {
            return;
        }
        NativeByteMap<Object> out = new NativeByteMap<>(
                memberStore,
                NativeObjectKind.SET_MEMBER_BYTES,
                hashSeed,
                maintenanceRegistry,
                this::notifyHeapChanged
        );
        boolean ok = false;
        try {
            for (int i = 0; i < intsetSize; i++) {
                byte[] member = Long.toString(intsetLongAt(i)).getBytes(StandardCharsets.US_ASCII);
                out.put(member, PRESENT);
            }
            ok = true;
        } finally {
            if (!ok) {
                out.close();
            }
        }

        this.members = out;
        this.intset16 = null;
        this.intset32 = null;
        this.intset64 = null;
        this.intsetEncodingBytes = 0;
        this.intsetSize = 0;
        notifyHeapChanged();
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
        if (members != null) {
            members.close();
            members = null;
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

    private static long primitiveArrayHeapBytes(int length, int elementBytes) {
        return ARRAY_HEADER_BYTES + (long) length * elementBytes;
    }

    private static long heapUpperBoundForEntryCount(long expectedEntries) {
        if (expectedEntries < 0L) {
            return Long.MAX_VALUE;
        }
        long hashTableBytes = addSaturating(
                FIXED_HEAP_BYTES,
                NativeByteMap.heapUpperBoundForEntries(expectedEntries)
        );
        long intsetBytes = addSaturating(
                FIXED_HEAP_BYTES,
                addSaturating(ARRAY_HEADER_BYTES, multiplySaturating(expectedEntries, Long.BYTES))
        );
        return Math.max(hashTableBytes, intsetBytes);
    }

    private static long candidateCount(List<byte[]> candidates) {
        return candidates == null ? 0L : candidates.size();
    }

    private static long addSaturating(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static long multiplySaturating(long left, long right) {
        if (left == 0L || right == 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private void notifyHeapChanged() {
        heapChangeListener.run();
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

    private static boolean containsDuplicateBefore(List<byte[]> values, int endExclusive, byte[] candidate) {
        for (int i = 0; i < endExclusive; i++) {
            if (Arrays.equals(values.get(i), candidate)) {
                return true;
            }
        }
        return false;
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
