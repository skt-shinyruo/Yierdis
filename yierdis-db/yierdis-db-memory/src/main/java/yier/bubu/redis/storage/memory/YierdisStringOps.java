package yier.bubu.redis.storage.memory;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringReadOps;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public final class YierdisStringOps implements StringReadOps, StringWriteOps {
    private static final long TTL_ENTRY_BYTES_ESTIMATE = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    private static final int MAX_STRING_BYTES = 512 * 1024 * 1024;
    private static final String INTEGER_RANGE_ERROR = "ERR value is not an integer or out of range";

    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final StringRoot stringRoot;

    YierdisStringOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.stringRoot = Objects.requireNonNull(keyLifecycle.stringRoot(), "stringRoot");
    }

    @Override
    public SetStringResult set(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption, boolean returnOldValue) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        boolean keepTtl = expireOption != null && expireOption.isKeepTtl();
        Long expireAtMillis = (expireOption == null || keepTtl) ? null : expireOption.toExpireAtMillis(now);
        long upperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(
                keyBytes == null ? 0 : keyBytes.length,
                valueLength(value)
        );
        if (expireAtMillis != null) {
            upperBound += TTL_ENTRY_BYTES_ESTIMATE;
        }
        final long finalUpperBound = upperBound;

        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return finalUpperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<SetStringResult> apply() {
                final boolean[] didSet = new boolean[]{false};
                final boolean[] existed = new boolean[]{false};
                final boolean[] removedExpired = new boolean[]{false};
                final KeyHandle[] handleRef = new KeyHandle[]{null};
                final long[] deltaBytes = new long[]{0};
                final byte[][] oldValue = new byte[1][];
                final boolean[] ttlChanged = new boolean[]{false};

                keyLifecycle.computeWithHandle(keyBytes, (k, oldRecord) -> {
                    handleRef[0] = k;
                    EntryRecord current = oldRecord;
                    long oldEstimate = estimateRecordBytes(k, current);
                    if (current != null && keyLifecycle.isKeyExpired(k, now)) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        current = null;
                        oldEstimate = 0L;
                        removedExpired[0] = true;
                    }

                    existed[0] = current != null;
                    if (mode == SetMode.NX && current != null) {
                        return current;
                    }
                    if (mode == SetMode.XX && current == null) {
                        return null;
                    }
                    if (returnOldValue && current != null && current.type() != ValueType.STRING) {
                        throw new WrongTypeException();
                    }
                    if (returnOldValue && current != null) {
                        byte[] raw = copyStringBytes(current);
                        oldValue[0] = raw == null ? null : Arrays.copyOf(raw, raw.length);
                    }

                    StoredString stored = storeSetValue(current, value);
                    long expireForRecord = keepTtl && current != null
                            ? current.expireAtMillis()
                            : expireAtMillis == null ? -1L : expireAtMillis;
                    EntryRecord next = stringRecord(k, stored.handle(), stored.encoding(), expireForRecord, current);

                    deltaBytes[0] -= oldEstimate;
                    deltaBytes[0] += estimateRecordBytes(k, next);
                    didSet[0] = true;
                    return next;
                });

                if (didSet[0]) {
                    KeyHandle keyHandle = currentKeyHandle(keyBytes, handleRef[0]);
                    if (keepTtl && existed[0]) {
                        MutationOutcome outcome = MutationOutcome.VALUE_CHANGED;
                        return YierdisDbMutationExecutor.MutationResult.of(
                                SetStringResult.of(true, oldValue[0], outcome),
                                deltaBytes[0]
                        );
                    }
                    if (expireAtMillis != null) {
                        keyLifecycle.setExpireAtMillis(keyHandle, expireAtMillis);
                        ttlChanged[0] = true;
                        MutationOutcome outcome = MutationOutcome.VALUE_AND_TTL_CHANGED;
                        return YierdisDbMutationExecutor.MutationResult.of(
                                SetStringResult.of(true, oldValue[0], outcome),
                                deltaBytes[0]
                        );
                    }
                    Long beforeTtl = keyLifecycle.expireAtMillis(keyHandle);
                    keyLifecycle.removeExpire(keyHandle);
                    if (beforeTtl != null) {
                        ttlChanged[0] = true;
                    }
                }
                MutationOutcome outcome = MutationOutcome.of(didSet[0], ttlChanged[0]);
                return YierdisDbMutationExecutor.MutationResult.of(
                        SetStringResult.of(didSet[0], oldValue[0], outcome),
                        deltaBytes[0]
                );
            }
        });
    }

    @Override
    public WriteResult<Boolean> setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
        SetStringResult result = set(keyBytes, sliceOf(value), mode, expireOption, false);
        return WriteResult.of(result.applied(), result.mutationOutcome());
    }

    @Override
    public WriteResult<Boolean> setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption) {
        SetStringResult result = set(keyBytes, value, mode, expireOption, false);
        return WriteResult.of(result.applied(), result.mutationOutcome());
    }

    @Override
    public WriteResult<Long> append(byte[] keyBytes, BytesSlice value) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        byte[] suffix = bytesOf(value);
        long upperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(
                keyBytes == null ? 0 : keyBytes.length,
                suffix.length
        );
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                final int[] newLen = new int[]{0};
                final boolean[] changed = new boolean[]{false};
                final boolean[] removedExpired = new boolean[]{false};
                final long[] deltaBytes = new long[]{0};
                keyLifecycle.computeWithHandle(keyBytes, (k, oldRecord) -> {
                    EntryRecord current = oldRecord;
                    long oldEstimate = estimateRecordBytes(k, current);
                    if (current != null && keyLifecycle.isKeyExpired(k, now)) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        current = null;
                        oldEstimate = 0L;
                        removedExpired[0] = true;
                    }
                    if (current == null) {
                        ensureMaxStringLength(suffix.length);
                        ValueHandle handle = stringRoot.store(suffix);
                        EntryRecord next = stringRecord(k, handle, ValueEncoding.STRING_RAW, -1L, null);
                        newLen[0] = suffix.length;
                        changed[0] = true;
                        deltaBytes[0] += estimateRecordBytes(k, next);
                        return next;
                    }

                    requireString(current);
                    ValueHandle handle = requireStringHandle(current);
                    int beforeLen = stringRoot.length(handle);
                    ensureMaxStringLength(Math.addExact(beforeLen, suffix.length));
                    if (suffix.length > 0) {
                        newLen[0] = stringRoot.append(handle, suffix);
                    } else {
                        newLen[0] = beforeLen;
                    }
                    EntryRecord next = stringRecord(k, handle, ValueEncoding.STRING_RAW, current.expireAtMillis(), current);
                    if (newLen[0] != beforeLen) {
                        changed[0] = true;
                    }
                    deltaBytes[0] -= oldEstimate;
                    deltaBytes[0] += estimateRecordBytes(k, next);
                    return next;
                });
                if (changed[0]) {
                    return YierdisDbMutationExecutor.MutationResult.of(
                            WriteResult.of((long) newLen[0], MutationOutcome.VALUE_CHANGED),
                            deltaBytes[0]
                    );
                }
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.unchanged((long) newLen[0]),
                        deltaBytes[0]
                );
            }
        });
    }

    @Override
    public WriteResult<Integer> setBit(byte[] keyBytes, long offset, int value) {
        internals.checkThread();
        validateBitValue(value);
        int requiredLen = requiredBitLength(offset);
        long now = System.currentTimeMillis();
        long currentLen = stringLength(keyBytes);
        long growth = Math.max(0L, (long) requiredLen - currentLen);
        long upperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(
                keyBytes == null ? 0 : keyBytes.length,
                (int) growth
        );
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Integer>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Integer>> apply() {
                final int[] oldBit = new int[]{0};
                final boolean[] changed = new boolean[]{false};
                final boolean[] removedExpired = new boolean[]{false};
                final long[] deltaBytes = new long[]{0};
                keyLifecycle.computeWithHandle(keyBytes, (k, oldRecord) -> {
                    EntryRecord current = oldRecord;
                    long oldEstimate = estimateRecordBytes(k, current);
                    if (current != null && keyLifecycle.isKeyExpired(k, now)) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        current = null;
                        oldEstimate = 0L;
                        removedExpired[0] = true;
                    }

                    ValueHandle handle;
                    boolean existed = current != null;
                    int beforeLen = 0;
                    if (current == null) {
                        handle = stringRoot.store((byte[]) null);
                    } else {
                        requireString(current);
                        handle = requireStringHandle(current);
                        beforeLen = stringRoot.length(handle);
                    }

                    oldBit[0] = getBit(handle, offset);
                    stringRoot.ensureLength(handle, requiredLen);
                    setBit(handle, offset, value);
                    int afterLen = stringRoot.length(handle);
                    if (!existed || oldBit[0] != value || afterLen != beforeLen) {
                        changed[0] = true;
                    }

                    EntryRecord next = stringRecord(
                            k,
                            handle,
                            ValueEncoding.STRING_RAW,
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    deltaBytes[0] -= oldEstimate;
                    deltaBytes[0] += estimateRecordBytes(k, next);
                    return next;
                });
                if (changed[0]) {
                    return YierdisDbMutationExecutor.MutationResult.of(
                            WriteResult.of(oldBit[0], MutationOutcome.VALUE_CHANGED),
                            deltaBytes[0]
                    );
                }
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.unchanged(oldBit[0]),
                        deltaBytes[0]
                );
            }
        });
    }

    @Override
    public WriteResult<Long> incrBy(byte[] keyBytes, long delta) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        long upperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, 32);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                final long[] result = new long[]{0L};
                final long[] deltaBytes = new long[]{0L};
                final boolean[] changed = new boolean[]{false};
                final boolean[] removedExpired = new boolean[]{false};
                keyLifecycle.computeWithHandle(keyBytes, (k, oldRecord) -> {
                    EntryRecord current = oldRecord;
                    long oldEstimate = estimateRecordBytes(k, current);
                    if (current != null && keyLifecycle.isKeyExpired(k, now)) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        current = null;
                        oldEstimate = 0L;
                        removedExpired[0] = true;
                    }

                    long currentValue = 0L;
                    if (current != null) {
                        requireString(current);
                        currentValue = parseLongAscii(copyStringBytes(current));
                    }
                    result[0] = safeAdd(currentValue, delta);
                    byte[] encoded = Long.toString(result[0]).getBytes(StandardCharsets.US_ASCII);
                    ValueHandle handle;
                    if (current != null) {
                        handle = requireStringHandle(current);
                        stringRoot.overwrite(handle, encoded);
                    } else {
                        handle = stringRoot.store(encoded);
                    }
                    EntryRecord next = stringRecord(
                            k,
                            handle,
                            ValueEncoding.STRING_INT,
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    deltaBytes[0] -= oldEstimate;
                    deltaBytes[0] += estimateRecordBytes(k, next);
                    changed[0] = true;
                    return next;
                });
                if (changed[0]) {
                    return YierdisDbMutationExecutor.MutationResult.of(
                            WriteResult.of(result[0], MutationOutcome.VALUE_CHANGED),
                            deltaBytes[0]
                    );
                }
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.unchanged(result[0]),
                        deltaBytes[0]
                );
            }
        });
    }

    @Override
    public byte[] getStringBytes(byte[] keyBytes) {
        internals.checkThread();
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        if (record == null) {
            return null;
        }
        requireString(record);
        return copyStringBytes(record);
    }

    @Override
    public BulkStringValue getStringValue(BytesView keyView) {
        internals.checkThread();
        EntryRecord record = keyLifecycle.liveEntryRecord(keyView);
        if (record == null) {
            return BulkStringValue.nullValue();
        }
        requireString(record);
        if (record.encoding() == ValueEncoding.STRING_INT) {
            return BulkStringValue.longAscii(parseLongAscii(copyStringBytes(record)));
        }
        return BulkStringValue.slice(stringRoot.slice(requireStringHandle(record)));
    }

    @Override
    public long strlen(BytesView keyView) {
        return stringLength(keyView);
    }

    @Override
    public int getBit(BytesView keyView, long offset) {
        internals.checkThread();
        EntryRecord record = keyLifecycle.liveEntryRecord(keyView);
        if (record == null) {
            return 0;
        }
        requireString(record);
        return getBit(requireStringHandle(record), offset);
    }

    @Override
    public long bitcount(BytesView keyView) {
        internals.checkThread();
        EntryRecord record = keyLifecycle.liveEntryRecord(keyView);
        if (record == null) {
            return 0L;
        }
        requireString(record);
        byte[] bytes = copyStringBytes(record);
        if (bytes.length == 0) {
            return 0L;
        }
        return bitcountRange(bytes, 0, bytes.length - 1);
    }

    @Override
    public long bitcount(BytesView keyView, long start, long end) {
        internals.checkThread();
        EntryRecord record = keyLifecycle.liveEntryRecord(keyView);
        if (record == null) {
            return 0L;
        }
        requireString(record);
        byte[] bytes = copyStringBytes(record);
        int len = bytes.length;
        if (len <= 0) {
            return 0L;
        }

        long s = start;
        long ed = end;
        if (s < 0) {
            s = len + s;
        }
        if (ed < 0) {
            ed = len + ed;
        }
        if (s < 0) {
            s = 0;
        }
        if (ed < 0 || s >= len) {
            return 0L;
        }
        if (ed >= len) {
            ed = len - 1L;
        }
        if (s > ed) {
            return 0L;
        }
        return bitcountRange(bytes, (int) s, (int) ed);
    }

    private int stringLength(byte[] keyBytes) {
        internals.checkThread();
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        requireString(record);
        return stringRoot.length(requireStringHandle(record));
    }

    private int stringLength(BytesView keyView) {
        internals.checkThread();
        EntryRecord record = keyLifecycle.liveEntryRecord(keyView);
        if (record == null) {
            return 0;
        }
        requireString(record);
        return stringRoot.length(requireStringHandle(record));
    }

    private StoredString storeSetValue(EntryRecord current, BytesSlice value) {
        byte[] bytes = bytesOf(value);
        Long parsed = tryParseLongForIntEncoding(bytes);
        ValueEncoding encoding = parsed == null ? ValueEncoding.STRING_RAW : ValueEncoding.STRING_INT;
        byte[] storedBytes = parsed == null
                ? bytes
                : Long.toString(parsed).getBytes(StandardCharsets.US_ASCII);
        ValueHandle handle;
        if (current != null && current.type() == ValueType.STRING && stringRoot.contains(current.valueHandle())) {
            handle = current.valueHandle();
            stringRoot.overwrite(handle, storedBytes);
        } else {
            handle = stringRoot.store(storedBytes);
        }
        return new StoredString(handle, encoding);
    }

    private EntryRecord stringRecord(
            KeyHandle keyHandle,
            ValueHandle valueHandle,
            ValueEncoding encoding,
            long expireAtMillis,
            EntryRecord previous
    ) {
        return keyLifecycle.newRecord(
                keyHandle,
                valueHandle,
                ValueType.STRING,
                encoding,
                expireAtMillis,
                stringMetadataBytes(encoding),
                previous
        );
    }

    private ValueHandle requireStringHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!stringRoot.contains(handle)) {
            throw new IllegalStateException("native string value handle is not available: " + (handle == null ? "null" : handle.raw()));
        }
        return handle;
    }

    private byte[] copyStringBytes(EntryRecord record) {
        return stringRoot.copy(requireStringHandle(record));
    }

    private static void requireString(EntryRecord record) {
        if (record.type() != ValueType.STRING) {
            throw new WrongTypeException();
        }
    }

    private long estimateRecordBytes(KeyHandle keyHandle, EntryRecord record) {
        return keyLifecycle.estimatedBytesForRemoval(keyHandle, record);
    }

    private static long stringMetadataBytes(ValueEncoding encoding) {
        return DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE
                + (encoding == ValueEncoding.STRING_INT ? Long.BYTES : 0L);
    }

    private KeyHandle currentKeyHandle(byte[] keyBytes, KeyHandle fallback) {
        KeyHandle current = keyLifecycle.keyHandle(keyBytes);
        return current == null ? fallback : current;
    }

    private static int valueLength(BytesSlice value) {
        return value == null ? 0 : value.length();
    }

    private static byte[] bytesOf(BytesSlice value) {
        if (value == null || value.length() <= 0) {
            return new byte[0];
        }
        byte[] out = new byte[value.length()];
        value.getBytes(0, out, 0, out.length);
        return out;
    }

    private static Long tryParseLongForIntEncoding(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            return parseLongAscii(bytes);
        } catch (YierdisCommandException ignored) {
            return null;
        }
    }

    private static long parseLongAscii(byte[] buf) {
        if (buf == null || buf.length <= 0) {
            throw new YierdisCommandException(INTEGER_RANGE_ERROR);
        }

        int i = 0;
        boolean negative = false;
        byte first = buf[0];
        if (first == '-' || first == '+') {
            negative = first == '-';
            i = 1;
            if (i == buf.length) {
                throw new YierdisCommandException(INTEGER_RANGE_ERROR);
            }
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < buf.length) {
            int digit = buf[i++] - '0';
            if (digit < 0 || digit > 9) {
                throw new YierdisCommandException(INTEGER_RANGE_ERROR);
            }
            if (result < multMin) {
                throw new YierdisCommandException(INTEGER_RANGE_ERROR);
            }
            result *= 10;
            if (result < limit + digit) {
                throw new YierdisCommandException(INTEGER_RANGE_ERROR);
            }
            result -= digit;
        }

        return negative ? result : -result;
    }

    private static long safeAdd(long a, long b) {
        if (b > 0 && a > Long.MAX_VALUE - b) {
            throw new YierdisCommandException(INTEGER_RANGE_ERROR);
        }
        if (b < 0 && a < Long.MIN_VALUE - b) {
            throw new YierdisCommandException(INTEGER_RANGE_ERROR);
        }
        return a + b;
    }

    private static void validateBitValue(int value) {
        if (value != 0 && value != 1) {
            throw new YierdisCommandException("ERR bit is not an integer or out of range");
        }
    }

    private static int requiredBitLength(long offset) {
        int byteIndex = bitByteIndex(offset);
        int requiredLen = byteIndex + 1;
        ensureMaxStringLength(requiredLen);
        return requiredLen;
    }

    private static int bitByteIndex(long offset) {
        if (offset < 0) {
            throw new YierdisCommandException("ERR bit offset is not an integer or out of range");
        }
        long byteIndexLong = offset >>> 3;
        if (byteIndexLong > Integer.MAX_VALUE) {
            throw new YierdisCommandException("ERR bit offset is not an integer or out of range");
        }
        return (int) byteIndexLong;
    }

    private static void ensureMaxStringLength(int requiredLen) {
        if (requiredLen < 0 || requiredLen > MAX_STRING_BYTES) {
            throw new YierdisCommandException("ERR string exceeds maximum allowed size");
        }
    }

    private int getBit(ValueHandle handle, long offset) {
        int byteIndex = bitByteIndex(offset);
        if (byteIndex >= stringRoot.length(handle)) {
            return 0;
        }
        int bit = (int) (offset & 7);
        int mask = 1 << (7 - bit);
        return (stringRoot.byteAt(handle, byteIndex) & mask) == 0 ? 0 : 1;
    }

    private void setBit(ValueHandle handle, long offset, int value) {
        int byteIndex = (int) (offset >>> 3);
        int bit = (int) (offset & 7);
        int mask = 1 << (7 - bit);
        byte oldByte = stringRoot.byteAt(handle, byteIndex);
        byte nextByte = value == 1 ? (byte) (oldByte | mask) : (byte) (oldByte & ~mask);
        if (nextByte != oldByte) {
            stringRoot.setByteAt(handle, byteIndex, nextByte);
        }
    }

    private static long bitcountRange(byte[] bytes, int start, int end) {
        if (bytes == null || start < 0 || end < start) {
            return 0L;
        }
        long count = 0L;
        int to = Math.min(end, bytes.length - 1);
        for (int i = start; i <= to; i++) {
            count += Integer.bitCount(bytes[i] & 0xFF);
        }
        return count;
    }

    private static BytesSlice sliceOf(byte[] value) {
        if (value == null) {
            return null;
        }
        return new BytesSlice() {
            @Override
            public void writeTo(BytesSink out) {
                out.writeBytes(value, 0, value.length);
            }

            @Override
            public int length() {
                return value.length;
            }

            @Override
            public byte getByte(int index) {
                return value[index];
            }
        };
    }

    private record StoredString(ValueHandle handle, ValueEncoding encoding) {
    }
}
