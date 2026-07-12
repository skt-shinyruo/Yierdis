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
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.value.NativeBytesSlice;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public final class YierdisStringOps implements StringReadOps, StringWriteOps {
    private static final long TTL_ENTRY_BYTES_ESTIMATE = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    private static final int ENTRY_RECORD_NATIVE_BYTES = 56;
    private static final int MAX_STRING_BYTES = 512 * 1024 * 1024;
    private static final int EMBSTR_BYTES_LIMIT = 44;
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
    public WriteResult<SetStringValue> set(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption, boolean returnOldValue) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        boolean keepTtl = expireOption != null && expireOption.isKeepTtl();
        Long expireAtMillis = (expireOption == null || keepTtl) ? null : expireOption.toExpireAtMillis(now);
        int newValueLength = valueLength(value);
        long upperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(
                keyBytes == null ? 0 : keyBytes.length,
                newValueLength
        );
        if (expireAtMillis != null) {
            upperBound = addSaturating(upperBound, TTL_ENTRY_BYTES_ESTIMATE);
        }
        final long estimatedUpperBound = upperBound;

        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                long nativeUpperBound = addSaturating(
                        setNativeUpperBound(keyBytes, mode, newValueLength, now),
                        setStagedNonNativeGrowthUpperBound(keyBytes, mode, now, expireAtMillis)
                );
                return withScopeBookkeeping(Math.max(
                        setReservationUpperBound(keyBytes, mode, estimatedUpperBound, now, keepTtl),
                        nativeUpperBound
                ));
            }

            @Override
            public PreparedEntryMutation<WriteResult<SetStringValue>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null && keyLifecycle.isKeyExpired(currentEntry.keyHandle(), now)) {
                    keyLifecycle.removeIfExpired(currentEntry.keyHandle(), current, now);
                    currentEntry = currentEntry(keyBytes);
                    current = currentEntry.record();
                }

                if (mode == SetMode.NX && current != null) {
                    WriteResult<SetStringValue> result = WriteResult.unchanged(
                            new SetStringValue(false, BulkStringValue.nullValue())
                    );
                    return preparedNoEntry(result, MutationOutcome.NONE);
                }
                if (mode == SetMode.XX && current == null) {
                    WriteResult<SetStringValue> result = WriteResult.unchanged(
                            new SetStringValue(false, BulkStringValue.nullValue())
                    );
                    return preparedNoEntry(result, MutationOutcome.NONE);
                }
                if (returnOldValue && current != null && current.type() != ValueType.STRING) {
                    throw new WrongTypeException();
                }

                StagedEntry staged = null;
                StoredString stored = null;
                PreparedTtlMutation ttlMutation = PreparedTtlMutation.NONE;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }

                    BulkStringValue oldValue = BulkStringValue.nullValue();
                    boolean transferOldValue = false;
                    if (returnOldValue && current != null) {
                        ValueHandle oldHandle = requireStringHandle(current);
                        int payloadLength = stringRoot.length(oldHandle);
                        oldValue = BulkStringValue.owned(
                                new NativeBytesSlice(keyLifecycle.nativeAllocator(), oldHandle.nativeHandle(), 0, payloadLength),
                                payloadLength,
                                stringRoot.estimatedBytes(oldHandle),
                                () -> stringRoot.release(oldHandle)
                        );
                        transferOldValue = true;
                    }

                    stored = storeSetValue(current, value);
                    long expireForRecord = keepTtl && current != null
                            ? current.expireAtMillis()
                            : expireAtMillis == null ? -1L : expireAtMillis;
                    EntryRecord next = stringRecord(targetKey, stored.handle(), stored.encoding(), expireForRecord, current);
                    ttlMutation = setTtlMutation(targetKey, current, keepTtl, expireAtMillis);
                    boolean ttlChanged = ttlChangedForSet(targetKey, current, keepTtl, expireAtMillis);
                    MutationOutcome outcome = MutationOutcome.of(true, ttlChanged);
                    WriteResult<SetStringValue> result = WriteResult.of(
                            new SetStringValue(true, oldValue),
                            outcome
                    );
                    long deltaBytes = estimateRecordBytes(targetKey, next) - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<SetStringValue>> prepared = new PreparedEntryMutation<>(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            outcome,
                            currentEntry.entryHandle(),
                            staged == null ? null : staged.entryHandle(),
                            staged == null ? null : staged.stagedKey(),
                            current,
                            next,
                            !transferOldValue,
                            ttlMutation
                    );
                    staged = null;
                    stored = null;
                    ttlMutation = PreparedTtlMutation.NONE;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    abortStaged(staged, stored, ttlMutation, failure);
                    throw failure;
                }
            }
        });
    }

    @Override
    public WriteResult<Boolean> setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
        WriteResult<SetStringValue> result = set(keyBytes, sliceOf(value), mode, expireOption, false);
        return WriteResult.of(result.value().applied(), result.mutationOutcome());
    }

    @Override
    public WriteResult<Boolean> setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption) {
        WriteResult<SetStringValue> result = set(keyBytes, value, mode, expireOption, false);
        return WriteResult.of(result.value().applied(), result.mutationOutcome());
    }

    @Override
    public WriteResult<Long> append(byte[] keyBytes, BytesSlice value) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        byte[] suffix = bytesOf(value);
        long estimatedUpperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(
                keyBytes == null ? 0 : keyBytes.length,
                suffix.length
        );
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return withScopeBookkeeping(Math.max(
                        estimatedUpperBound,
                        stringGrowthNativeUpperBound(keyBytes, suffix.length, now, true)
                ));
            }

            @Override
            public PreparedEntryMutation<WriteResult<Long>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null && keyLifecycle.isKeyExpired(currentEntry.keyHandle(), now)) {
                    keyLifecycle.removeIfExpired(currentEntry.keyHandle(), current, now);
                    currentEntry = currentEntry(keyBytes);
                    current = currentEntry.record();
                }

                StagedEntry staged = null;
                StoredString stored = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        ensureMaxStringLength(suffix.length);
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                        stored = new StoredString(stringRoot.store(suffix), ValueEncoding.STRING_RAW);
                    } else {
                        requireString(current);
                        byte[] before = copyStringBytes(current);
                        int beforeLen = before.length;
                        ensureMaxStringLength(Math.addExact(beforeLen, suffix.length));
                        int newLen = beforeLen + suffix.length;
                        ValueHandle handle = requireStringHandle(current);
                        if (suffix.length > 0) {
                            byte[] replacement = Arrays.copyOf(before, newLen);
                            System.arraycopy(suffix, 0, replacement, beforeLen, suffix.length);
                            handle = stringRoot.store(replacement);
                        }
                        stored = new StoredString(handle, ValueEncoding.STRING_RAW);
                    }
                    int newLen = current == null ? suffix.length : stringRoot.length(stored.handle());
                    int beforeLen = current == null ? -1 : stringRoot.length(requireStringHandle(current));
                    EntryRecord next = stringRecord(
                            targetKey,
                            stored.handle(),
                            stored.encoding(),
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    WriteResult<Long> result = newLen != beforeLen
                            ? WriteResult.of((long) newLen, MutationOutcome.VALUE_CHANGED)
                            : WriteResult.unchanged((long) newLen);
                    MutationOutcome outcome = result.mutationOutcome();
                    long deltaBytes = estimateRecordBytes(targetKey, next) - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<Long>> prepared = new PreparedEntryMutation<>(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            outcome,
                            currentEntry.entryHandle(),
                            staged == null ? null : staged.entryHandle(),
                            staged == null ? null : staged.stagedKey(),
                            current,
                            next,
                            true,
                            PreparedTtlMutation.NONE
                    );
                    staged = null;
                    stored = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    if (current != null && stored != null && current.valueHandle().equals(stored.handle())) {
                        stored = null;
                    }
                    abortStaged(staged, stored, PreparedTtlMutation.NONE, failure);
                    throw failure;
                }
            }
        });
    }

    @Override
    public WriteResult<Integer> setBit(byte[] keyBytes, long offset, int value) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        validateBitValue(value);
        int requiredLen = requiredBitLength(offset);
        long now = System.currentTimeMillis();
        long currentLen = currentStringLengthForEstimate(keyBytes, now);
        long growth = Math.max(0L, (long) requiredLen - currentLen);
        long estimatedUpperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(
                keyBytes == null ? 0 : keyBytes.length,
                (int) growth
        );
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Integer>>() {
            @Override
            public long upperBoundBytes() {
                return withScopeBookkeeping(Math.max(
                        estimatedUpperBound,
                        stringGrowthNativeUpperBound(keyBytes, requiredLen, now, true)
                ));
            }

            @Override
            public PreparedEntryMutation<WriteResult<Integer>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null && keyLifecycle.isKeyExpired(currentEntry.keyHandle(), now)) {
                    keyLifecycle.removeIfExpired(currentEntry.keyHandle(), current, now);
                    currentEntry = currentEntry(keyBytes);
                    current = currentEntry.record();
                }

                StagedEntry staged = null;
                StoredString stored = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    boolean existed = current != null;
                    byte[] before;
                    if (current == null) {
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                        before = new byte[0];
                    } else {
                        requireString(current);
                        before = copyStringBytes(current);
                    }

                    int oldBit = getBit(before, offset);
                    byte[] replacement = before.length >= requiredLen
                            ? Arrays.copyOf(before, before.length)
                            : Arrays.copyOf(before, requiredLen);
                    setBitInArray(replacement, offset, value);
                    stored = new StoredString(stringRoot.store(replacement), ValueEncoding.STRING_RAW);
                    int afterLen = replacement.length;
                    boolean changed = !existed || oldBit != value || afterLen != before.length;

                    EntryRecord next = stringRecord(
                            targetKey,
                            stored.handle(),
                            stored.encoding(),
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    WriteResult<Integer> result = changed
                            ? WriteResult.of(oldBit, MutationOutcome.VALUE_CHANGED)
                            : WriteResult.unchanged(oldBit);
                    MutationOutcome outcome = result.mutationOutcome();
                    long deltaBytes = estimateRecordBytes(targetKey, next) - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<Integer>> prepared = new PreparedEntryMutation<>(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            outcome,
                            currentEntry.entryHandle(),
                            staged == null ? null : staged.entryHandle(),
                            staged == null ? null : staged.stagedKey(),
                            current,
                            next,
                            true,
                            PreparedTtlMutation.NONE
                    );
                    staged = null;
                    stored = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    abortStaged(staged, stored, PreparedTtlMutation.NONE, failure);
                    throw failure;
                }
            }
        });
    }

    @Override
    public WriteResult<Long> incrBy(byte[] keyBytes, long delta) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        long estimatedUpperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, 32);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return withScopeBookkeeping(Math.max(
                        estimatedUpperBound,
                        stringGrowthNativeUpperBound(keyBytes, 32, now, true)
                ));
            }

            @Override
            public PreparedEntryMutation<WriteResult<Long>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null && keyLifecycle.isKeyExpired(currentEntry.keyHandle(), now)) {
                    keyLifecycle.removeIfExpired(currentEntry.keyHandle(), current, now);
                    currentEntry = currentEntry(keyBytes);
                    current = currentEntry.record();
                }

                StagedEntry staged = null;
                StoredString stored = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    long currentValue = 0L;
                    if (current != null) {
                        requireString(current);
                        currentValue = parseLongAscii(copyStringBytes(current));
                    } else {
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }
                    long result = safeAdd(currentValue, delta);
                    byte[] encoded = Long.toString(result).getBytes(StandardCharsets.US_ASCII);
                    stored = new StoredString(stringRoot.store(encoded), ValueEncoding.STRING_INT);
                    EntryRecord next = stringRecord(
                            targetKey,
                            stored.handle(),
                            stored.encoding(),
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    WriteResult<Long> writeResult = WriteResult.of(result, MutationOutcome.VALUE_CHANGED);
                    long deltaBytes = estimateRecordBytes(targetKey, next) - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<Long>> prepared = new PreparedEntryMutation<>(
                            keyLifecycle,
                            writeResult,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            MutationOutcome.VALUE_CHANGED,
                            currentEntry.entryHandle(),
                            staged == null ? null : staged.entryHandle(),
                            staged == null ? null : staged.stagedKey(),
                            current,
                            next,
                            true,
                            PreparedTtlMutation.NONE
                    );
                    staged = null;
                    stored = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    abortStaged(staged, stored, PreparedTtlMutation.NONE, failure);
                    throw failure;
                }
            }
        });
    }

    @Override
    public byte[] getStringBytes(byte[] keyBytes) {
        internals.checkThread();
        EntryRecord record = liveTouchedStringRecord(keyBytes);
        if (record == null) {
            return null;
        }
        return copyStringBytes(record);
    }

    @Override
    public BulkStringValue getStringValue(BytesView keyView) {
        internals.checkThread();
        EntryRecord record = liveTouchedStringRecord(keyView);
        if (record == null) {
            return BulkStringValue.nullValue();
        }
        if (record.encoding() == ValueEncoding.STRING_INT) {
            return BulkStringValue.longAscii(parseLongAscii(copyStringBytes(record)));
        }
        return BulkStringValue.slice(stringRoot.slice(requireStringHandle(record)));
    }

    @Override
    public long strlen(BytesView keyView) {
        internals.checkThread();
        EntryRecord record = liveTouchedStringRecord(keyView);
        if (record == null) {
            return 0;
        }
        return stringRoot.length(requireStringHandle(record));
    }

    @Override
    public int getBit(BytesView keyView, long offset) {
        internals.checkThread();
        EntryRecord record = liveTouchedStringRecord(keyView);
        if (record == null) {
            return 0;
        }
        return getBit(requireStringHandle(record), offset);
    }

    @Override
    public long bitcount(BytesView keyView) {
        internals.checkThread();
        EntryRecord record = liveTouchedStringRecord(keyView);
        if (record == null) {
            return 0L;
        }
        byte[] bytes = copyStringBytes(record);
        if (bytes.length == 0) {
            return 0L;
        }
        return bitcountRange(bytes, 0, bytes.length - 1);
    }

    @Override
    public long bitcount(BytesView keyView, long start, long end) {
        internals.checkThread();
        EntryRecord record = liveTouchedStringRecord(keyView);
        if (record == null) {
            return 0L;
        }
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

    private EntryRecord liveTouchedStringRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        return liveTouchedStringRecord(keyHandle);
    }

    private EntryRecord liveTouchedStringRecord(BytesView keyView) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyView);
        return liveTouchedStringRecord(keyHandle);
    }

    private EntryRecord liveTouchedStringRecord(KeyHandle keyHandle) {
        EntryRecord record = keyLifecycle.liveEntryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        requireString(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private StoredString storeSetValue(EntryRecord current, BytesSlice value) {
        byte[] bytes = bytesOf(value);
        Long parsed = tryParseLongForIntEncoding(bytes);
        ValueEncoding encoding = parsed == null ? stringObjectEncoding(bytes) : ValueEncoding.STRING_INT;
        byte[] storedBytes = parsed == null
                ? bytes
                : Long.toString(parsed).getBytes(StandardCharsets.US_ASCII);
        ValueHandle handle = stringRoot.store(storedBytes);
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

    private CurrentEntry currentEntry(byte[] keyBytes) {
        EntryHandle entryHandle = keyLifecycle.entryHandle(keyBytes);
        EntryRecord record = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
        KeyHandle keyHandle = entryHandle == null ? null : keyLifecycle.keyHandle(keyBytes);
        return new CurrentEntry(entryHandle, keyHandle, record);
    }

    private StagedEntry stageNewEntry(byte[] keyBytes) {
        EntryHandle entryHandle = keyLifecycle.entryTable().reserve();
        NativeKeyDirectory.StagedInsert stagedKey = null;
        try {
            stagedKey = keyLifecycle.keyDirectory().stageInsert(keyBytes);
            return new StagedEntry(entryHandle, stagedKey);
        } catch (RuntimeException | Error failure) {
            try {
                keyLifecycle.entryTable().release(entryHandle);
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
            if (stagedKey != null) {
                try {
                    stagedKey.close();
                } catch (RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    private PreparedTtlMutation setTtlMutation(
            KeyHandle keyHandle,
            EntryRecord current,
            boolean keepTtl,
            Long expireAtMillis
    ) {
        if (keepTtl && current != null) {
            return PreparedTtlMutation.NONE;
        }
        if (expireAtMillis != null) {
            return keyLifecycle.prepareSetExpireAtMillis(keyHandle, expireAtMillis);
        }
        return keyLifecycle.prepareRemoveExpire(keyHandle);
    }

    private boolean ttlChangedForSet(
            KeyHandle keyHandle,
            EntryRecord current,
            boolean keepTtl,
            Long expireAtMillis
    ) {
        Long before = current == null ? null : keyLifecycle.expireAtMillis(keyHandle);
        if (keepTtl && current != null) {
            return false;
        }
        if (expireAtMillis != null) {
            return true;
        }
        return before != null;
    }

    private <T> PreparedEntryMutation<T> preparedNoEntry(T result, MutationOutcome outcome) {
        return new PreparedEntryMutation<>(
                keyLifecycle,
                result,
                0L,
                0L,
                outcome,
                null,
                null,
                null,
                null,
                null,
                false,
                PreparedTtlMutation.NONE
        );
    }

    private void abortStaged(
            StagedEntry staged,
            StoredString stored,
            PreparedTtlMutation ttlMutation,
            Throwable failure
    ) {
        if (ttlMutation != null) {
            try {
                ttlMutation.abort();
            } catch (RuntimeException | Error abortFailure) {
                failure.addSuppressed(abortFailure);
            }
        }
        if (stored != null) {
            try {
                stringRoot.release(stored.handle());
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
        }
        if (staged != null) {
            try {
                staged.close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            try {
                keyLifecycle.entryTable().release(staged.entryHandle());
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
        }
    }

    private int currentStringLengthForEstimate(byte[] keyBytes, long nowMillis) {
        EntryRecord current = keyLifecycle.entryRecord(keyBytes);
        if (current == null) {
            return 0;
        }
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return 0;
        }
        requireString(current);
        return stringRoot.length(requireStringHandle(current));
    }

    private long setReservationUpperBound(byte[] keyBytes, SetMode mode, long newValueUpperBound, long nowMillis, boolean keepTtl) {
        EntryRecord current = keyLifecycle.entryRecord(keyBytes);
        if (current == null) {
            return mode == SetMode.XX ? 0L : newValueUpperBound;
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return mode == SetMode.XX ? 0L : newValueUpperBound;
        }
        if (mode == SetMode.NX) {
            return 0L;
        }

        boolean currentHasTtl = keyLifecycle.expireAtMillis(keyHandle) != null;
        long nextEstimate = keepTtl && currentHasTtl
                ? addSaturating(newValueUpperBound, TTL_ENTRY_BYTES_ESTIMATE)
                : newValueUpperBound;
        if (current.type() != ValueType.STRING || !stringRoot.contains(current.valueHandle())) {
            return nextEstimate;
        }

        long oldEstimate = estimateRecordBytes(keyHandle, current)
                + (keyBytes == null ? 0L : Math.max(0L, (long) keyBytes.length))
                + Math.max(0L, (long) stringRoot.length(current.valueHandle()));
        if (currentHasTtl) {
            oldEstimate = addSaturating(oldEstimate, TTL_ENTRY_BYTES_ESTIMATE);
        }

        if (nextEstimate <= oldEstimate) {
            return 0L;
        }
        return nextEstimate - oldEstimate;
    }

    private long setNativeUpperBound(byte[] keyBytes, SetMode mode, int valueLength, long nowMillis) {
        EntryRecord current = keyLifecycle.entryRecord(keyBytes);
        if (current == null) {
            return mode == SetMode.XX ? 0L : nativePeak(keyLength(keyBytes), ENTRY_RECORD_NATIVE_BYTES, valueLength);
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return mode == SetMode.XX ? 0L : newStringNativeUpperBound(keyBytes, valueLength);
        }
        if (mode == SetMode.NX) {
            return 0L;
        }
        return replacementValueNativeUpperBound(current, valueLength);
    }

    private long stringGrowthNativeUpperBound(
            byte[] keyBytes,
            int requiredValueLength,
            long nowMillis,
            boolean createWhenMissing
    ) {
        EntryRecord current = keyLifecycle.entryRecord(keyBytes);
        if (current == null) {
            return createWhenMissing ? newStringCreateUpperBound(keyBytes, requiredValueLength) : 0L;
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return createWhenMissing ? newStringCreateUpperBound(keyBytes, requiredValueLength) : 0L;
        }
        return replacementValueNativeUpperBound(current, requiredValueLength);
    }

    private long replacementValueNativeUpperBound(EntryRecord current, int requiredValueLength) {
        return nativePeak(requiredValueLength);
    }

    private long setStagedNonNativeGrowthUpperBound(
            byte[] keyBytes,
            SetMode mode,
            long nowMillis,
            Long expireAtMillis
    ) {
        EntryRecord current = keyLifecycle.entryRecord(keyBytes);
        if (current == null) {
            return mode == SetMode.XX ? 0L : newEntryStagedNonNativeGrowth(expireAtMillis, true);
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            boolean addingNewTtl = expireAtMillis != null && keyLifecycle.expireAtMillis(keyHandle) == null;
            return mode == SetMode.XX ? 0L : newEntryStagedNonNativeGrowth(expireAtMillis, addingNewTtl);
        }
        if (mode == SetMode.NX || expireAtMillis == null) {
            return 0L;
        }

        boolean addingNewTtl = keyLifecycle.expireAtMillis(keyHandle) == null;
        return keyLifecycle.estimateExpireSetNonNativeGrowthBytes(addingNewTtl);
    }

    private long newStringCreateUpperBound(byte[] keyBytes, int valueLength) {
        return addSaturating(
                newStringNativeUpperBound(keyBytes, valueLength),
                keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes()
        );
    }

    private long newEntryStagedNonNativeGrowth(Long expireAtMillis, boolean addingNewTtl) {
        long growth = keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes();
        if (expireAtMillis != null) {
            growth = addSaturating(
                    growth,
                    keyLifecycle.estimateExpireSetNonNativeGrowthBytes(addingNewTtl)
            );
        }
        return growth;
    }

    private long newStringNativeUpperBound(byte[] keyBytes, int valueLength) {
        return nativePeak(keyLength(keyBytes), ENTRY_RECORD_NATIVE_BYTES, valueLength);
    }

    private long nativePeak(int... nativeAllocationSizes) {
        return MutationMemoryEstimator.peakAdditionalBytes(
                keyLifecycle.nativeAllocator(),
                0L,
                0L,
                nativeAllocationSizes
        );
    }

    private static long stringMetadataBytes(ValueEncoding encoding) {
        return DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE
                + (encoding == ValueEncoding.STRING_INT ? Long.BYTES : 0L);
    }

    private static int keyLength(byte[] keyBytes) {
        return keyBytes == null ? 0 : keyBytes.length;
    }

    private static long addSaturating(long left, long right) {
        if (right <= 0) {
            return left;
        }
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long withScopeBookkeeping(long upperBound) {
        return Math.max(
                Math.max(0L, upperBound),
                MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes()
        );
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
            long parsed = parseLongAscii(bytes);
            byte[] canonical = Long.toString(parsed).getBytes(StandardCharsets.US_ASCII);
            return Arrays.equals(bytes, canonical) ? parsed : null;
        } catch (YierdisCommandException ignored) {
            return null;
        }
    }

    private static ValueEncoding stringObjectEncoding(byte[] bytes) {
        return bytes != null && bytes.length <= EMBSTR_BYTES_LIMIT
                ? ValueEncoding.STRING_EMBSTR
                : ValueEncoding.STRING_RAW;
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

    private static int getBit(byte[] bytes, long offset) {
        int byteIndex = bitByteIndex(offset);
        if (byteIndex >= bytes.length) {
            return 0;
        }
        int bit = (int) (offset & 7);
        int mask = 1 << (7 - bit);
        return (bytes[byteIndex] & mask) == 0 ? 0 : 1;
    }

    private static void setBitInArray(byte[] bytes, long offset, int value) {
        int byteIndex = (int) (offset >>> 3);
        int bit = (int) (offset & 7);
        int mask = 1 << (7 - bit);
        bytes[byteIndex] = value == 1
                ? (byte) (bytes[byteIndex] | mask)
                : (byte) (bytes[byteIndex] & ~mask);
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

    private record CurrentEntry(
            EntryHandle entryHandle,
            KeyHandle keyHandle,
            EntryRecord record
    ) {
    }

    private record StagedEntry(
            EntryHandle entryHandle,
            NativeKeyDirectory.StagedInsert stagedKey
    ) implements AutoCloseable {
        private KeyHandle keyHandle() {
            return stagedKey.keyHandle();
        }

        private long stagedHeapBytes() {
            return stagedKey.stagedHeapBytes();
        }

        @Override
        public void close() {
            Throwable failure = null;
            try {
                stagedKey.close();
            } catch (RuntimeException | Error e) {
                failure = e;
            }
            if (failure != null) {
                rethrow(failure);
            }
        }
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
        throw new IllegalStateException(failure);
    }
}
