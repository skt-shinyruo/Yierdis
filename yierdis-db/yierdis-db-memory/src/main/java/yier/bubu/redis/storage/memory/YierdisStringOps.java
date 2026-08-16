package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;

import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.CurrentEntry;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.StagedEntry;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringReadOps;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.NativeStorageLayout;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

final class YierdisStringOps implements StringReadOps, StringWriteOps {
    private static final int MAX_STRING_BYTES = 512 * 1024 * 1024;
    private static final int EMBSTR_BYTES_LIMIT = 44;
    private static final String INTEGER_RANGE_ERROR = "ERR value is not an integer or out of range";

    private final YierdisDbKernel kernel;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbMemoryContext memoryContext;
    private final StringRoot stringRoot;

    YierdisStringOps(
            YierdisDbKernel kernel,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbMemoryContext memoryContext,
            StringRoot stringRoot
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
        this.stringRoot = Objects.requireNonNull(stringRoot, "stringRoot");
    }

    @Override
    public WriteResult<SetStringValue> set(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption) {
        return set(MutationContext.none(), keyBytes, value, mode, expireOption);
    }

    WriteResult<SetStringValue> set(
            MutationContext context,
            byte[] keyBytes,
            BytesSlice value,
            SetMode mode,
            ExpireOption expireOption
    ) {
        return setInternal(context, keyBytes, value, mode, expireOption, false);
    }

    private WriteResult<SetStringValue> setInternal(
            MutationContext context,
            byte[] keyBytes,
            BytesSlice value,
            SetMode mode,
            ExpireOption expireOption,
            boolean returnOldValue
    ) {
        kernel.execute(DbUse.ownerCheck());
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        kernel.reclaimExpiredBeforeMutation(keyBytes, now);
        boolean keepTtl = expireOption != null && expireOption.isKeepTtl();
        Long expireAtMillis = (expireOption == null || keepTtl) ? null : expireOption.toExpireAtMillis(now);
        int newValueLength = valueLength(value);
        long upperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(
                keyBytes == null ? 0 : keyBytes.length,
                newValueLength
        );
        final long estimatedUpperBound = upperBound;

        return kernel.execute(new MutationUse<WriteResult<SetStringValue>>() {
            @Override
            public MutationContext context() {
                return context;
            }

            @Override
            public long upperBoundBytes() {
                long nativeUpperBound = addSaturating(
                        setNativeUpperBound(keyBytes, mode, newValueLength, now),
                        setStagedNonNativeGrowthUpperBound(keyBytes, mode, now)
                );
                return withScopeBookkeeping(Math.max(
                        setReservationUpperBound(keyBytes, mode, estimatedUpperBound, now),
                        nativeUpperBound
                ));
            }

            @Override
            public PreparedDbMutation<WriteResult<SetStringValue>> prepare(YierdisDbKernel scope) {
                CurrentEntry currentEntry = keyLifecycle.currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (mode == SetMode.NX && current != null) {
                    WriteResult<SetStringValue> result = WriteResult.unchanged(
                            new SetStringValue(false, ByteValue.nullValue())
                    );
                    return preparedNoEntry(scope, result, MutationOutcome.NONE);
                }
                if (mode == SetMode.XX && current == null) {
                    WriteResult<SetStringValue> result = WriteResult.unchanged(
                            new SetStringValue(false, ByteValue.nullValue())
                    );
                    return preparedNoEntry(scope, result, MutationOutcome.NONE);
                }
                if (returnOldValue && current != null && current.type() != ValueType.STRING) {
                    throw new WrongTypeException();
                }

                StagedEntry staged = null;
                StoredString stored = null;
                ByteValue oldValue = ByteValue.nullValue();
                boolean oldValueOwnedByPreparedMutation = false;
                AtomicBoolean releaseOldValueOnClose = null;
                Runnable releaseOldValueHook = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = keyLifecycle.stageEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }

                    stored = storeSetValue(current, value);
                    long expireForRecord = keepTtl && current != null
                            ? current.expireAtMillis()
                            : expireAtMillis == null ? -1L : expireAtMillis;
                    EntryRecord next = stringRecord(targetKey, stored.handle(), stored.encoding(), expireForRecord, current);
                    boolean ttlChanged = ttlChangedForSet(current, keepTtl, expireAtMillis);
                    if (returnOldValue && current != null) {
                        releaseOldValueOnClose = new AtomicBoolean();
                        AtomicBoolean releaseOnClose = releaseOldValueOnClose;
                        oldValue = stringRoot.retainedValueWithCloseHook(
                                requireStringHandle(current),
                                () -> {
                                    if (releaseOnClose.get()) {
                                        keyLifecycle.releaseValue(current);
                                    }
                                }
                        );
                        releaseOldValueHook = () -> releaseOnClose.set(true);
                    }
                    MutationOutcome outcome = MutationOutcome.of(true, ttlChanged);
                    WriteResult<SetStringValue> result = WriteResult.of(
                            new SetStringValue(true, oldValue),
                            outcome
                    );
                    long deltaBytes = estimateRecordBytes(targetKey, next) - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<SetStringValue>> prepared = scope.upsert(
                            result,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            outcome,
                            currentEntry,
                            staged,
                            next,
                            !returnOldValue
                    );
                    if (releaseOldValueHook != null) {
                        prepared.releaseReplacedValueWith(releaseOldValueHook);
                    }
                    prepared.closeOnAbort(oldValue);
                    if (replacementCanReleaseNativePages(current, stored)) {
                        prepared.requestNativePageTrimAfterCommit();
                    }
                    oldValueOwnedByPreparedMutation = true;
                    staged = null;
                    stored = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    if (!oldValueOwnedByPreparedMutation) {
                        closeReplyValue(oldValue, failure);
                    }
                    abortStaged(staged, stored, failure);
                    throw failure;
                }
            }
        });
    }

    @Override
    public PreparedMutation<SetStringValue> prepareSet(
            byte[] keyBytes,
            BytesSlice value,
            SetMode mode,
            ExpireOption expireOption,
            boolean returnOldValue
    ) {
        kernel.execute(DbUse.ownerCheck());
        Objects.requireNonNull(keyBytes, "keyBytes");
        byte[] preparedKey = Arrays.copyOf(keyBytes, keyBytes.length);
        byte[] preparedValue = bytesOf(value);
        PreparedEntryState state = preparedEntryState(preparedKey);
        EntryRecord current = state.liveRecord();
        boolean currentIsNonString = current != null && current.type() != ValueType.STRING;
        if (returnOldValue && currentIsNonString) {
            throw new WrongTypeException();
        }
        boolean applied = (mode != SetMode.NX || current == null)
                && (mode != SetMode.XX || current != null);
        ByteValue oldValue = current != null
                && current.type() == ValueType.STRING
                && returnOldValue
                ? stringRoot.retainedValue(requireStringHandle(current))
                : ByteValue.nullValue();
        return new PreparedSetMutation(
                preparedKey,
                preparedValue,
                mode,
                expireOption,
                state,
                new SetStringValue(applied, oldValue)
        );
    }

    @Override
    public WriteResult<Boolean> setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
        return setString(MutationContext.none(), keyBytes, value, mode, expireOption);
    }

    WriteResult<Boolean> setString(
            MutationContext context,
            byte[] keyBytes,
            byte[] value,
            SetMode mode,
            ExpireOption expireOption
    ) {
        WriteResult<SetStringValue> result = set(context, keyBytes, sliceOf(value), mode, expireOption);
        return WriteResult.of(result.value().applied(), result.mutationOutcome());
    }

    @Override
    public WriteResult<Boolean> setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption) {
        return setString(MutationContext.none(), keyBytes, value, mode, expireOption);
    }

    WriteResult<Boolean> setString(
            MutationContext context,
            byte[] keyBytes,
            BytesSlice value,
            SetMode mode,
            ExpireOption expireOption
    ) {
        WriteResult<SetStringValue> result = set(context, keyBytes, value, mode, expireOption);
        return WriteResult.of(result.value().applied(), result.mutationOutcome());
    }

    @Override
    public WriteResult<Long> append(byte[] keyBytes, BytesSlice value) {
        return append(MutationContext.none(), keyBytes, value);
    }

    WriteResult<Long> append(MutationContext context, byte[] keyBytes, BytesSlice value) {
        kernel.execute(DbUse.ownerCheck());
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        kernel.reclaimExpiredBeforeMutation(keyBytes, now);
        byte[] suffix = bytesOf(value);
        long estimatedUpperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(
                keyBytes == null ? 0 : keyBytes.length,
                suffix.length
        );
        return kernel.execute(new MutationUse<WriteResult<Long>>() {
            @Override
            public MutationContext context() {
                return context;
            }

            @Override
            public long upperBoundBytes() {
                int replacementLength = Math.addExact(
                        currentStringLengthForEstimate(keyBytes, now),
                        suffix.length
                );
                return withScopeBookkeeping(Math.max(
                        estimatedUpperBound,
                        stringGrowthNativeUpperBound(keyBytes, replacementLength, now, true)
                ));
            }

            @Override
            public PreparedDbMutation<WriteResult<Long>> prepare(YierdisDbKernel scope) {
                CurrentEntry currentEntry = keyLifecycle.currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                StagedEntry staged = null;
                StoredString stored = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        ensureMaxStringLength(suffix.length);
                        staged = keyLifecycle.stageEntry(keyBytes);
                        targetKey = staged.keyHandle();
                        stored = new StoredString(stringRoot.store(suffix), ValueEncoding.STRING_RAW);
                    } else {
                        requireString(current);
                        byte[] before = copyStringBytes(current);
                        int beforeLen = before.length;
                        if (suffix.length == 0) {
                            return preparedNoEntry(
                                    scope,
                                    WriteResult.unchanged((long) beforeLen),
                                    MutationOutcome.NONE
                            );
                        }
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
                    PreparedEntryMutation<WriteResult<Long>> prepared = scope.upsert(
                            result,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            outcome,
                            currentEntry,
                            staged,
                            next,
                            true
                    );
                    if (replacementCanReleaseNativePages(current, stored)) {
                        prepared.requestNativePageTrimAfterCommit();
                    }
                    staged = null;
                    stored = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    if (current != null && stored != null && current.valueHandle().equals(stored.handle())) {
                        stored = null;
                    }
                    abortStaged(staged, stored, failure);
                    throw failure;
                }
            }
        });
    }

    @Override
    public WriteResult<Integer> setBit(byte[] keyBytes, long offset, int value) {
        return setBit(MutationContext.none(), keyBytes, offset, value);
    }

    WriteResult<Integer> setBit(MutationContext context, byte[] keyBytes, long offset, int value) {
        kernel.execute(DbUse.ownerCheck());
        Objects.requireNonNull(keyBytes, "keyBytes");
        validateBitValue(value);
        int requiredLen = requiredBitLength(offset);
        long now = System.currentTimeMillis();
        kernel.reclaimExpiredBeforeMutation(keyBytes, now);
        long currentLen = currentStringLengthForEstimate(keyBytes, now);
        long growth = Math.max(0L, (long) requiredLen - currentLen);
        long estimatedUpperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(
                keyBytes == null ? 0 : keyBytes.length,
                (int) growth
        );
        return kernel.execute(new MutationUse<WriteResult<Integer>>() {
            @Override
            public MutationContext context() {
                return context;
            }

            @Override
            public long upperBoundBytes() {
                return withScopeBookkeeping(Math.max(
                        estimatedUpperBound,
                        stringGrowthNativeUpperBound(keyBytes, requiredLen, now, true)
                ));
            }

            @Override
            public PreparedDbMutation<WriteResult<Integer>> prepare(YierdisDbKernel scope) {
                CurrentEntry currentEntry = keyLifecycle.currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                StagedEntry staged = null;
                StoredString stored = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    boolean existed = current != null;
                    byte[] before;
                    if (current == null) {
                        staged = keyLifecycle.stageEntry(keyBytes);
                        targetKey = staged.keyHandle();
                        before = new byte[0];
                    } else {
                        requireString(current);
                        before = copyStringBytes(current);
                    }

                    int oldBit = getBit(before, offset);
                    if (existed && oldBit == value && before.length >= requiredLen) {
                        return preparedNoEntry(scope, WriteResult.unchanged(oldBit), MutationOutcome.NONE);
                    }
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
                    PreparedEntryMutation<WriteResult<Integer>> prepared = scope.upsert(
                            result,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            outcome,
                            currentEntry,
                            staged,
                            next,
                            true
                    );
                    if (replacementCanReleaseNativePages(current, stored)) {
                        prepared.requestNativePageTrimAfterCommit();
                    }
                    staged = null;
                    stored = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    abortStaged(staged, stored, failure);
                    throw failure;
                }
            }
        });
    }

    @Override
    public WriteResult<Long> incrBy(byte[] keyBytes, long delta) {
        return incrBy(MutationContext.none(), keyBytes, delta);
    }

    WriteResult<Long> incrBy(MutationContext context, byte[] keyBytes, long delta) {
        kernel.execute(DbUse.ownerCheck());
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        kernel.reclaimExpiredBeforeMutation(keyBytes, now);
        long estimatedUpperBound = YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, 32);
        return kernel.execute(new MutationUse<WriteResult<Long>>() {
            @Override
            public MutationContext context() {
                return context;
            }

            @Override
            public long upperBoundBytes() {
                return withScopeBookkeeping(Math.max(
                        estimatedUpperBound,
                        stringGrowthNativeUpperBound(keyBytes, 32, now, true)
                ));
            }

            @Override
            public PreparedDbMutation<WriteResult<Long>> prepare(YierdisDbKernel scope) {
                CurrentEntry currentEntry = keyLifecycle.currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                StagedEntry staged = null;
                StoredString stored = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    long currentValue = 0L;
                    if (current != null) {
                        requireString(current);
                        currentValue = parseLongAscii(copyStringBytes(current));
                    } else {
                        staged = keyLifecycle.stageEntry(keyBytes);
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
                    PreparedEntryMutation<WriteResult<Long>> prepared = scope.upsert(
                            writeResult,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            MutationOutcome.VALUE_CHANGED,
                            currentEntry,
                            staged,
                            next,
                            true
                    );
                    if (replacementCanReleaseNativePages(current, stored)) {
                        prepared.requestNativePageTrimAfterCommit();
                    }
                    staged = null;
                    stored = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    abortStaged(staged, stored, failure);
                    throw failure;
                }
            }
        });
    }

    @Override
    public byte[] getStringBytes(byte[] keyBytes) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveTouchedStringRecord(scope, keyBytes);
            if (record == null) {
                return null;
            }
            return copyStringBytes(record);
        }));
    }

    @Override
    public ByteValue getStringValue(BytesView keyView) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveTouchedStringRecord(scope, keyView);
            if (record == null) {
                return ByteValue.nullValue();
            }
            return stringRoot.retainedValue(requireStringHandle(record));
        }));
    }

    @Override
    public long strlen(BytesView keyView) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveTouchedStringRecord(scope, keyView);
            if (record == null) {
                return 0L;
            }
            return (long) stringRoot.length(requireStringHandle(record));
        }));
    }

    @Override
    public int getBit(BytesView keyView, long offset) {
        return kernel.execute(DbUse.read(scope -> {
            bitByteIndex(offset);
            EntryRecord record = liveTouchedStringRecord(scope, keyView);
            if (record == null) {
                return 0;
            }
            return getBit(requireStringHandle(record), offset);
        }));
    }

    @Override
    public long bitcount(BytesView keyView) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveTouchedStringRecord(scope, keyView);
            if (record == null) {
                return 0L;
            }
            byte[] bytes = copyStringBytes(record);
            if (bytes.length == 0) {
                return 0L;
            }
            return bitcountRange(bytes, 0, bytes.length - 1);
        }));
    }

    @Override
    public long bitcount(BytesView keyView, long start, long end) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveTouchedStringRecord(scope, keyView);
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
        }));
    }

    private EntryRecord liveTouchedStringRecord(YierdisDbKernel scope, byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        return liveTouchedStringRecord(scope, keyHandle);
    }

    private EntryRecord liveTouchedStringRecord(YierdisDbKernel scope, BytesView keyView) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyView);
        return liveTouchedStringRecord(scope, keyHandle);
    }

    private EntryRecord liveTouchedStringRecord(YierdisDbKernel scope, KeyHandle keyHandle) {
        EntryRecord record = scope.liveEntryRecord(keyHandle);
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
                previous
        );
    }

    private ValueHandle requireStringHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!stringRoot.contains(handle)) {
            throw new IllegalStateException("native string value handle is not available: " + (handle == null ? "null" : handle.nativeHandle()));
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

    private boolean replacementCanReleaseNativePages(EntryRecord current, StoredString replacement) {
        if (current == null || replacement == null) {
            return false;
        }
        if (current.type() != ValueType.STRING) {
            return true;
        }
        ValueHandle currentHandle = requireStringHandle(current);
        return !currentHandle.equals(replacement.handle())
                && stringRoot.estimatedBytes(currentHandle) > stringRoot.estimatedBytes(replacement.handle());
    }

    private PreparedEntryState preparedEntryState(byte[] keyBytes) {
        EntryHandle entryHandle = keyLifecycle.entryHandle(keyBytes);
        EntryRecord record = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        boolean expired = record != null
                && keyHandle != null
                && keyLifecycle.isKeyExpired(keyHandle, System.currentTimeMillis());
        return new PreparedEntryState(entryHandle, record, record == null ? 0L : record.version(), expired);
    }

    private boolean ttlChangedForSet(
            EntryRecord current,
            boolean keepTtl,
            Long expireAtMillis
    ) {
        Long before = current == null || current.expireAtMillis() < 0L ? null : current.expireAtMillis();
        if (keepTtl && current != null) {
            return false;
        }
        if (expireAtMillis != null) {
            return true;
        }
        return before != null;
    }

    private static <T> PreparedDbMutation<T> preparedNoEntry(
            YierdisDbKernel scope,
            T result,
            MutationOutcome outcome
    ) {
        return scope.unchanged(result, outcome);
    }

    private void abortStaged(
            StagedEntry staged,
            StoredString stored,
            Throwable failure
    ) {
        if (stored != null) {
            try {
                stringRoot.release(stored.handle());
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
        }
        keyLifecycle.abortStagedEntry(staged, failure);
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

    private long setReservationUpperBound(byte[] keyBytes, SetMode mode, long newValueUpperBound, long nowMillis) {
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

        long nextEstimate = newValueUpperBound;
        if (current.type() != ValueType.STRING || !stringRoot.contains(current.valueHandle())) {
            return nextEstimate;
        }

        long oldEstimate = estimateRecordBytes(keyHandle, current)
                + (keyBytes == null ? 0L : Math.max(0L, (long) keyBytes.length))
                + Math.max(0L, (long) stringRoot.length(current.valueHandle()));
        if (nextEstimate <= oldEstimate) {
            return 0L;
        }
        return nextEstimate - oldEstimate;
    }

    private long setNativeUpperBound(byte[] keyBytes, SetMode mode, int valueLength, long nowMillis) {
        EntryRecord current = keyLifecycle.entryRecord(keyBytes);
        if (current == null) {
            return mode == SetMode.XX ? 0L : nativePeak(
                    keyLength(keyBytes),
                    NativeStorageLayout.ENTRY_RECORD_BYTES,
                    valueLength
            );
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
            long nowMillis
    ) {
        EntryRecord current = keyLifecycle.entryRecord(keyBytes);
        if (current == null) {
            return mode == SetMode.XX ? 0L : keyLifecycle.estimatedInsertHeapGrowthBytes();
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return mode == SetMode.XX ? 0L : keyLifecycle.estimatedInsertHeapGrowthBytes();
        }
        return 0L;
    }

    private long newStringCreateUpperBound(byte[] keyBytes, int valueLength) {
        return addSaturating(
                newStringNativeUpperBound(keyBytes, valueLength),
                keyLifecycle.estimatedInsertHeapGrowthBytes()
        );
    }

    private long newStringNativeUpperBound(byte[] keyBytes, int valueLength) {
        return nativePeak(keyLength(keyBytes), NativeStorageLayout.ENTRY_RECORD_BYTES, valueLength);
    }

    private long nativePeak(int... nativeAllocationSizes) {
        return memoryContext.nativeAllocationPeakAdditionalBytes(
                0L,
                0L,
                nativeAllocationSizes
        );
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

    private long withScopeBookkeeping(long upperBound) {
        return Math.max(
                Math.max(0L, upperBound),
                memoryContext.nativeAllocationScopeBookkeepingBytes(0)
        );
    }

    private static int valueLength(BytesSlice value) {
        return value == null ? 0 : value.length();
    }

    private static void closeReplyValue(ByteValue value, Throwable failure) {
        try {
            value.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
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

    private record PreparedEntryState(
            EntryHandle entryHandle,
            EntryRecord record,
            long version,
            boolean expired
    ) {
        private EntryRecord liveRecord() {
            return expired ? null : record;
        }
    }

    private final class PreparedSetMutation implements PreparedMutation<SetStringValue> {
        private final byte[] keyBytes;
        private final byte[] valueBytes;
        private final SetMode mode;
        private final ExpireOption expireOption;
        private final PreparedEntryState expectedState;
        private final SetStringValue preview;
        private boolean closed;
        private boolean committed;
        private boolean trimNativePagesAfterClose;

        private PreparedSetMutation(
                byte[] keyBytes,
                byte[] valueBytes,
                SetMode mode,
                ExpireOption expireOption,
                PreparedEntryState expectedState,
                SetStringValue preview
        ) {
            this.keyBytes = keyBytes;
            this.valueBytes = valueBytes;
            this.mode = mode;
            this.expireOption = expireOption;
            this.expectedState = expectedState;
            this.preview = preview;
        }

        @Override
        public SetStringValue preview() {
            return preview;
        }

        @Override
        public boolean isCurrent() {
            kernel.execute(DbUse.ownerCheck());
            PreparedEntryState current = preparedEntryState(keyBytes);
            return Objects.equals(expectedState.entryHandle(), current.entryHandle())
                    && expectedState.version() == current.version()
                    && expectedState.expired() == current.expired();
        }

        @Override
        public MutationOutcome commit(MutationContext context) {
            kernel.execute(DbUse.ownerCheck());
            Objects.requireNonNull(context, "context");
            requireCommittable();
            WriteResult<SetStringValue> result = set(
                    context,
                    keyBytes,
                    sliceOf(valueBytes),
                    mode,
                    expireOption
            );
            committed = true;
            MutationOutcome outcome = result.mutationOutcome();
            trimNativePagesAfterClose = outcome.changedAny() && expectedState.liveRecord() != null;
            return outcome;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            preview.close();
            if (trimNativePagesAfterClose) {
                memoryContext.trimEmptyNativePagesAfterPreparedPreviewClose();
            }
        }

        private void requireCommittable() {
            if (closed) {
                throw new IllegalStateException("prepared mutation is closed");
            }
            if (committed) {
                throw new IllegalStateException("prepared mutation is already committed");
            }
            if (!isCurrent()) {
                throw new IllegalStateException("prepared mutation is stale");
            }
        }
    }

}
