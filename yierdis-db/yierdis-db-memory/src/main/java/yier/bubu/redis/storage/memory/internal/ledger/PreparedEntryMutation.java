package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

public final class PreparedEntryMutation<T> extends AbstractPreparedMutation<T> {
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final T result;
    private final EntryRecord oldRecord;
    private final EntryRecord newRecord;
    private final boolean releaseReplacedValue;
    private Runnable releaseReplacedValueHook;
    private final PreparedTtlMutation ttlMutation;
    private AutoCloseable abortResource;
    private Runnable abortNewValueHook;
    private Runnable beforeEntryPublishHook;
    private boolean nativePageTrimRequested;

    private EntryHandle existingEntryHandle;
    private EntryHandle stagedEntryHandle;
    private NativeKeyDirectory.StagedInsert stagedKey;
    private boolean entryPublished;
    private boolean replacedValueReleaseClaimed;

    public static <T> PreparedEntryMutation<T> unchanged(
            YierdisDbKeyLifecycle keyLifecycle,
            T result,
            MutationOutcome outcome
    ) {
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

    public static <T> PreparedEntryMutation<T> insert(
            YierdisDbKeyLifecycle keyLifecycle,
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            EntryHandle stagedEntryHandle,
            NativeKeyDirectory.StagedInsert stagedKey,
            EntryRecord newRecord,
            PreparedTtlMutation ttlMutation
    ) {
        return new PreparedEntryMutation<>(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                outcome,
                null,
                stagedEntryHandle,
                stagedKey,
                null,
                newRecord,
                false,
                ttlMutation
        );
    }

    public static <T> PreparedEntryMutation<T> replace(
            YierdisDbKeyLifecycle keyLifecycle,
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            EntryHandle existingEntryHandle,
            EntryRecord oldRecord,
            EntryRecord newRecord,
            boolean releaseReplacedValue,
            PreparedTtlMutation ttlMutation
    ) {
        return new PreparedEntryMutation<>(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                outcome,
                existingEntryHandle,
                null,
                null,
                oldRecord,
                newRecord,
                releaseReplacedValue,
                ttlMutation
        );
    }

    public static <T> PreparedEntryMutation<T> delete(
            YierdisDbKeyLifecycle keyLifecycle,
            T result,
            long actualDeltaBytes,
            MutationOutcome outcome,
            EntryHandle existingEntryHandle,
            EntryRecord oldRecord,
            boolean releaseReplacedValue,
            PreparedTtlMutation ttlMutation
    ) {
        return new PreparedEntryMutation<>(
                keyLifecycle,
                result,
                actualDeltaBytes,
                0L,
                outcome,
                existingEntryHandle,
                null,
                null,
                oldRecord,
                null,
                releaseReplacedValue,
                ttlMutation
        );
    }

    private PreparedEntryMutation(
            YierdisDbKeyLifecycle keyLifecycle,
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            EntryHandle existingEntryHandle,
            EntryHandle stagedEntryHandle,
            NativeKeyDirectory.StagedInsert stagedKey,
            EntryRecord oldRecord,
            EntryRecord newRecord,
            boolean releaseReplacedValue,
            PreparedTtlMutation ttlMutation
    ) {
        super(
                actualDeltaBytes,
                MemoryUsageSnapshot.addSaturating(
                        Math.max(0L, stagedNonNativeGrowthBytes),
                        ttlMutation == null ? 0L : ttlMutation.stagedNonNativeGrowthBytes()
                ),
                outcome
        );
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.result = result;
        this.existingEntryHandle = existingEntryHandle;
        this.stagedEntryHandle = stagedEntryHandle;
        this.stagedKey = stagedKey;
        this.oldRecord = oldRecord;
        this.newRecord = newRecord;
        this.releaseReplacedValue = releaseReplacedValue;
        this.ttlMutation = ttlMutation == null ? PreparedTtlMutation.NONE : ttlMutation;
    }

    public PreparedEntryMutation<T> releaseReplacedValueWith(Runnable hook) {
        if (releaseReplacedValueHook != null) {
            throw new IllegalStateException("replaced-value release hook is already configured");
        }
        releaseReplacedValueHook = Objects.requireNonNull(hook, "hook");
        return this;
    }

    public PreparedEntryMutation<T> closeOnAbort(AutoCloseable resource) {
        if (abortResource != null) {
            throw new IllegalStateException("abort resource is already configured");
        }
        abortResource = Objects.requireNonNull(resource, "resource");
        return this;
    }

    public PreparedEntryMutation<T> releaseNewValueOnAbortWith(Runnable hook) {
        if (abortNewValueHook != null) {
            throw new IllegalStateException("new-value abort hook is already configured");
        }
        abortNewValueHook = Objects.requireNonNull(hook, "hook");
        return this;
    }

    public PreparedEntryMutation<T> beforeEntryPublish(Runnable hook) {
        if (beforeEntryPublishHook != null) {
            throw new IllegalStateException("before-entry-publish hook is already configured");
        }
        beforeEntryPublishHook = Objects.requireNonNull(hook, "hook");
        return this;
    }

    public PreparedEntryMutation<T> requestNativePageTrimAfterCommit() {
        nativePageTrimRequested = true;
        return this;
    }

    @Override
    public boolean shouldTrimNativePagesAfterCommit() {
        return nativePageTrimRequested || actualDeltaBytes() < 0L;
    }

    @Override
    protected T commitPrepared() {
        boolean deletingEntry = newRecord == null && existingEntryHandle != null;
        if (deletingEntry) {
            ttlMutation.commit();
        }
        if (newRecord != null) {
            if (beforeEntryPublishHook != null) {
                Runnable hook = beforeEntryPublishHook;
                beforeEntryPublishHook = null;
                hook.run();
            }
            if (existingEntryHandle != null) {
                keyLifecycle.replaceEntry(existingEntryHandle, oldRecord, newRecord);
                entryPublished = true;
            } else if (stagedEntryHandle != null && stagedKey != null) {
                keyLifecycle.entryTable().writeReserved(stagedEntryHandle, newRecord);
                keyLifecycle.keyDirectory().publishStagedInsert(stagedKey, stagedEntryHandle);
                stagedKey = null;
                stagedEntryHandle = null;
                entryPublished = true;
            }
        } else if (deletingEntry) {
            keyLifecycle.keyDirectory().remove(existingEntryHandle);
            keyLifecycle.releaseEntry(existingEntryHandle, oldRecord);
            entryPublished = true;
        }
        if (!deletingEntry) {
            ttlMutation.commit();
        }
        return result;
    }

    @Override
    protected void releaseSupersededPrepared() {
        Throwable failure = null;
        try {
            ttlMutation.releaseSuperseded();
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        if (releaseReplacedValueHook != null) {
            Runnable releaseHook = releaseReplacedValueHook;
            releaseReplacedValueHook = null;
            try {
                releaseHook.run();
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            }
        } else if (releaseReplacedValue
                && oldRecord != null
                && (newRecord == null || !sameValue(oldRecord, newRecord))
                && !replacedValueReleaseClaimed) {
            replacedValueReleaseClaimed = true;
            try {
                keyLifecycle.releaseValue(oldRecord);
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            }
        }
        existingEntryHandle = null;
        abortNewValueHook = null;
        abortResource = null;
        if (failure != null) {
            rethrow(failure);
        }
    }

    @Override
    protected void abortPrepared() {
        Throwable failure = null;
        try {
            ttlMutation.abort();
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        if (stagedKey != null) {
            try {
                stagedKey.close();
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            } finally {
                stagedKey = null;
            }
        }
        if (stagedEntryHandle != null) {
            try {
                keyLifecycle.entryTable().release(stagedEntryHandle);
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            } finally {
                stagedEntryHandle = null;
            }
        }
        if (!entryPublished && newRecord != null && !sameValue(oldRecord, newRecord)) {
            if (abortNewValueHook != null) {
                Runnable abortHook = abortNewValueHook;
                abortNewValueHook = null;
                try {
                    abortHook.run();
                } catch (RuntimeException | Error e) {
                    failure = addFailure(failure, e);
                }
            } else {
                try {
                    keyLifecycle.releaseValue(newRecord);
                } catch (RuntimeException | Error e) {
                    failure = addFailure(failure, e);
                }
            }
        }
        if (abortResource != null) {
            try {
                abortResource.close();
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            } catch (Exception e) {
                failure = addFailure(failure, new IllegalStateException("prepared result cleanup failed", e));
            } finally {
                abortResource = null;
            }
        }
        beforeEntryPublishHook = null;
        if (failure != null) {
            rethrow(failure);
        }
    }

    private static boolean sameValue(EntryRecord oldRecord, EntryRecord newRecord) {
        return oldRecord != null
                && newRecord != null
                && oldRecord.type() == newRecord.type()
                && oldRecord.valueHandle() != null
                && oldRecord.valueHandle().equals(newRecord.valueHandle());
    }

    private static Throwable addFailure(Throwable failure, Throwable next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
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
