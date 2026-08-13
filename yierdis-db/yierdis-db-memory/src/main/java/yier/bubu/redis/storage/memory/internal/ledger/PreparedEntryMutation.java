package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.Objects;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.CurrentEntry;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.StagedEntry;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;

public final class PreparedEntryMutation<T> extends AbstractPreparedMutation<T> {
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final T result;
    private final EntryRecord oldRecord;
    private final EntryRecord newRecord;
    private final boolean releaseReplacedValue;
    private Runnable releaseReplacedValueHook;
    private AutoCloseable abortResource;
    private Runnable abortNewValueHook;
    private Runnable beforeEntryPublishHook;
    private boolean nativePageTrimRequested;

    private EntryHandle existingEntryHandle;
    private StagedEntry stagedEntry;
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
                false
        );
    }

    public static <T> PreparedEntryMutation<T> insert(
            YierdisDbKeyLifecycle keyLifecycle,
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            StagedEntry stagedEntry,
            EntryRecord newRecord
    ) {
        return new PreparedEntryMutation<>(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                outcome,
                null,
                stagedEntry,
                null,
                newRecord,
                false
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
            boolean releaseReplacedValue
    ) {
        return new PreparedEntryMutation<>(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                outcome,
                existingEntryHandle,
                null,
                oldRecord,
                newRecord,
                releaseReplacedValue
        );
    }

    public static <T> PreparedEntryMutation<T> delete(
            YierdisDbKeyLifecycle keyLifecycle,
            T result,
            long actualDeltaBytes,
            MutationOutcome outcome,
            EntryHandle existingEntryHandle,
            EntryRecord oldRecord,
            boolean releaseReplacedValue
    ) {
        return new PreparedEntryMutation<>(
                keyLifecycle,
                result,
                actualDeltaBytes,
                0L,
                outcome,
                existingEntryHandle,
                null,
                oldRecord,
                null,
                releaseReplacedValue
        );
    }

    public static <T> PreparedEntryMutation<T> upsert(
            YierdisDbKeyLifecycle keyLifecycle,
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            CurrentEntry current,
            StagedEntry staged,
            EntryRecord newRecord,
            boolean releaseReplacedValue
    ) {
        Objects.requireNonNull(current, "current");
        if (current.record() == null) {
            return insert(
                    keyLifecycle,
                    result,
                    actualDeltaBytes,
                    stagedNonNativeGrowthBytes,
                    outcome,
                    Objects.requireNonNull(staged, "staged"),
                    newRecord
            );
        }
        return replace(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                outcome,
                current.entryHandle(),
                current.record(),
                newRecord,
                releaseReplacedValue
        );
    }

    private PreparedEntryMutation(
            YierdisDbKeyLifecycle keyLifecycle,
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            EntryHandle existingEntryHandle,
            StagedEntry stagedEntry,
            EntryRecord oldRecord,
            EntryRecord newRecord,
            boolean releaseReplacedValue
    ) {
        super(
                actualDeltaBytes,
                Math.max(0L, stagedNonNativeGrowthBytes),
                outcome
        );
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.result = result;
        this.existingEntryHandle = existingEntryHandle;
        this.stagedEntry = stagedEntry;
        this.oldRecord = oldRecord;
        this.newRecord = newRecord;
        this.releaseReplacedValue = releaseReplacedValue;
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
        if (newRecord != null) {
            if (beforeEntryPublishHook != null) {
                Runnable hook = beforeEntryPublishHook;
                beforeEntryPublishHook = null;
                hook.run();
            }
            if (existingEntryHandle != null) {
                keyLifecycle.replaceEntry(existingEntryHandle, oldRecord, newRecord);
                entryPublished = true;
            } else if (stagedEntry != null) {
                keyLifecycle.publishStagedEntry(stagedEntry, newRecord);
                stagedEntry = null;
                entryPublished = true;
            }
        } else if (deletingEntry) {
            keyLifecycle.deleteEntry(existingEntryHandle, oldRecord);
            entryPublished = true;
        }
        return result;
    }

    @Override
    protected void releaseSupersededPrepared() {
        Throwable failure = null;
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
        if (stagedEntry != null) {
            try {
                stagedEntry.close();
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            } finally {
                stagedEntry = null;
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
