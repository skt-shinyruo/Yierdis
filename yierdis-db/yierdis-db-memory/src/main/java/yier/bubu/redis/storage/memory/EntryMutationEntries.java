package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;

final class EntryMutationEntries {
    private EntryMutationEntries() {
    }

    static CurrentEntry current(YierdisDbKeyLifecycle keyLifecycle, byte[] keyBytes) {
        EntryHandle entryHandle = keyLifecycle.entryHandle(keyBytes);
        EntryRecord record = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
        KeyHandle keyHandle = entryHandle == null ? null : keyLifecycle.keyHandle(keyBytes);
        return new CurrentEntry(entryHandle, keyHandle, record);
    }

    static StagedEntry stage(YierdisDbKeyLifecycle keyLifecycle, byte[] keyBytes) {
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

    static <T> PreparedEntryMutation<T> upsert(
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
        if (current.record() == null) {
            StagedEntry inserted = Objects.requireNonNull(staged, "staged");
            return PreparedEntryMutation.insert(
                    keyLifecycle,
                    result,
                    actualDeltaBytes,
                    stagedNonNativeGrowthBytes,
                    outcome,
                    inserted.entryHandle(),
                    inserted.stagedKey(),
                    newRecord
            );
        }
        return PreparedEntryMutation.replace(
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

    static void abortStaged(
            YierdisDbKeyLifecycle keyLifecycle,
            StagedEntry staged,
            Throwable failure
    ) {
        if (staged == null) {
            return;
        }
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

    record CurrentEntry(
            EntryHandle entryHandle,
            KeyHandle keyHandle,
            EntryRecord record
    ) {
    }

    record StagedEntry(
            EntryHandle entryHandle,
            NativeKeyDirectory.StagedInsert stagedKey
    ) implements AutoCloseable {
        KeyHandle keyHandle() {
            return stagedKey.keyHandle();
        }

        long stagedHeapBytes() {
            return stagedKey.stagedHeapBytes();
        }

        @Override
        public void close() {
            stagedKey.close();
        }
    }
}
