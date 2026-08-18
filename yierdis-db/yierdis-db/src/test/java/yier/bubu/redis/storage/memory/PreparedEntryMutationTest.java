package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.CurrentEntry;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.StagedEntry;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public class PreparedEntryMutationTest {
    @Test
    public void factoriesExposeExplicitOperationsAndUpsertResolvesBeforeCommit() {
        YierdisDb db = TestDbSupport.open();
        try {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            CurrentEntry existing = publishEntry(lifecycle, "existing");
            EntryRecord replacement = record(lifecycle, existing, existing.record());

            PreparedEntryMutation<String> unchanged = PreparedEntryMutation.unchanged(lifecycle, "unchanged");
            StagedEntry insertedEntry = lifecycle.stageEntry(bytes("inserted"));
            PreparedEntryMutation<String> insert = PreparedEntryMutation.insert(
                    lifecycle, "insert", 1L, 1L, insertedEntry, record(lifecycle, insertedEntry)
            );
            PreparedEntryMutation<String> replace = PreparedEntryMutation.replace(
                    lifecycle,
                    "replace",
                    0L,
                    0L,
                    existing.entryHandle(),
                    existing.record(),
                    replacement,
                    false
            );
            PreparedEntryMutation<String> delete = PreparedEntryMutation.delete(
                    lifecycle, "delete", -1L, existing.entryHandle(), existing.record(), false
            );

            CurrentEntry missing = lifecycle.currentEntry(bytes("upsert-insert"));
            StagedEntry upsertedEntry = lifecycle.stageEntry(bytes("upsert-insert"));
            PreparedEntryMutation<String> upsertInsert = PreparedEntryMutation.upsert(
                    lifecycle,
                    "upsert-insert",
                    1L,
                    1L,
                    missing,
                    upsertedEntry,
                    record(lifecycle, upsertedEntry),
                    false
            );
            PreparedEntryMutation<String> upsertReplace = PreparedEntryMutation.upsert(
                    lifecycle,
                    "upsert-replace",
                    0L,
                    0L,
                    existing,
                    null,
                    replacement,
                    false
            );

            Assert.assertEquals(PreparedEntryMutation.Operation.UNCHANGED, unchanged.operation());
            Assert.assertEquals(PreparedEntryMutation.Operation.INSERT, insert.operation());
            Assert.assertEquals(PreparedEntryMutation.Operation.REPLACE, replace.operation());
            Assert.assertEquals(PreparedEntryMutation.Operation.DELETE, delete.operation());
            Assert.assertEquals(PreparedEntryMutation.Operation.INSERT, upsertInsert.operation());
            Assert.assertEquals(PreparedEntryMutation.Operation.REPLACE, upsertReplace.operation());

            Assert.assertThrows(
                    NullPointerException.class,
                    () -> PreparedEntryMutation.insert(
                            lifecycle, "invalid", 1L, 1L, null, replacement
                    )
            );
            Assert.assertThrows(
                    NullPointerException.class,
                    () -> PreparedEntryMutation.replace(
                            lifecycle, "invalid", 0L, 0L, null, existing.record(), replacement, false
                    )
            );
            Assert.assertThrows(
                    NullPointerException.class,
                    () -> PreparedEntryMutation.delete(
                            lifecycle, "invalid", -1L, existing.entryHandle(), null, false
                    )
            );

            unchanged.abort();
            insert.abort();
            replace.abort();
            delete.abort();
            upsertInsert.abort();
            upsertReplace.abort();
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void replacementUpsertRejectsStagedEntry() {
        YierdisDb db = TestDbSupport.open();
        try {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            CurrentEntry existing = publishEntry(lifecycle, "existing");
            EntryRecord replacement = record(lifecycle, existing, existing.record());
            StagedEntry unexpectedStagedEntry = lifecycle.stageEntry(bytes("invalid-upsert"));
            try {
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> PreparedEntryMutation.upsert(
                                lifecycle,
                                "invalid",
                                0L,
                                0L,
                                existing,
                                unexpectedStagedEntry,
                                replacement,
                                false
                        )
                );
            } finally {
                unexpectedStagedEntry.close();
            }
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void operationsCommitWithExistingHooksAndTrimSemantics() {
        YierdisDb db = TestDbSupport.open();
        try {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            AtomicInteger beforePublishCalls = new AtomicInteger();
            AtomicInteger releaseCalls = new AtomicInteger();

            PreparedEntryMutation<String> unchanged = PreparedEntryMutation.unchanged(lifecycle, "unchanged");
            Assert.assertEquals("unchanged", unchanged.commit());
            unchanged.releaseSuperseded();

            byte[] key = bytes("entry");
            StagedEntry staged = lifecycle.stageEntry(key);
            EntryRecord insertedRecord = record(lifecycle, staged);
            PreparedEntryMutation<String> insert = PreparedEntryMutation.insert(
                    lifecycle, "insert", 1L, staged.stagedHeapBytes(), staged, insertedRecord
            ).beforeEntryPublish(beforePublishCalls::incrementAndGet);
            Assert.assertFalse(insert.shouldTrimNativePagesAfterCommit());
            Assert.assertEquals("insert", insert.commit());
            insert.releaseSuperseded();
            Assert.assertEquals(insertedRecord, lifecycle.entryRecord(key));

            CurrentEntry inserted = lifecycle.currentEntry(key);
            EntryRecord replacementRecord = record(lifecycle, inserted, inserted.record());
            PreparedEntryMutation<String> replace = PreparedEntryMutation.replace(
                    lifecycle,
                    "replace",
                    0L,
                    0L,
                    inserted.entryHandle(),
                    inserted.record(),
                    replacementRecord,
                    false
            ).beforeEntryPublish(beforePublishCalls::incrementAndGet)
                    .releaseReplacedValueWith(releaseCalls::incrementAndGet)
                    .requestNativePageTrimAfterCommit();
            Assert.assertTrue(replace.shouldTrimNativePagesAfterCommit());
            Assert.assertEquals("replace", replace.commit());
            replace.releaseSuperseded();
            Assert.assertEquals(replacementRecord, lifecycle.entryRecord(key));

            CurrentEntry replaced = lifecycle.currentEntry(key);
            PreparedEntryMutation<String> delete = PreparedEntryMutation.delete(
                    lifecycle,
                    "delete",
                    -1L,
                    replaced.entryHandle(),
                    replaced.record(),
                    false
            ).releaseReplacedValueWith(releaseCalls::incrementAndGet);
            Assert.assertTrue(delete.shouldTrimNativePagesAfterCommit());
            Assert.assertEquals("delete", delete.commit());
            delete.releaseSuperseded();

            Assert.assertNull(lifecycle.entryRecord(key));
            Assert.assertEquals(2, beforePublishCalls.get());
            Assert.assertEquals(2, releaseCalls.get());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void abortReleasesStagedEntryAndConfiguredResourcesWithoutPublishing() {
        YierdisDb db = TestDbSupport.open();
        try {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            long baselineLiveObjects = KeyLifecycleTestAccess.backend(db).stats().liveObjects();
            AtomicInteger newValueReleaseCalls = new AtomicInteger();
            AtomicInteger resultCloseCalls = new AtomicInteger();
            AtomicInteger beforePublishCalls = new AtomicInteger();
            byte[] key = bytes("aborted");
            StagedEntry staged = lifecycle.stageEntry(key);
            PreparedEntryMutation<String> insert = PreparedEntryMutation.insert(
                    lifecycle,
                    "insert",
                    1L,
                    staged.stagedHeapBytes(),
                    staged,
                    record(lifecycle, staged)
            ).releaseNewValueOnAbortWith(newValueReleaseCalls::incrementAndGet)
                    .closeOnAbort(resultCloseCalls::incrementAndGet)
                    .beforeEntryPublish(beforePublishCalls::incrementAndGet);

            insert.abort();

            Assert.assertNull(lifecycle.entryRecord(key));
            Assert.assertEquals(baselineLiveObjects, KeyLifecycleTestAccess.backend(db).stats().liveObjects());
            Assert.assertEquals(1, newValueReleaseCalls.get());
            Assert.assertEquals(1, resultCloseCalls.get());
            Assert.assertEquals(0, beforePublishCalls.get());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void releaseFailureLeavesCommittedReplacementAndDoesNotRetryClaimedHook() {
        YierdisDb db = TestDbSupport.open();
        try {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            CurrentEntry existing = publishEntry(lifecycle, "existing");
            EntryRecord replacement = record(lifecycle, existing, existing.record());
            AtomicInteger releaseCalls = new AtomicInteger();
            PreparedEntryMutation<String> replace = PreparedEntryMutation.replace(
                    lifecycle,
                    "replace",
                    0L,
                    0L,
                    existing.entryHandle(),
                    existing.record(),
                    replacement,
                    false
            ).releaseReplacedValueWith(() -> {
                releaseCalls.incrementAndGet();
                throw new IllegalStateException("release failed");
            });

            Assert.assertEquals("replace", replace.commit());
            Assert.assertThrows(IllegalStateException.class, replace::releaseSuperseded);
            replace.releaseSuperseded();

            Assert.assertEquals(replacement, lifecycle.entryRecord(bytes("existing")));
            Assert.assertEquals(1, releaseCalls.get());
        } finally {
            db.shutdown();
        }
    }

    private static CurrentEntry publishEntry(YierdisDbKeyLifecycle lifecycle, String key) {
        byte[] keyBytes = bytes(key);
        StagedEntry staged = lifecycle.stageEntry(keyBytes);
        lifecycle.publishStagedEntry(staged, record(lifecycle, staged));
        return lifecycle.currentEntry(keyBytes);
    }

    private static EntryRecord record(YierdisDbKeyLifecycle lifecycle, StagedEntry staged) {
        return lifecycle.newRecord(
                staged.keyHandle(),
                ValueHandle.NULL,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                -1L,
                null
        );
    }

    private static EntryRecord record(
            YierdisDbKeyLifecycle lifecycle,
            CurrentEntry current,
            EntryRecord previous
    ) {
        return lifecycle.newRecord(
                current.keyHandle(),
                ValueHandle.NULL,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                -1L,
                previous
        );
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
