package yier.bubu.redis.storage.memory.internal.keyspace;

import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;

public final class TestNativeKeyDirectories {
    private TestNativeKeyDirectories() {
    }

    public static EntryHandle insert(NativeKeyDirectory directory, byte[] key, EntryHandle entry) {
        NativeKeyDirectory.StagedInsert staged = directory.stageInsert(key);
        directory.publishStagedInsert(staged, entry);
        return entry;
    }
}
