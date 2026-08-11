package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

final class YierdisDbOwnedResources implements AutoCloseable {
    private final StableMemoryBackend stableMemoryBackend;
    private boolean backendCloseAttempted;

    YierdisDbOwnedResources(StableMemoryBackend stableMemoryBackend) {
        this.stableMemoryBackend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
    }

    void clearData(
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot
    ) {
        // 清图发生在关闭后端之前，避免释放值对象时解析到已经失效的稳定句柄。
        Throwable failure = null;
        if (entries != null && keyDirectory != null) {
            Throwable[] entryFailure = new Throwable[1];
            try {
                keyDirectory.forEachEntry((keyHandle, entryHandle) -> {
                    EntryRecord record = null;
                    try {
                        record = entries.get(entryHandle);
                    } catch (Throwable next) {
                        entryFailure[0] = recordFailure(entryFailure[0], next);
                    }
                    try {
                        releaseValue(record, stringRoot, listRoot, hashRoot, setRoot, zsetRoot);
                    } catch (Throwable next) {
                        entryFailure[0] = recordFailure(entryFailure[0], next);
                    }
                    try {
                        entries.release(entryHandle);
                    } catch (Throwable next) {
                        entryFailure[0] = recordFailure(entryFailure[0], next);
                    }
                });
            } catch (Throwable next) {
                entryFailure[0] = recordFailure(entryFailure[0], next);
            }
            failure = recordFailure(failure, entryFailure[0]);
        }
        if (keyDirectory != null) {
            try {
                keyDirectory.clear();
            } catch (Throwable next) {
                failure = recordFailure(failure, next);
            }
        }
        throwIfFailure(failure);
    }

    void releaseAll(
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot
    ) {
        Throwable failure = null;
        try {
            clearData(entries, keyDirectory, stringRoot, listRoot, hashRoot, setRoot, zsetRoot);
        } catch (Throwable next) {
            failure = recordFailure(failure, next);
        }
        failure = closeResource(entries, failure);
        failure = closeResource(keyDirectory, failure);
        failure = closeResource(stringRoot, failure);
        failure = closeResource(listRoot, failure);
        failure = closeResource(hashRoot, failure);
        failure = closeResource(setRoot, failure);
        failure = closeResource(zsetRoot, failure);
        try {
            close();
        } catch (Throwable next) {
            failure = recordFailure(failure, next);
        }
        throwIfFailure(failure);
    }

    void releaseEntry(
            EntryTable entries,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot,
            EntryHandle entryHandle
    ) {
        Objects.requireNonNull(entries, "entries");
        Objects.requireNonNull(entryHandle, "entryHandle");
        Throwable failure = null;
        EntryRecord record = null;
        try {
            record = entries.get(entryHandle);
        } catch (Throwable next) {
            failure = recordFailure(failure, next);
        }
        try {
            releaseValue(record, stringRoot, listRoot, hashRoot, setRoot, zsetRoot);
        } catch (Throwable next) {
            failure = recordFailure(failure, next);
        }
        try {
            entries.release(entryHandle);
        } catch (Throwable next) {
            failure = recordFailure(failure, next);
        }
        throwIfFailure(failure);
    }

    private static Throwable closeResource(AutoCloseable resource, Throwable failure) {
        if (resource == null) {
            return failure;
        }
        try {
            resource.close();
        } catch (Throwable next) {
            return recordFailure(failure, next);
        }
        return failure;
    }

    private static void releaseValue(
            EntryRecord record,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot
    ) {
        if (record == null || record.valueHandle() == null || record.valueHandle().isNull()) {
            return;
        }
        ValueType type = record.type();
        switch (type) {
            case STRING -> stringRoot.release(record.valueHandle());
            case LIST -> listRoot.release(record.valueHandle());
            case HASH -> hashRoot.release(record.valueHandle());
            case SET -> setRoot.release(record.valueHandle());
            case ZSET -> zsetRoot.release(record.valueHandle());
        }
    }

    @Override
    public void close() {
        if (backendCloseAttempted) {
            return;
        }
        // 即使后端关闭抛错，也不能在后续 shutdown 重复释放同一张资源图。
        backendCloseAttempted = true;
        stableMemoryBackend.close();
    }

    private static void throwIfFailure(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException("db resource cleanup failed", failure);
    }

    private static Throwable recordFailure(Throwable current, Throwable next) {
        if (next == null) {
            return current;
        }
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }
}
