package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.entry.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ValueType;

public final class YierdisDbOwnedResources implements AutoCloseable {
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final NativeAllocator nativeAllocator;
    private final boolean ownsMemoryRuntime;
    private final boolean ownsNativeAllocator;

    YierdisDbOwnedResources(
            YierdisFfmMemoryRuntime memoryRuntime,
            NativeAllocator nativeAllocator,
            boolean ownsMemoryRuntime,
            boolean ownsNativeAllocator
    ) {
        this.memoryRuntime = memoryRuntime;
        this.nativeAllocator = nativeAllocator;
        this.ownsMemoryRuntime = ownsMemoryRuntime;
        this.ownsNativeAllocator = ownsNativeAllocator;
    }

    void clearData(
            YierdisExpireIndex expires,
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot
    ) {
        // 尽量清完整个数据图，再统一抛出首个失败；suppressed 保留其它组件的清理异常。
        Throwable failure = null;
        if (expires != null) {
            try {
                expires.clear();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (entries != null && keyDirectory != null) {
            Throwable[] entryFailure = new Throwable[1];
            try {
                keyDirectory.forEachEntry((keyHandle, entryHandle) -> {
                    EntryRecord record = null;
                    try {
                        record = entries.get(entryHandle);
                    } catch (Throwable t) {
                        entryFailure[0] = recordFailure(entryFailure[0], t);
                    }
                    try {
                        releaseValue(record, stringRoot, listRoot, hashRoot, setRoot, zsetRoot);
                    } catch (Throwable t) {
                        entryFailure[0] = recordFailure(entryFailure[0], t);
                    }
                    try {
                        entries.release(entryHandle);
                    } catch (Throwable t) {
                        entryFailure[0] = recordFailure(entryFailure[0], t);
                    }
                });
            } catch (Throwable t) {
                entryFailure[0] = recordFailure(entryFailure[0], t);
            }
            failure = recordFailure(failure, entryFailure[0]);
        }
        if (keyDirectory != null) {
            try {
                keyDirectory.clear();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        throwIfFailure(failure);
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

    void releaseAll(
            YierdisExpireIndex expires,
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
            // shutdown 先清 key/entry/ttl 图；value roots 稍后 close，确保它们还可以在 allocator 关闭前释放子对象。
            clearData(expires, entries, keyDirectory, stringRoot, listRoot, hashRoot, setRoot, zsetRoot);
        } catch (Throwable t) {
            failure = recordFailure(failure, t);
        }
        if (entries != null) {
            try {
                entries.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (keyDirectory != null) {
            try {
                keyDirectory.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (stringRoot != null) {
            try {
                stringRoot.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (listRoot != null) {
            try {
                listRoot.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (hashRoot != null) {
            try {
                hashRoot.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (setRoot != null) {
            try {
                setRoot.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (zsetRoot != null) {
            try {
                zsetRoot.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        try {
            // allocator/runtime 必须最后关闭，因为上面的 table/root close 仍需要解析并释放 native handle。
            close();
        } catch (Throwable t) {
            failure = recordFailure(failure, t);
        }
        throwIfFailure(failure);
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
        Throwable failure = null;
        if (ownsNativeAllocator && nativeAllocator != null) {
            try {
                nativeAllocator.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (ownsMemoryRuntime && memoryRuntime != null) {
            try {
                memoryRuntime.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (failure != null) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("db resource shutdown failed", failure);
        }
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
