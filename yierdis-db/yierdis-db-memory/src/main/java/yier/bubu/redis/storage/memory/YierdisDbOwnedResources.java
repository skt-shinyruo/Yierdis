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
import yier.bubu.redis.memory.api.OffHeapAllocator;

public final class YierdisDbOwnedResources implements AutoCloseable {
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final OffHeapAllocator offHeapAllocator;
    private final NativeAllocator nativeAllocator;
    private final boolean ownsMemoryRuntime;
    private final boolean ownsOffHeapAllocator;
    private final boolean ownsNativeAllocator;

    YierdisDbOwnedResources(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            NativeAllocator nativeAllocator,
            boolean ownsMemoryRuntime,
            boolean ownsOffHeapAllocator,
            boolean ownsNativeAllocator
    ) {
        this.memoryRuntime = memoryRuntime;
        this.offHeapAllocator = offHeapAllocator;
        this.nativeAllocator = nativeAllocator;
        this.ownsMemoryRuntime = ownsMemoryRuntime;
        this.ownsOffHeapAllocator = ownsOffHeapAllocator;
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
        if (entries != null) {
            try {
                entries.clear();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (keyDirectory != null) {
            try {
                keyDirectory.clear();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        failure = clearRoot(failure, stringRoot);
        failure = clearRoot(failure, listRoot);
        failure = clearRoot(failure, hashRoot);
        failure = clearRoot(failure, setRoot);
        failure = clearRoot(failure, zsetRoot);
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
            clearData(expires, entries, keyDirectory, null, null, null, null, null);
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

    private static Throwable clearRoot(Throwable failure, TypeRoot root) {
        if (root == null) {
            return failure;
        }
        try {
            root.clear();
        } catch (Throwable t) {
            return recordFailure(failure, t);
        }
        return failure;
    }

    @Override
    public void close() {
        Throwable failure = null;
        if (ownsOffHeapAllocator && offHeapAllocator != null) {
            try {
                offHeapAllocator.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
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
