package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.entry.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.api.OffHeapAllocator;

public final class YierdisDbOwnedResources implements AutoCloseable {
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final OffHeapAllocator offHeapAllocator;
    private final boolean ownsMemoryRuntime;
    private final boolean ownsOffHeapAllocator;

    YierdisDbOwnedResources(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            boolean ownsMemoryRuntime,
            boolean ownsOffHeapAllocator
    ) {
        this.memoryRuntime = memoryRuntime;
        this.offHeapAllocator = offHeapAllocator;
        this.ownsMemoryRuntime = ownsMemoryRuntime;
        this.ownsOffHeapAllocator = ownsOffHeapAllocator;
    }

    void clearData(YierdisKeyspace<YierdisObject> store, YierdisExpireIndex expires) {
        clearData(store, expires, null, null);
    }

    void clearData(
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            EntryTable entries,
            NativeKeyDirectory keyDirectory
    ) {
        Throwable failure = null;
        if (store != null) {
            final Throwable[] releaseFailure = new Throwable[1];
            try {
                store.forEach((k, e) -> {
                    try {
                        e.releasePayloadIfAny();
                    } catch (Throwable t) {
                        releaseFailure[0] = recordFailure(releaseFailure[0], t);
                    }
                });
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
            failure = recordFailure(failure, releaseFailure[0]);
            try {
                store.clear();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
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

    void releaseAll(YierdisKeyspace<YierdisObject> store, YierdisExpireIndex expires) {
        releaseAll(store, expires, null, null);
    }

    void releaseAll(
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            EntryTable entries,
            NativeKeyDirectory keyDirectory
    ) {
        releaseAll(store, expires, entries, keyDirectory, null);
    }

    void releaseAll(
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot
    ) {
        releaseAll(store, expires, entries, keyDirectory, stringRoot, null, null, null, null);
    }

    void releaseAll(
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot
    ) {
        releaseAll(store, expires, entries, keyDirectory, stringRoot, listRoot, null, null, null);
    }

    void releaseAll(
            YierdisKeyspace<YierdisObject> store,
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
            clearData(store, expires, entries, keyDirectory);
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
            close();
        } catch (Throwable t) {
            failure = recordFailure(failure, t);
        }
        throwIfFailure(failure);
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
