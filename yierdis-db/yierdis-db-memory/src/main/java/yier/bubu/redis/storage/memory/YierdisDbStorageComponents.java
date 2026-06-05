package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.entry.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;

public final class YierdisDbStorageComponents {
    private static final int ENTRY_TABLE_NATIVE_SLOT_CAPACITY = 64 * 1024;
    private static final int STRING_NATIVE_SLOT_CAPACITY = 64 * 1024;
    private static final int KEY_NATIVE_SLOT_CAPACITY = 64 * 1024;
    private static final int COLLECTION_ROOT_NATIVE_SLOT_CAPACITY = 64 * 1024;

    final YierdisFfmMemoryRuntime memoryRuntime;
    final NativeAllocator nativeAllocator;
    final YierdisDbOwnedResources resources;
    final YierdisExpireIndex expires;
    final EntryTable entries;
    final NativeKeyDirectory keyDirectory;
    final StringRoot stringRoot;
    final ListRoot listRoot;
    final HashRoot hashRoot;
    final SetRoot setRoot;
    final ZSetRoot zsetRoot;

    private YierdisDbStorageComponents(
            YierdisFfmMemoryRuntime memoryRuntime,
            NativeAllocator nativeAllocator,
            YierdisDbOwnedResources resources,
            YierdisExpireIndex expires,
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot
    ) {
        this.memoryRuntime = memoryRuntime;
        this.nativeAllocator = nativeAllocator;
        this.resources = resources;
        this.expires = expires;
        this.entries = entries;
        this.keyDirectory = keyDirectory;
        this.stringRoot = stringRoot;
        this.listRoot = listRoot;
        this.hashRoot = hashRoot;
        this.setRoot = setRoot;
        this.zsetRoot = zsetRoot;
    }

    static YierdisDbStorageComponents create(
            YierdisFfmMemoryRuntime memoryRuntime,
            boolean ownsMemoryRuntime
    ) {
        YierdisFfmMemoryRuntime resolvedRuntime =
                memoryRuntime == null ? new YierdisFfmMemoryRuntime("db") : memoryRuntime;
        boolean resolvedOwnsRuntime = memoryRuntime == null || ownsMemoryRuntime;

        // Entry、key bytes、string bytes 和 collection roots 共享一个 stable allocator，
        // 使 defrag/释放时可以按统一的 native handle 域验证对象类型与存活状态。
        NativeAllocator nativeAllocator = new YierdisStableNativeAllocator(
                resolvedRuntime,
                sharedNativeSlotCapacity()
        );
        YierdisDbOwnedResources resources = new YierdisDbOwnedResources(
                resolvedRuntime,
                nativeAllocator,
                resolvedOwnsRuntime,
                true
        );
        EntryTable entries = new EntryTable(resolvedRuntime, nativeAllocator);
        NativeKeyDirectory keyDirectory = new NativeKeyDirectory(nativeAllocator);
        StringRoot stringRoot = new StringRoot(nativeAllocator);
        ListRoot listRoot = new ListRoot(nativeAllocator);
        HashRoot hashRoot = new HashRoot(nativeAllocator);
        SetRoot setRoot = new SetRoot(resolvedRuntime, nativeAllocator);
        ZSetRoot zsetRoot = new ZSetRoot(resolvedRuntime, nativeAllocator);
        return new YierdisDbStorageComponents(
                resolvedRuntime,
                nativeAllocator,
                resources,
                new YierdisFfmExpireIndex(resolvedRuntime, nativeAllocator),
                entries,
                keyDirectory,
                stringRoot,
                listRoot,
                hashRoot,
                setRoot,
                zsetRoot
        );
    }

    static int sharedNativeSlotCapacity() {
        return ENTRY_TABLE_NATIVE_SLOT_CAPACITY
                + STRING_NATIVE_SLOT_CAPACITY
                + KEY_NATIVE_SLOT_CAPACITY
                + COLLECTION_ROOT_NATIVE_SLOT_CAPACITY;
    }
}
