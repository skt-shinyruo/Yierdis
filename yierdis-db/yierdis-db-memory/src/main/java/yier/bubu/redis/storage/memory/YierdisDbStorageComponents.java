package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.expire.YierdisExpireIndex;
import yier.bubu.redis.storage.memory.internal.expire.YierdisNativeExpireIndex;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

final class YierdisDbStorageComponents {
    final StableMemoryBackend stableMemoryBackend;
    final YierdisDbOwnedResources resources;
    final HashTableMaintenanceRegistry hashTableMaintenanceRegistry;
    final YierdisExpireIndex expires;
    final EntryTable entries;
    final NativeKeyDirectory keyDirectory;
    final StringRoot stringRoot;
    final ListRoot listRoot;
    final HashRoot hashRoot;
    final SetRoot setRoot;
    final ZSetRoot zsetRoot;

    private YierdisDbStorageComponents(
            StableMemoryBackend stableMemoryBackend,
            YierdisDbOwnedResources resources,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry,
            YierdisExpireIndex expires,
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot
    ) {
        this.stableMemoryBackend = stableMemoryBackend;
        this.resources = resources;
        this.hashTableMaintenanceRegistry = hashTableMaintenanceRegistry;
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
            StableMemoryBackend stableMemoryBackend,
            HashSeed hashSeed
    ) {
        StableMemoryBackend backend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
        HashSeed resolvedHashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        HashTableMaintenanceRegistry hashTableMaintenanceRegistry = new HashTableMaintenanceRegistry();
        EntryTable entries = new EntryTable(backend);
        NativeKeyDirectory keyDirectory = new NativeKeyDirectory(
                backend,
                resolvedHashSeed,
                hashTableMaintenanceRegistry
        );
        StringRoot stringRoot = new StringRoot(backend);
        ListRoot listRoot = new ListRoot(backend);
        HashRoot hashRoot = new HashRoot(backend, resolvedHashSeed, hashTableMaintenanceRegistry);
        SetRoot setRoot = new SetRoot(backend, resolvedHashSeed, hashTableMaintenanceRegistry);
        ZSetRoot zsetRoot = new ZSetRoot(backend, resolvedHashSeed, hashTableMaintenanceRegistry);
        return new YierdisDbStorageComponents(
                backend,
                new YierdisDbOwnedResources(backend),
                hashTableMaintenanceRegistry,
                new YierdisNativeExpireIndex(backend, resolvedHashSeed, hashTableMaintenanceRegistry),
                entries,
                keyDirectory,
                stringRoot,
                listRoot,
                hashRoot,
                setRoot,
                zsetRoot
        );
    }
}
