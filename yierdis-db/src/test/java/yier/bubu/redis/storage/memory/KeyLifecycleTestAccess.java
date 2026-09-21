package yier.bubu.redis.storage.memory;

import java.lang.reflect.Field;
import java.util.Objects;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

final class KeyLifecycleTestAccess {
    private KeyLifecycleTestAccess() {
    }

    static StableMemoryBackend backend(YierdisDb db) {
        return inspect(Objects.requireNonNull(db, "db").keyLifecycle()).stableMemoryBackend();
    }

    static Inspection inspect(YierdisDbKeyLifecycle lifecycle) {
        Objects.requireNonNull(lifecycle, "lifecycle");
        Object ownedResources = field(lifecycle, "ownedResources", Object.class);
        return new Inspection(
                field(ownedResources, "stableMemoryBackend", StableMemoryBackend.class),
                field(ownedResources, "entryTable", EntryTable.class),
                field(ownedResources, "keyDirectory", NativeKeyDirectory.class),
                field(ownedResources, "stringRoot", StringRoot.class),
                field(ownedResources, "listRoot", ListRoot.class),
                field(ownedResources, "hashRoot", HashRoot.class),
                field(ownedResources, "setRoot", SetRoot.class),
                field(ownedResources, "zsetRoot", ZSetRoot.class)
        );
    }

    private static <T> T field(Object owner, String name, Class<T> type) {
        try {
            Field field = owner.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return type.cast(field.get(owner));
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("unable to inspect key lifecycle field: " + name, failure);
        }
    }

    record Inspection(
            StableMemoryBackend stableMemoryBackend,
            EntryTable entryTable,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot
    ) {
    }
}
