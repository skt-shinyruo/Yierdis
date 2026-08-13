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
        return new Inspection(
                field(lifecycle, "stableMemoryBackend", StableMemoryBackend.class),
                field(lifecycle, "entryTable", EntryTable.class),
                field(lifecycle, "keyDirectory", NativeKeyDirectory.class),
                field(lifecycle, "stringRoot", StringRoot.class),
                field(lifecycle, "listRoot", ListRoot.class),
                field(lifecycle, "hashRoot", HashRoot.class),
                field(lifecycle, "setRoot", SetRoot.class),
                field(lifecycle, "zsetRoot", ZSetRoot.class)
        );
    }

    private static <T> T field(YierdisDbKeyLifecycle lifecycle, String name, Class<T> type) {
        try {
            Field field = YierdisDbKeyLifecycle.class.getDeclaredField(name);
            field.setAccessible(true);
            return type.cast(field.get(lifecycle));
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
