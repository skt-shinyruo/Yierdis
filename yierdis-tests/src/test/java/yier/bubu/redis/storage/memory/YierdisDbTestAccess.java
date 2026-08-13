package yier.bubu.redis.storage.memory;

import java.lang.reflect.Field;
import java.util.Objects;
import yier.bubu.redis.memory.api.StableMemoryBackend;

final class YierdisDbTestAccess {
    private YierdisDbTestAccess() {
    }

    static StableMemoryBackend backend(YierdisDb db) {
        YierdisDbKeyLifecycle lifecycle = Objects.requireNonNull(db, "db").keyLifecycle();
        try {
            Field field = YierdisDbKeyLifecycle.class.getDeclaredField("stableMemoryBackend");
            field.setAccessible(true);
            return StableMemoryBackend.class.cast(field.get(lifecycle));
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("unable to inspect key lifecycle backend", failure);
        }
    }
}
