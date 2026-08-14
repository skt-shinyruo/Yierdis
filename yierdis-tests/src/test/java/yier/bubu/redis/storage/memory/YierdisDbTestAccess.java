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
            Field ownedResourcesField = YierdisDbKeyLifecycle.class.getDeclaredField("ownedResources");
            ownedResourcesField.setAccessible(true);
            Object ownedResources = ownedResourcesField.get(lifecycle);
            Field backendField = ownedResources.getClass().getDeclaredField("stableMemoryBackend");
            backendField.setAccessible(true);
            return StableMemoryBackend.class.cast(backendField.get(ownedResources));
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("unable to inspect key lifecycle backend", failure);
        }
    }
}
