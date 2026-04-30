package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.ops.ScanCursorV2;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

final class YierdisDbKeyLifecycle {
    private final YierdisKeyspace<YierdisObject> store;
    private final YierdisExpireIndex expires;
    private final OffHeapAllocator offHeapAllocator;
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final Consumer<YierdisObject> touchCallback;
    private final LongConsumer adjustUsedBytesCallback;

    YierdisDbKeyLifecycle(
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            OffHeapAllocator offHeapAllocator,
            YierdisFfmMemoryRuntime memoryRuntime,
            Consumer<YierdisObject> touchCallback,
            LongConsumer adjustUsedBytesCallback
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.expires = Objects.requireNonNull(expires, "expires");
        this.offHeapAllocator = offHeapAllocator;
        this.memoryRuntime = memoryRuntime;
        this.touchCallback = Objects.requireNonNull(touchCallback, "touchCallback");
        this.adjustUsedBytesCallback = Objects.requireNonNull(adjustUsedBytesCallback, "adjustUsedBytesCallback");
    }

    OffHeapAllocator offHeapAllocator() {
        return offHeapAllocator;
    }

    YierdisFfmMemoryRuntime memoryRuntime() {
        return memoryRuntime;
    }

    KeyHandle keyHandle(byte[] keyBytes) {
        return store.keyHandle(keyBytes);
    }

    KeyHandle keyHandle(BytesView keyView) {
        return store.keyHandle(keyView);
    }

    YierdisObject getStoredObject(KeyHandle keyHandle) {
        return store.get(keyHandle);
    }

    YierdisObject getLiveObject(byte[] keyBytes) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle == null) {
            return null;
        }
        return getLiveObject(handle);
    }

    YierdisObject getLiveObject(BytesView keyView) {
        KeyHandle handle = keyHandle(keyView);
        if (handle == null) {
            return null;
        }
        return getLiveObject(handle);
    }

    YierdisObject getLiveObject(KeyHandle keyHandle) {
        YierdisObject object = getStoredObject(keyHandle);
        if (object == null) {
            return null;
        }
        if (removeIfExpired(keyHandle, object, System.currentTimeMillis())) {
            return null;
        }
        touch(object);
        return object;
    }

    YierdisObject computeWithHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super YierdisObject, ? extends YierdisObject> remappingFunction
    ) {
        return store.computeWithHandle(keyBytes, remappingFunction);
    }

    YierdisObject computeIfPresentWithHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super YierdisObject, ? extends YierdisObject> remappingFunction
    ) {
        return store.computeIfPresentWithHandle(keyBytes, remappingFunction);
    }

    boolean remove(byte[] keyBytes, YierdisObject expectedValue) {
        return store.remove(keyBytes, expectedValue);
    }

    boolean remove(KeyHandle keyHandle, YierdisObject expectedValue) {
        return store.remove(keyHandle, expectedValue);
    }

    ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, YierdisKeyspace.ScanConsumer<YierdisObject> consumer) {
        return store.scan(cursor, maxSteps, consumer);
    }

    Long expireAtMillis(KeyHandle keyHandle) {
        return expires.get(keyHandle);
    }

    boolean removeIfExpired(byte[] keyBytes, YierdisObject object, long nowMillis) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle == null) {
            return false;
        }
        return removeIfExpired(handle, object, nowMillis);
    }

    boolean removeIfExpired(KeyHandle keyHandle, YierdisObject object, long nowMillis) {
        Long expireAtMillis = expireAtMillis(keyHandle);
        if (expireAtMillis == null || expireAtMillis > nowMillis) {
            return false;
        }
        removeExpire(keyHandle);
        if (remove(keyHandle, object)) {
            object.releasePayloadIfAny();
            adjustUsedBytesCallback.accept(-object.estimatedBytes);
            return true;
        }
        return false;
    }

    boolean isKeyExpired(KeyHandle keyHandle, long nowMillis) {
        Long expireAtMillis = expireAtMillis(keyHandle);
        return expireAtMillis != null && expireAtMillis <= nowMillis;
    }

    void setExpireAtMillis(byte[] keyBytes, long expireAtMillis) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle != null) {
            setExpireAtMillis(handle, expireAtMillis);
            return;
        }
        expires.setExpireAtMillis(keyBytes, expireAtMillis, store);
    }

    void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        expires.setExpireAtMillis(keyHandle, expireAtMillis);
    }

    void removeExpire(byte[] keyBytes) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle != null) {
            removeExpire(handle);
            return;
        }
        expires.removeExpire(keyBytes);
    }

    void removeExpire(KeyHandle keyHandle) {
        expires.removeExpire(keyHandle);
    }

    void touch(YierdisObject object) {
        touchCallback.accept(object);
    }
}
