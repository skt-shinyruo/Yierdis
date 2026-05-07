package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.storage.api.ScanCursorV2;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

public final class YierdisDbKeyLifecycle {
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

    public OffHeapAllocator offHeapAllocator() {
        return offHeapAllocator;
    }

    public YierdisFfmMemoryRuntime memoryRuntime() {
        return memoryRuntime;
    }

    public KeyHandle keyHandle(byte[] keyBytes) {
        return store.keyHandle(keyBytes);
    }

    public KeyHandle keyHandle(BytesView keyView) {
        return store.keyHandle(keyView);
    }

    public YierdisObject getStoredObject(KeyHandle keyHandle) {
        return store.get(keyHandle);
    }

    public int keyCount() {
        return store.size();
    }

    public int expireCount() {
        return expires.size();
    }

    public KeyHandle randomKeyHandle() {
        return store.randomKeyHandle();
    }

    public KeyHandle randomExpireKeyHandle() {
        return expires.randomKeyHandle();
    }

    public void forEachKeyHandle(java.util.function.BiConsumer<KeyHandle, YierdisObject> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        store.forEachKeyHandle(consumer);
    }

    public Long expireAtMillis(byte[] keyBytes) {
        KeyHandle handle = keyHandle(keyBytes);
        return handle == null ? null : expires.get(handle);
    }

    public YierdisObject getLiveObject(byte[] keyBytes) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle == null) {
            return null;
        }
        return getLiveObject(handle);
    }

    public YierdisObject getLiveObject(BytesView keyView) {
        KeyHandle handle = keyHandle(keyView);
        if (handle == null) {
            return null;
        }
        return getLiveObject(handle);
    }

    public YierdisObject getLiveObject(KeyHandle keyHandle) {
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

    public YierdisObject computeWithHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super YierdisObject, ? extends YierdisObject> remappingFunction
    ) {
        return store.computeWithHandle(keyBytes, remappingFunction);
    }

    public YierdisObject computeIfPresentWithHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super YierdisObject, ? extends YierdisObject> remappingFunction
    ) {
        return store.computeIfPresentWithHandle(keyBytes, remappingFunction);
    }

    public boolean remove(byte[] keyBytes, YierdisObject expectedValue) {
        return store.remove(keyBytes, expectedValue);
    }

    public boolean remove(KeyHandle keyHandle, YierdisObject expectedValue) {
        return store.remove(keyHandle, expectedValue);
    }

    public ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, YierdisKeyspace.ScanConsumer<YierdisObject> consumer) {
        return store.scan(cursor, maxSteps, consumer);
    }

    public Long expireAtMillis(KeyHandle keyHandle) {
        return expires.get(keyHandle);
    }

    public boolean removeObject(KeyHandle keyHandle, YierdisObject object) {
        return store.remove(keyHandle, object);
    }

    public boolean removeIfExpired(byte[] keyBytes, YierdisObject object, long nowMillis) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle == null) {
            return false;
        }
        return removeIfExpired(handle, object, nowMillis);
    }

    public boolean removeIfExpired(KeyHandle keyHandle, YierdisObject object, long nowMillis) {
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

    public boolean isKeyExpired(KeyHandle keyHandle, long nowMillis) {
        Long expireAtMillis = expireAtMillis(keyHandle);
        return expireAtMillis != null && expireAtMillis <= nowMillis;
    }

    public void setExpireAtMillis(byte[] keyBytes, long expireAtMillis) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle != null) {
            setExpireAtMillis(handle, expireAtMillis);
            return;
        }
        expires.setExpireAtMillis(keyBytes, expireAtMillis, store);
    }

    public void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        expires.setExpireAtMillis(keyHandle, expireAtMillis);
    }

    public void removeExpire(byte[] keyBytes) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle != null) {
            removeExpire(handle);
            return;
        }
        expires.removeExpire(keyBytes);
    }

    public void removeExpire(KeyHandle keyHandle) {
        expires.removeExpire(keyHandle);
    }

    public void touch(YierdisObject object) {
        touchCallback.accept(object);
    }
}
