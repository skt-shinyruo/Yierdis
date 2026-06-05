package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.NativeHandle;

import java.util.function.Consumer;

interface NativeHandleOwner {
    void forEachNativeHandle(Consumer<NativeHandle> consumer);
}
