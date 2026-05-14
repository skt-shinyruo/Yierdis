package yier.bubu.redis.memory.foreign;

import yier.bubu.redis.memory.api.NativeHandle;

@FunctionalInterface
interface YierdisNativeDefragValidator {
    void validate(NativeHandle handle, YierdisNativeObjectMeta sourceMeta, YierdisNativeBlock target);
}
