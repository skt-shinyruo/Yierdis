package yier.bubu.redis.memory.foreign;

@FunctionalInterface
interface YierdisNativeDefragValidator {
    void validate(long localRaw, YierdisNativeObjectMeta sourceMeta, YierdisNativeBlock target);
}
