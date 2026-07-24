package yier.bubu.redis.memory.foreign;

import yier.bubu.redis.memory.api.NativeHandleDomain;

record YierdisNativeObjectMeta(
        long slotId,
        long address,
        int size,
        int capacity,
        int segmentId,
        int pageClass,
        int generation,
        NativeHandleDomain domain,
        int kindCode,
        int flags,
        int pinCount,
        int ownerShardId,
        long allocEpoch,
        long freeEpoch,
        int state
) {
}
