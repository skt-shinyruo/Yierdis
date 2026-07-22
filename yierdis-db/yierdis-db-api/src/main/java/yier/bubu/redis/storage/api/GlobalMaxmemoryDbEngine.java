package yier.bubu.redis.storage.api;

import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;

public interface GlobalMaxmemoryDbEngine
        extends RuntimeDbEngine, MaxmemoryParticipant, MaxmemoryCoordinatorAware {
    @Override
    MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis);

    @Override
    MemoryReclaimResult trimMemory(MemoryPressureBudget budget);
}
