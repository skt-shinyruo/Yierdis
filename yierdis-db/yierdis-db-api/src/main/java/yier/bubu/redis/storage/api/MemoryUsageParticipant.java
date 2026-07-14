package yier.bubu.redis.storage.api;

import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

public interface MemoryUsageParticipant {
    MemoryUsageSnapshot memoryUsage();

    default MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
        return MemoryReclaimResult.empty();
    }
}
