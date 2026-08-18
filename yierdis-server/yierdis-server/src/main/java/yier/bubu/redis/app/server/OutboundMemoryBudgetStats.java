package yier.bubu.redis.app.server;

/**
 * 出站回复容量的不可变快照。
 */
public record OutboundMemoryBudgetStats(
        long capacityBytes,
        long reservedBytes,
        long allocatedBytes,
        long peakReservedBytes,
        long peakAllocatedBytes,
        long capacityRejectedReservations,
        int waitingConnections,
        int activeConnections,
        long activeSlots,
        boolean closed
) {
}
