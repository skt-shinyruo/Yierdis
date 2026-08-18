package yier.bubu.redis.protocol.resp.netty;

/**
 * 入站请求内存准入的瞬时统计快照。
 */
public record InboundMemoryBudgetStats(
        long capacityBytes,
        long reservedBytes,
        int waitingConnections,
        boolean backpressured,
        long rejectedConnections,
        long peakReservedBytes,
        long readCreditBytes,
        long retainedInputCapacityBytes,
        long consolidationBytes,
        boolean closed
) {
}
