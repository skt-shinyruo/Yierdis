package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 与一个已被 Netty 持有的输入缓冲关联的预算额度。
 */
final class InboundBufferLease implements AutoCloseable {
    static final int COMPONENT_OVERHEAD_BYTES = 64;

    private final InboundMemoryBudget budget;
    private final ConnectionMemoryAccount account;
    private final long reservedBytes;
    private final AtomicBoolean closed = new AtomicBoolean();

    private InboundBufferLease(InboundMemoryBudget budget, ConnectionMemoryAccount account, long reservedBytes) {
        this.budget = budget;
        this.account = account;
        this.reservedBytes = reservedBytes;
    }

    static InboundBufferLease admitted(
            InboundMemoryBudget budget,
            ConnectionMemoryAccount account,
            long reservedBytes
    ) {
        if (budget != null && account != null && reservedBytes > 0L) {
            budget.adjustRetainedInputCapacity(reservedBytes);
        }
        return new InboundBufferLease(budget, account, Math.max(0L, reservedBytes));
    }

    static InboundBufferLease unaccounted() {
        return new InboundBufferLease(null, null, 0L);
    }

    static long chargeForCapacity(int capacity) {
        return InboundMemoryBudget.saturatedAdd(Math.max(0, capacity), COMPONENT_OVERHEAD_BYTES);
    }

    static long chargeForRetainedBuffer(ByteBuf buffer) {
        if (buffer == null) {
            return 0L;
        }
        ByteBuf root = buffer;
        while (root.unwrap() != null) {
            root = root.unwrap();
        }
        return chargeForCapacity(root.capacity());
    }

    long reservedBytes() {
        return reservedBytes;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true) || budget == null || account == null || reservedBytes == 0L) {
            return;
        }
        budget.adjustRetainedInputCapacity(-reservedBytes);
        budget.release(account, reservedBytes);
    }
}
