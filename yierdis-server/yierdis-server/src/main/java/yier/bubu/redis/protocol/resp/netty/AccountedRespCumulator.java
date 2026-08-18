package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * 显式拥有 RESP 分片及其租约，只有组件真的移除时才归还其完整保留容量。
 */
final class AccountedRespCumulator implements AutoCloseable {
    enum ConsolidationResult {
        NOT_NEEDED,
        CONSOLIDATED,
        WAITING,
        REQUEST_LIMIT
    }

    private final ByteBufAllocator allocator;
    private final InboundMemoryBudget budget;
    private final InboundConnectionMemory connection;
    private final int maxComponents;
    private final ArrayDeque<Component> components = new ArrayDeque<>();
    private CompositeByteBuf cumulation;
    private PendingConsolidation pendingConsolidation;

    AccountedRespCumulator(
            ByteBufAllocator allocator,
            InboundMemoryBudget budget,
            InboundConnectionMemory connection,
            int maxComponents
    ) {
        this.allocator = allocator;
        this.budget = budget;
        this.connection = connection;
        this.maxComponents = Math.max(2, maxComponents);
        this.cumulation = allocator.compositeBuffer(Integer.MAX_VALUE);
    }

    ByteBuf buffer() {
        return cumulation;
    }

    void append(ByteBuf input, InboundBufferLease lease) {
        if (input == null) {
            lease.close();
            return;
        }
        int readable = input.readableBytes();
        if (readable == 0) {
            ReferenceCountUtil.safeRelease(input);
            lease.close();
            return;
        }
        boolean added = false;
        try {
            cumulation.addComponent(true, input);
            components.addLast(new Component(readable, lease));
            added = true;
        } finally {
            if (!added) {
                ReferenceCountUtil.safeRelease(input);
                lease.close();
            }
        }
    }

    long releasableChargeAfterRead(int additionalBytes) {
        if (additionalBytes < 0) {
            throw new IllegalArgumentException("additionalBytes must be non-negative");
        }
        long remaining = (long) cumulation.readerIndex() + additionalBytes;
        long releasable = 0L;
        for (Component component : components) {
            if (remaining < component.length) {
                break;
            }
            releasable = InboundMemoryBudget.saturatedAdd(releasable, component.lease.reservedBytes());
            remaining -= component.length;
        }
        return releasable;
    }

    void discardFullyReadComponents() {
        while (!components.isEmpty()) {
            Component first = components.peekFirst();
            int readerIndex = cumulation.readerIndex();
            if (readerIndex < first.length) {
                return;
            }
            int writerIndex = cumulation.writerIndex();
            cumulation.removeComponent(0);
            cumulation.setIndex(readerIndex - first.length, writerIndex - first.length);
            components.removeFirst().lease.close();
        }
    }

    ConsolidationResult consolidateIfNeeded(Executor resumeExecutor, Runnable resumeCallback) {
        PendingConsolidation pending = pendingConsolidation;
        if (pending != null) {
            if (!pending.granted) {
                return ConsolidationResult.WAITING;
            }
            pendingConsolidation = null;
            return consolidateReserved(pending.newCharge);
        }
        if (components.size() < maxComponents) {
            return ConsolidationResult.NOT_NEEDED;
        }
        int readable = cumulation.readableBytes();
        if (readable == 0) {
            discardFullyReadComponents();
            return ConsolidationResult.NOT_NEEDED;
        }

        int targetCapacity = allocator.calculateNewCapacity(readable, Integer.MAX_VALUE);
        long newCharge = InboundBufferLease.chargeForCapacity(targetCapacity);
        long oldCharge = 0L;
        for (Component component : components) {
            oldCharge = InboundMemoryBudget.saturatedAdd(oldCharge, component.lease.reservedBytes());
        }
        if (budget != null && connection != null) {
            PendingConsolidation candidate = new PendingConsolidation(newCharge);
            pendingConsolidation = candidate;
            connection.setResumeCallback(
                    Objects.requireNonNull(resumeExecutor, "resumeExecutor"),
                    () -> {
                        if (pendingConsolidation == candidate
                                && connection.claimGrantedReservation(candidate.newCharge)) {
                            candidate.granted = true;
                            resumeCallback.run();
                        }
                    }
            );
            InboundMemoryBudget.ReservationResult result = budget.tryTransfer(connection, newCharge, oldCharge);
            if (result == InboundMemoryBudget.ReservationResult.WAITING) {
                return ConsolidationResult.WAITING;
            }
            pendingConsolidation = null;
            if (result != InboundMemoryBudget.ReservationResult.RESERVED) {
                return ConsolidationResult.REQUEST_LIMIT;
            }
        }
        return consolidateReserved(newCharge);
    }

    private ConsolidationResult consolidateReserved(long admittedCharge) {
        long newCharge = admittedCharge;
        ByteBuf merged = null;
        CompositeByteBuf replacement = null;
        InboundBufferLease mergedLease = null;
        boolean mergedLeaseOwnsAdmission = false;
        long consolidationCharge = 0L;
        try {
            int readable = cumulation.readableBytes();
            int targetCapacity = allocator.calculateNewCapacity(readable, Integer.MAX_VALUE);
            merged = allocator.buffer(targetCapacity, targetCapacity);
            long actualCharge = InboundBufferLease.chargeForCapacity(merged.capacity());
            if (actualCharge > newCharge) {
                throw new IllegalStateException("allocator returned capacity larger than admission credit");
            }
            if (budget != null && connection != null && actualCharge < newCharge) {
                budget.release(connection, newCharge - actualCharge);
                newCharge = actualCharge;
            }
            if (budget != null && connection != null) {
                budget.adjustConsolidation(newCharge);
                consolidationCharge = newCharge;
            }
            merged.writeBytes(cumulation, cumulation.readerIndex(), readable);
            replacement = allocator.compositeBuffer(Integer.MAX_VALUE);
            replacement.addComponent(true, merged);
            merged = null;
            mergedLease = InboundBufferLease.admitted(
                    budget,
                    connection == null ? null : connection.account(),
                    newCharge
            );
            mergedLeaseOwnsAdmission = true;

            CompositeByteBuf previous = cumulation;
            cumulation = replacement;
            replacement = null;
            previous.release();
            while (!components.isEmpty()) {
                components.removeFirst().lease.close();
            }
            components.addLast(new Component(readable, mergedLease));
            mergedLease = null;
            return ConsolidationResult.CONSOLIDATED;
        } catch (Throwable failure) {
            if (merged != null) {
                ReferenceCountUtil.safeRelease(merged);
            }
            if (replacement != null) {
                ReferenceCountUtil.safeRelease(replacement);
            }
            if (mergedLease != null) {
                mergedLease.close();
            }
            if (!mergedLeaseOwnsAdmission && budget != null && connection != null) {
                budget.release(connection, newCharge);
            }
            return ConsolidationResult.REQUEST_LIMIT;
        } finally {
            if (consolidationCharge > 0L && budget != null && connection != null) {
                budget.adjustConsolidation(-consolidationCharge);
            }
        }
    }

    @Override
    public void close() {
        cancelPendingConsolidation();
        CompositeByteBuf current = cumulation;
        cumulation = null;
        ReferenceCountUtil.safeRelease(current);
        while (!components.isEmpty()) {
            components.removeFirst().lease.close();
        }
    }

    private void cancelPendingConsolidation() {
        PendingConsolidation pending = pendingConsolidation;
        pendingConsolidation = null;
        if (pending == null || budget == null || connection == null) {
            return;
        }
        if (pending.granted || connection.claimGrantedReservation(pending.newCharge)) {
            budget.release(connection, pending.newCharge);
        } else {
            budget.cancelWaiter(connection);
        }
    }

    private record Component(int length, InboundBufferLease lease) {
    }

    private static final class PendingConsolidation {
        private final long newCharge;
        private volatile boolean granted;

        private PendingConsolidation(long newCharge) {
            this.newCharge = newCharge;
        }
    }
}
