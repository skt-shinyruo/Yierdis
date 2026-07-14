package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import yier.bubu.redis.execution.api.ReplyCapacityUnavailableException;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationSink;
import yier.bubu.redis.execution.api.ReplyTooLargeException;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 将一个回复写入固定上限的 ByteBuf 块，并在每次 allocator 调用前转换对应 lease。
 */
final class BoundedChunkedReplySink implements ReplyReservationSink, AutoCloseable {
    static final long CHUNK_COMPONENT_OVERHEAD_BYTES = 1_024L;

    @FunctionalInterface
    interface ChunkAllocator {
        ByteBuf allocate(int initialCapacity, int maxCapacity);
    }

    private final ReplySlot slot;
    private final ChunkAllocator allocator;
    private final int chunkPayloadBytes;
    private final long controlReservationBytes;
    private final long maxTotalBytes;
    private final Consumer<AutoCloseable> ownerResourceCloser;

    private ReplyPlan plan;
    private long maximumRetainedSourceBytes;
    private long writtenBytes;
    private ByteBuf current;
    private int currentWritable;
    private boolean finished;

    BoundedChunkedReplySink(
            ReplySlot slot,
            ChunkAllocator allocator,
            int chunkPayloadBytes,
            long controlReservationBytes,
            long maxTotalBytes
    ) {
        this(
                slot,
                allocator,
                chunkPayloadBytes,
                controlReservationBytes,
                maxTotalBytes,
                BoundedChunkedReplySink::closeResource
        );
    }

    BoundedChunkedReplySink(
            ReplySlot slot,
            ChunkAllocator allocator,
            int chunkPayloadBytes,
            long controlReservationBytes,
            long maxTotalBytes,
            Consumer<AutoCloseable> ownerResourceCloser
    ) {
        this.slot = Objects.requireNonNull(slot, "slot");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        if (chunkPayloadBytes <= 0) {
            throw new IllegalArgumentException("chunkPayloadBytes must be > 0");
        }
        if (controlReservationBytes <= 0L || maxTotalBytes < controlReservationBytes) {
            throw new IllegalArgumentException("invalid reply reservation limits");
        }
        this.chunkPayloadBytes = chunkPayloadBytes;
        this.controlReservationBytes = controlReservationBytes;
        this.maxTotalBytes = maxTotalBytes;
        this.ownerResourceCloser = Objects.requireNonNull(ownerResourceCloser, "ownerResourceCloser");
    }

    static BoundedChunkedReplySink forChannel(
            ReplySlot slot,
            Channel channel,
            int chunkPayloadBytes,
            long controlReservationBytes,
            long maxTotalBytes
    ) {
        return forChannel(
                slot,
                channel,
                chunkPayloadBytes,
                controlReservationBytes,
                maxTotalBytes,
                BoundedChunkedReplySink::closeResource
        );
    }

    static BoundedChunkedReplySink forChannel(
            ReplySlot slot,
            Channel channel,
            int chunkPayloadBytes,
            long controlReservationBytes,
            long maxTotalBytes,
            Consumer<AutoCloseable> ownerResourceCloser
    ) {
        Objects.requireNonNull(channel, "channel");
        return new BoundedChunkedReplySink(
                slot,
                (initialCapacity, maxCapacity) -> channel.alloc().buffer(initialCapacity, maxCapacity),
                chunkPayloadBytes,
                controlReservationBytes,
                maxTotalBytes,
                ownerResourceCloser
        );
    }

    @Override
    public void require(ReplyPlan requestedPlan) {
        Objects.requireNonNull(requestedPlan, "requestedPlan");
        ensureOpen();

        if (plan != null && plan.reserveMaximum()) {
            reserveNestedMaximumPlan(requestedPlan);
            return;
        }

        long target = reservationTarget(requestedPlan);
        if (target > maxTotalBytes) {
            throw tooLarge("reply exceeds configured single-reply capacity");
        }
        long currentReservation = slot.lease().reservedBytes();
        if (target > currentReservation
                && !slot.lease().tryReserveAdditional(target - currentReservation, maxTotalBytes)) {
            slot.waitForAdditionalCapacity(target - currentReservation, maxTotalBytes);
            slot.markWaitingForCapacity();
            throw new ReplyCapacityUnavailableException("reply capacity is currently unavailable");
        }
        if (target <= currentReservation) {
            slot.cancelCapacityWait();
        }
        slot.clearCapacityWaitAfterReservation();
        if (requestedPlan.reserveMaximum()) {
            maximumRetainedSourceBytes = plan == null ? 0L : plan.retainedSourceBytes();
        }
        plan = requestedPlan;
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length) {
        Objects.requireNonNull(source, "source");
        Objects.checkFromIndexSize(sourceIndex, length, source.length);
        ensureOpen();
        if (length == 0) {
            return;
        }
        if (plan == null) {
            ensureControlWriteFits(length);
        } else if (plan.reserveMaximum() && !fitsMaximumWrite(length)) {
            slot.fail(ReplyCleanupOwner.SEQUENCER);
            throw tooLarge("reply encoded bytes exceed its maximum reservation");
        } else if (!plan.reserveMaximum() && exceedsExactPlan(length)) {
            slot.fail(ReplyCleanupOwner.SEQUENCER);
            throw tooLarge("reply encoded bytes exceed its preflight plan");
        }

        int offset = sourceIndex;
        int remaining = length;
        while (remaining > 0) {
            ensureWritableChunk(remaining);
            int writable = Math.min(remaining, currentWritable);
            current.writeBytes(source, offset, writable);
            offset += writable;
            remaining -= writable;
            currentWritable -= writable;
            writtenBytes += writable;
        }
    }

    @Override
    public long writtenBytes() {
        return writtenBytes;
    }

    @Override
    public boolean transferOwnership(AutoCloseable resource) {
        AutoCloseable source = Objects.requireNonNull(resource, "resource");
        slot.addOwnedResource(() -> ownerResourceCloser.accept(source));
        return true;
    }

    void finish() {
        ensureOpen();
        finished = true;
    }

    @Override
    public void close() {
        finished = true;
    }

    private void ensureWritableChunk(int remainingWriteBytes) {
        if (current != null && currentWritable > 0) {
            return;
        }
        int payloadCapacity = nextChunkPayloadCapacity(remainingWriteBytes);
        long convertedCredit = saturatedAdd(payloadCapacity, CHUNK_COMPONENT_OVERHEAD_BYTES);
        if (!slot.lease().convertToAllocated(convertedCredit)) {
            slot.fail(ReplyCleanupOwner.SEQUENCER);
            throw new IllegalStateException("reply reservation was not converted before ByteBuf allocation");
        }

        ByteBuf buffer = null;
        try {
            buffer = allocator.allocate(payloadCapacity, payloadCapacity);
            long actualCharge = saturatedAdd(buffer.capacity(), CHUNK_COMPONENT_OVERHEAD_BYTES);
            if (actualCharge > convertedCredit) {
                buffer.release();
                buffer = null;
                slot.lease().releaseAllocated(convertedCredit);
                slot.fail(ReplyCleanupOwner.SEQUENCER);
                throw new IllegalStateException("allocator returned a buffer larger than converted reply credit");
            }
            long surplus = convertedCredit - actualCharge;
            if (surplus > 0L) {
                slot.lease().releaseAllocated(surplus);
            }
            slot.addChunk(buffer, actualCharge);
            current = buffer;
            currentWritable = buffer.writableBytes();
            buffer = null;
        } catch (RuntimeException | Error failure) {
            if (buffer != null) {
                buffer.release();
            }
            slot.fail(ReplyCleanupOwner.SEQUENCER);
            throw failure;
        }
    }

    private int nextChunkPayloadCapacity(int remainingWriteBytes) {
        if (plan == null) {
            return controlPayloadCapacity();
        }
        if (!plan.reserveMaximum()) {
            long remainingPlanBytes = plan.encodedUpperBoundBytes() - writtenBytes;
            if (remainingPlanBytes <= 0L) {
                throw tooLarge("reply encoded bytes exceed its preflight plan");
            }
            return (int) Math.min(chunkPayloadBytes, remainingPlanBytes);
        }
        return Math.min(chunkPayloadBytes, Math.max(1, remainingWriteBytes));
    }

    private void ensureControlWriteFits(int additionalBytes) {
        int controlPayloadCapacity = controlPayloadCapacity();
        if (controlPayloadCapacity <= 0
                || writtenBytes > (long) controlPayloadCapacity - additionalBytes) {
            if (writtenBytes > 0L) {
                slot.fail(ReplyCleanupOwner.SEQUENCER);
            }
            throw tooLarge("reply requires an explicit preflight plan");
        }
    }

    private int controlPayloadCapacity() {
        long payloadCapacity = controlReservationBytes - CHUNK_COMPONENT_OVERHEAD_BYTES;
        if (payloadCapacity <= 0L) {
            return 0;
        }
        return (int) Math.min(chunkPayloadBytes, payloadCapacity);
    }

    private ReplyTooLargeException tooLarge(String message) {
        slot.recordOversizedReply();
        return new ReplyTooLargeException(message);
    }

    private long reservationTarget(ReplyPlan requestedPlan) {
        if (requestedPlan.reserveMaximum()) {
            return maxTotalBytes;
        }
        long chunks = chunkCount(requestedPlan.encodedUpperBoundBytes());
        long target = saturatedAdd(controlReservationBytes, requestedPlan.totalUpperBoundBytes());
        return saturatedAdd(target, saturatedMultiply(chunks, CHUNK_COMPONENT_OVERHEAD_BYTES));
    }

    private long chunkCount(long encodedBytes) {
        if (encodedBytes <= 0L) {
            return 0L;
        }
        return 1L + (encodedBytes - 1L) / chunkPayloadBytes;
    }

    private boolean exceedsExactPlan(int additionalBytes) {
        return additionalBytes > plan.encodedUpperBoundBytes() - writtenBytes;
    }

    private void reserveNestedMaximumPlan(ReplyPlan requestedPlan) {
        if (requestedPlan.reserveMaximum()) {
            return;
        }
        long encodedCharge = saturatedAdd(
                requestedPlan.encodedUpperBoundBytes(),
                saturatedMultiply(
                        chunkCount(requestedPlan.encodedUpperBoundBytes()),
                        CHUNK_COMPONENT_OVERHEAD_BYTES
                )
        );
        long nestedCharge = saturatedAdd(encodedCharge, requestedPlan.retainedSourceBytes());
        if (!fitsMaximumCharge(nestedCharge)) {
            throw tooLarge("nested reply exceeds its maximum reservation");
        }
        maximumRetainedSourceBytes = saturatedAdd(maximumRetainedSourceBytes, requestedPlan.retainedSourceBytes());
    }

    private boolean fitsMaximumWrite(int additionalBytes) {
        long remainingBytes = additionalBytes;
        if (current != null && currentWritable > 0) {
            remainingBytes = Math.max(0L, remainingBytes - currentWritable);
        }
        long chunkCharge = saturatedAdd(
                remainingBytes,
                saturatedMultiply(chunkCount(remainingBytes), CHUNK_COMPONENT_OVERHEAD_BYTES)
        );
        return fitsMaximumCharge(chunkCharge);
    }

    private boolean fitsMaximumCharge(long additionalCharge) {
        long currentCharge = saturatedAdd(slot.lease().allocatedBytes(), maximumRetainedSourceBytes);
        long projectedCharge = saturatedAdd(currentCharge, additionalCharge);
        return projectedCharge <= maxTotalBytes - controlReservationBytes;
    }

    private void ensureOpen() {
        if (finished || slot.state() == ReplySlotState.CANCELLED || slot.state() == ReplySlotState.FAILED) {
            throw new IllegalStateException("reply sink is closed");
        }
    }

    private static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long saturatedMultiply(long left, long right) {
        if (left <= 0L || right <= 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private static void closeResource(AutoCloseable resource) {
        try {
            resource.close();
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("reply resource close failed", failure);
        }
    }
}
