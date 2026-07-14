package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import java.util.Objects;

/**
 * 在手工读取前保留一个固定 receive-buffer 额度，并分别维护 ingress 与 executor 的暂停原因。
 */
public final class InboundReadCreditHandler extends ChannelInboundHandlerAdapter implements InboundReadControl {
    private final InboundMemoryBudget budget;
    private final InboundConnectionMemory connection;
    private final int receiveBufferCapacity;
    private final long receiveCreditBytes;

    private ChannelHandlerContext context;
    private PendingReadCredit pendingReadCredit;
    private long outstandingCreditBytes;
    private boolean ingressPaused;
    private boolean executorPaused;
    private boolean readInFlight;
    private boolean readScheduled;
    private boolean closed;

    public InboundReadCreditHandler(
            InboundMemoryBudget budget,
            InboundConnectionMemory connection,
            int receiveBufferCapacity
    ) {
        this.budget = Objects.requireNonNull(budget, "budget");
        this.connection = Objects.requireNonNull(connection, "connection");
        if (receiveBufferCapacity <= 0) {
            throw new IllegalArgumentException("receiveBufferCapacity must be > 0");
        }
        this.receiveBufferCapacity = receiveBufferCapacity;
        this.receiveCreditBytes = InboundBufferLease.chargeForCapacity(receiveBufferCapacity);
    }

    public InboundConnectionMemory connectionMemory() {
        return connection;
    }

    public boolean inputPausedByIngress() {
        return ingressPaused;
    }

    public boolean inputPausedByExecutor() {
        return executorPaused;
    }

    public long outstandingReadCreditBytes() {
        return outstandingCreditBytes;
    }

    public void pauseExecutorInput() {
        executeOnEventLoop(() -> executorPaused = true);
    }

    public void resumeExecutorInput() {
        executeOnEventLoop(() -> {
            executorPaused = false;
            scheduleReadIfAllowed();
        });
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        context = ctx;
        ctx.channel().config().setRecvByteBufAllocator(new FixedRecvByteBufAllocator(receiveBufferCapacity));
        ctx.channel().config().setMaxMessagesPerRead(1);
        ctx.channel().config().setAutoRead(false);
        scheduleReadIfAllowed();
        super.handlerAdded(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        scheduleReadIfAllowed();
        super.channelActive(ctx);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (readInFlight && outstandingCreditBytes > 0L) {
            releaseOutstandingCredit();
            readInFlight = false;
            scheduleReadIfAllowed();
        }
        super.channelReadComplete(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cleanup();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        cleanup();
        super.handlerRemoved(ctx);
    }

    @Override
    public void pauseIngress() {
        executeOnEventLoop(() -> {
            ingressPaused = true;
            cancelPendingReadCredit();
        });
    }

    @Override
    public void resumeIngress() {
        executeOnEventLoop(() -> {
            ingressPaused = false;
            scheduleReadIfAllowed();
        });
    }

    InboundBufferLease takeInputLease(ByteBuf input) {
        long actualCharge = InboundBufferLease.chargeForRetainedBuffer(input);
        long credit = outstandingCreditBytes;
        outstandingCreditBytes = 0L;
        readInFlight = false;
        if (credit > 0L) {
            budget.adjustReadCredit(-credit);
        }

        if (actualCharge <= credit) {
            if (credit > actualCharge) {
                budget.release(connection, credit - actualCharge);
            }
            return InboundBufferLease.admitted(budget, connection.account(), actualCharge);
        }

        long additionalCharge = actualCharge - credit;
        InboundMemoryBudget.ReservationResult result = budget.tryReserve(connection, additionalCharge);
        if (result == InboundMemoryBudget.ReservationResult.RESERVED) {
            return InboundBufferLease.admitted(budget, connection.account(), actualCharge);
        }
        if (result == InboundMemoryBudget.ReservationResult.WAITING) {
            budget.cancelWaiter(connection);
        }
        if (credit > 0L) {
            budget.release(connection, credit);
        }
        ingressPaused = true;
        return null;
    }

    void rejectInbound(ByteBuf input) {
        ReferenceCountUtil.safeRelease(input);
        pauseIngress();
    }

    private void scheduleReadIfAllowed() {
        ChannelHandlerContext ctx = context;
        if (ctx == null || closed || readScheduled || !readAllowed(ctx)) {
            return;
        }
        readScheduled = true;
        ctx.executor().execute(() -> {
            readScheduled = false;
            requestReadIfAllowed();
        });
    }

    private void requestReadIfAllowed() {
        ChannelHandlerContext ctx = context;
        if (ctx == null || closed || !readAllowed(ctx)
                || readInFlight || outstandingCreditBytes > 0L || pendingReadCredit != null) {
            return;
        }

        PendingReadCredit pending = new PendingReadCredit(receiveCreditBytes);
        pendingReadCredit = pending;
        connection.setResumeCallback(ctx.executor(), () -> {
            if (pendingReadCredit == pending && connection.claimGrantedReservation(pending.bytes)) {
                pendingReadCredit = null;
                grantReadCredit(pending.bytes);
            }
        });
        InboundMemoryBudget.ReservationResult result = budget.tryReserve(connection, pending.bytes);
        if (result == InboundMemoryBudget.ReservationResult.RESERVED) {
            pendingReadCredit = null;
            grantReadCredit(pending.bytes);
            return;
        }
        if (result == InboundMemoryBudget.ReservationResult.WAITING) {
            return;
        }
        pendingReadCredit = null;
        ingressPaused = true;
    }

    private void grantReadCredit(long bytes) {
        ChannelHandlerContext ctx = context;
        if (ctx == null || closed || !readAllowed(ctx)) {
            budget.release(connection, bytes);
            return;
        }
        outstandingCreditBytes = bytes;
        readInFlight = true;
        budget.adjustReadCredit(bytes);
        ctx.read();
    }

    private boolean readAllowed(ChannelHandlerContext ctx) {
        return !ingressPaused
                && !executorPaused
                && ctx.channel().isActive()
                && ctx.channel().isWritable();
    }

    private void cancelPendingReadCredit() {
        PendingReadCredit pending = pendingReadCredit;
        pendingReadCredit = null;
        if (pending != null) {
            if (connection.claimGrantedReservation(pending.bytes)) {
                budget.release(connection, pending.bytes);
            } else {
                budget.cancelWaiter(connection);
            }
        }
    }

    private void releaseOutstandingCredit() {
        long credit = outstandingCreditBytes;
        outstandingCreditBytes = 0L;
        if (credit == 0L) {
            return;
        }
        budget.adjustReadCredit(-credit);
        budget.release(connection, credit);
    }

    private void cleanup() {
        if (closed) {
            return;
        }
        closed = true;
        cancelPendingReadCredit();
        releaseOutstandingCredit();
        connection.close();
        context = null;
    }

    private void executeOnEventLoop(Runnable action) {
        ChannelHandlerContext ctx = context;
        if (ctx == null || closed) {
            return;
        }
        if (ctx.executor().inEventLoop()) {
            action.run();
            return;
        }
        ctx.executor().execute(action);
    }

    private record PendingReadCredit(long bytes) {
    }
}
