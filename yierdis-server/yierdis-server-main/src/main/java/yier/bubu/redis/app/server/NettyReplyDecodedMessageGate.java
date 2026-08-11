package yier.bubu.redis.app.server;

import io.netty.channel.ChannelHandlerContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.protocol.resp.netty.RespDecodedMessageGate;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * 在 RESP 解码和下游处理之间先取得回复控制额度，再创建回复槽位。
 */
final class NettyReplyDecodedMessageGate implements RespDecodedMessageGate {
    private final long controlReservationBytes;
    private final long singleReplyLimitBytes;
    private final OutboundConnectionMemory connectionMemory;
    private final ConnectionReplySequencer sequencer;
    private volatile CompletableFuture<Void> registrationBarrier;

    NettyReplyDecodedMessageGate(
            long controlReservationBytes,
            long singleReplyLimitBytes,
            OutboundConnectionMemory connectionMemory,
            ConnectionReplySequencer sequencer
    ) {
        if (controlReservationBytes <= 0L || singleReplyLimitBytes <= 0L) {
            throw new IllegalArgumentException("reply control and single-reply limits must be positive");
        }
        if (controlReservationBytes > singleReplyLimitBytes) {
            throw new IllegalArgumentException("reply control reservation must fit one reply");
        }
        this.controlReservationBytes = controlReservationBytes;
        this.singleReplyLimitBytes = singleReplyLimitBytes;
        this.connectionMemory = Objects.requireNonNull(connectionMemory, "connectionMemory");
        this.sequencer = Objects.requireNonNull(sequencer, "sequencer");
    }

    @Override
    public Admission tryAdmit(ChannelHandlerContext ctx, Object decoded, Runnable resumeOnEventLoop) {
        Objects.requireNonNull(decoded, "decoded");
        Objects.requireNonNull(resumeOnEventLoop, "resumeOnEventLoop");
        if (!sequencer.acceptingRegistrations() || connectionMemory.closed()) {
            return Admission.closed();
        }
        CompletableFuture<Void> barrier = registrationBarrier;
        if (barrier != null) {
            if (!barrier.isDone()) {
                barrier.whenComplete((ignored, failure) -> resumeLater(ctx, resumeOnEventLoop));
                return Admission.waiting();
            }
            if (registrationBarrier == barrier) {
                registrationBarrier = null;
            }
        }

        Optional<OutboundMemoryLease> reservation = connectionMemory.reserve(
                controlReservationBytes,
                singleReplyLimitBytes
        );
        if (reservation.isPresent()) {
            Optional<ReplySlot> slot = sequencer.register(reservation.get());
            if (slot.isEmpty()) {
                return Admission.closed();
            }
            ReplySlot registered = slot.get();
            if (isExec(decoded)) {
                // EXEC 的动态回复需要把当前 lease 扩到单回复上限；后续控制槽若先占额度会把扩容锁死。
                registrationBarrier = registered.cleanupCompletion();
            }
            return Admission.admitted(new RegisteredRespMessage(decoded, registered));
        }

        if (connectionMemory.awaitCapacity(controlReservationBytes, singleReplyLimitBytes, resumeOnEventLoop)) {
            return Admission.waiting();
        }
        if (ctx != null) {
            ctx.close();
        }
        return Admission.closed();
    }

    Optional<ReplySlot> tryRegisterTerminalSlot() {
        if (!sequencer.acceptingRegistrations() || connectionMemory.closed()) {
            return Optional.empty();
        }
        return connectionMemory.reserve(controlReservationBytes, singleReplyLimitBytes)
                .flatMap(sequencer::register);
    }

    CompletableFuture<Void> shutdownGracefully() {
        return sequencer.shutdownGracefully();
    }

    OutboundConnectionMemory connectionMemoryForTests() {
        return connectionMemory;
    }

    private static boolean isExec(Object decoded) {
        if (!(decoded instanceof ExecutionRequest request)
                || request.argc() == 0
                || request.isNull(0)
                || request.len(0) != 4) {
            return false;
        }
        return asciiUpper(request.byteAt(0, 0)) == 'E'
                && asciiUpper(request.byteAt(0, 1)) == 'X'
                && asciiUpper(request.byteAt(0, 2)) == 'E'
                && asciiUpper(request.byteAt(0, 3)) == 'C';
    }

    private static int asciiUpper(byte value) {
        int ascii = value & 0xff;
        return ascii >= 'a' && ascii <= 'z' ? ascii - ('a' - 'A') : ascii;
    }

    private static void resumeLater(ChannelHandlerContext ctx, Runnable resumeOnEventLoop) {
        if (ctx == null) {
            resumeOnEventLoop.run();
            return;
        }
        try {
            ctx.executor().execute(resumeOnEventLoop);
        } catch (RuntimeException failure) {
            ctx.close();
        }
    }
}
