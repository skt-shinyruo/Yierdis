package yier.bubu.redis.app.server;

import io.netty.channel.ChannelHandlerContext;
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

        Optional<OutboundMemoryLease> reservation = connectionMemory.reserve(
                controlReservationBytes,
                singleReplyLimitBytes
        );
        if (reservation.isPresent()) {
            Optional<ReplySlot> slot = sequencer.register(reservation.get());
            return slot.<Admission>map(value -> Admission.admitted(new RegisteredRespMessage(decoded, value)))
                    .orElseGet(Admission::closed);
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
}
