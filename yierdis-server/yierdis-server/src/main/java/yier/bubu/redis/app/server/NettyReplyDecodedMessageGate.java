package yier.bubu.redis.app.server;

import io.netty.channel.ChannelHandlerContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyAdmissionRequirement;
import yier.bubu.redis.protocol.resp.netty.RespDecodedMessage;
import yier.bubu.redis.protocol.resp.netty.RespDecodedMessageGate;
import yier.bubu.redis.protocol.resp.netty.RespProtocolError;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * 在 RESP 解码和下游处理之间先取得回复控制额度，再创建回复槽位。
 */
final class NettyReplyDecodedMessageGate implements RespDecodedMessageGate {
    private final long controlReservationBytes;
    private final long singleReplyLimitBytes;
    private final OutboundConnectionMemory connectionMemory;
    private final ConnectionReplySequencer sequencer;
    private final Function<ExecutionRequest, ReplyAdmissionRequirement> requirementResolver;
    private volatile CompletableFuture<Void> registrationBarrier;

    NettyReplyDecodedMessageGate(
            long controlReservationBytes,
            long singleReplyLimitBytes,
            OutboundConnectionMemory connectionMemory,
            ConnectionReplySequencer sequencer
    ) {
        this(
                controlReservationBytes,
                singleReplyLimitBytes,
                connectionMemory,
                sequencer,
                ignored -> ReplyAdmissionRequirement.PIPELINED
        );
    }

    NettyReplyDecodedMessageGate(
            long controlReservationBytes,
            long singleReplyLimitBytes,
            OutboundConnectionMemory connectionMemory,
            ConnectionReplySequencer sequencer,
            Function<ExecutionRequest, ReplyAdmissionRequirement> requirementResolver
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
        this.requirementResolver = Objects.requireNonNull(requirementResolver, "requirementResolver");
    }

    @Override
    public Admission tryAdmit(
            ChannelHandlerContext ctx,
            RespDecodedMessage decoded,
            Runnable resumeOnEventLoop
    ) {
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(decoded, "decoded");
        Objects.requireNonNull(resumeOnEventLoop, "resumeOnEventLoop");
        if (!sequencer.acceptingRegistrations() || connectionMemory.closed()) {
            return Admission.CLOSED;
        }
        CompletableFuture<Void> barrier = activeRegistrationBarrier();
        if (barrier != null) {
            barrier.whenComplete((ignored, failure) -> resumeLater(ctx, resumeOnEventLoop));
            return Admission.WAITING;
        }
        ReplyAdmissionRequirement requirement = requirement(decoded);

        Optional<OutboundMemoryLease> reservation = connectionMemory.reserve(
                controlReservationBytes,
                singleReplyLimitBytes
        );
        if (reservation.isPresent()) {
            Optional<ReplySlot> slot = sequencer.register(reservation.get());
            if (slot.isEmpty()) {
                return Admission.CLOSED;
            }
            ReplySlot registered = slot.get();
            if (requirement == ReplyAdmissionRequirement.BARRIER_UNTIL_CLEANUP) {
                // 动态回复需要把当前 lease 扩到单回复上限；后续控制槽若先占额度会把扩容锁死。
                registrationBarrier = registered.cleanupCompletion();
            }
            RegisteredRespMessage registeredMessage = new RegisteredRespMessage(decoded, registered);
            try {
                ctx.fireChannelRead(registeredMessage);
            } catch (Throwable ignored) {
                registeredMessage.close();
                ctx.close();
            }
            return Admission.ADMITTED;
        }

        if (connectionMemory.awaitCapacity(controlReservationBytes, singleReplyLimitBytes, resumeOnEventLoop)) {
            return Admission.WAITING;
        }
        ctx.close();
        return Admission.CLOSED;
    }

    Optional<ReplySlot> tryRegisterTerminalSlot() {
        if (!sequencer.acceptingRegistrations()
                || connectionMemory.closed()
                || activeRegistrationBarrier() != null) {
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

    private CompletableFuture<Void> activeRegistrationBarrier() {
        CompletableFuture<Void> barrier = registrationBarrier;
        if (barrier == null || !barrier.isDone()) {
            return barrier;
        }
        if (registrationBarrier == barrier) {
            registrationBarrier = null;
        }
        return null;
    }

    private ReplyAdmissionRequirement requirement(RespDecodedMessage decoded) {
        return switch (decoded) {
            case RespDecodedMessage.Request value -> Objects.requireNonNull(
                    requirementResolver.apply(value.request()),
                    "reply admission requirement resolver returned null"
            );
            case RespProtocolError ignored -> ReplyAdmissionRequirement.PIPELINED;
        };
    }

    private static void resumeLater(ChannelHandlerContext ctx, Runnable resumeOnEventLoop) {
        try {
            ctx.executor().execute(resumeOnEventLoop);
        } catch (RuntimeException failure) {
            ctx.close();
        }
    }
}
