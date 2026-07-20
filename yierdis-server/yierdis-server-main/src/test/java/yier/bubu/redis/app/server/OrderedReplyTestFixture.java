package yier.bubu.redis.app.server;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.charset.StandardCharsets;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.executor.CommandExecutor;

final class OrderedReplyTestFixture implements AutoCloseable {
    static final long CONTROL_BYTES = 4_096L;
    static final long MAX_REPLY_BYTES = 128L * 1024L;
    private static final long CONNECTION_BYTES = 256L * 1024L;
    private static final long GLOBAL_BYTES = 512L * 1024L;
    private static final int CHUNK_BYTES = 64 * 1024;

    private final EmbeddedChannel channel;
    private final OutboundMemoryBudget budget;
    private final ConnectionReplySequencer sequencer;
    private final NettyExecutionConnection connection;
    private final OutboundConnectionMemory connectionMemory;

    private OrderedReplyTestFixture(
            CommandExecutor<NettyExecutionConnection> executor,
            RedisReplyWriterFactory replyWriterFactory
    ) {
        channel = new EmbeddedChannel(new YierdisFastCommandHandler(executor, replyWriterFactory));
        connection = NettyExecutionConnection.getOrCreate(channel, 16, 1_024);
        budget = new OutboundMemoryBudget(GLOBAL_BYTES);
        connectionMemory = budget.openConnection(CONNECTION_BYTES);
        sequencer = new ConnectionReplySequencer(
                channel,
                connectionMemory,
                () -> { },
                slot -> BoundedChunkedReplySink.forChannel(
                        slot,
                        channel,
                        CHUNK_BYTES,
                        CONTROL_BYTES,
                        MAX_REPLY_BYTES
                )
        );
        connection.bindReplyGate(new NettyReplyDecodedMessageGate(
                CONTROL_BYTES,
                MAX_REPLY_BYTES,
                connectionMemory,
                sequencer
        ));
    }

    static OrderedReplyTestFixture open(
            CommandExecutor<NettyExecutionConnection> executor,
            RedisReplyWriterFactory replyWriterFactory
    ) {
        return new OrderedReplyTestFixture(executor, replyWriterFactory);
    }

    EmbeddedChannel channel() {
        return channel;
    }

    NettyExecutionConnection connection() {
        return connection;
    }

    void write(ExecutionRequest request) {
        channel.writeInbound(register(request));
    }

    void writeProtocolError(String message) {
        channel.writeInbound(register(new yier.bubu.redis.protocol.resp.netty.RespProtocolError(message, true)));
    }

    RegisteredRespMessage register(Object message) {
        OutboundMemoryLease lease = connectionMemory.reserve(CONTROL_BYTES, MAX_REPLY_BYTES).orElseThrow();
        ReplySlot slot = sequencer.register(lease).orElseThrow();
        return new RegisteredRespMessage(message, slot);
    }

    ReplySlot registerReadyAscii(String value) {
        OutboundMemoryLease lease = connectionMemory.reserve(CONTROL_BYTES, MAX_REPLY_BYTES).orElseThrow();
        ReplySlot slot = sequencer.register(lease).orElseThrow();
        slot.addChunk(Unpooled.copiedBuffer(value, StandardCharsets.US_ASCII));
        slot.markReady(false);
        return slot;
    }

    void drain() {
        for (int i = 0; i < 4; i++) {
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();
        }
    }

    @Override
    public void close() {
        channel.finishAndReleaseAll();
        sequencer.close();
        budget.close();
    }
}
