package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Objects;

final class NioBenchmarkClient implements AutoCloseable {
    private static final int DEFAULT_READ_BUFFER_BYTES = 64 * 1024;

    record ReplyLimits(
            int readBufferBytes,
            int maxBulkBytes,
            int maxLineBytes,
            int maxArrayLength,
            int maxDepth
    ) {
        private static final ReplyLimits DEFAULTS = new ReplyLimits(
                DEFAULT_READ_BUFFER_BYTES,
                RespProtocolLimits.DEFAULT_MAX_BULK_BYTES,
                RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES,
                RespProtocolLimits.DEFAULT_MAX_ARGS,
                IncrementalRespReplyDecoder.MAX_SUPPORTED_DEPTH
        );

        ReplyLimits {
            if (readBufferBytes <= 0) {
                throw new IllegalArgumentException("readBufferBytes must be > 0");
            }
        }

        static ReplyLimits defaults() {
            return DEFAULTS;
        }
    }

    @FunctionalInterface
    interface GatheringWrite {
        long write(ByteBuffer[] sources) throws IOException;
    }

    enum Phase {
        CONNECTING,
        READY,
        WRITING,
        READING,
        DONE
    }

    final SocketChannel channel;
    private final GatheringWrite gatheringWrite;
    SelectionKey selectionKey;
    final PreparedPipeline preparedPipeline;
    final ByteBuffer firstWritePrefix;
    final ByteBuffer[] gatheringWriteBuffers;
    final ByteBuffer pipelineWriteBuffer;
    final ByteBuffer readBuffer;
    final IncrementalRespReplyDecoder replyDecoder;

    int prefixPending;
    int pendingReplies;
    long batchStartNanos;
    long batchLatencyMicros;
    Phase phase;

    private boolean firstBatch = true;
    private boolean batchLatencyCaptured;
    private long lastWriteBytes;

    NioBenchmarkClient(
            SocketChannel channel,
            GatheringWrite gatheringWrite,
            PreparedPipeline compiledPipeline,
            byte[] prefix,
            int prefixReplies,
            BenchmarkRandom random,
            ReplyLimits replyLimits
    ) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.gatheringWrite = Objects.requireNonNull(gatheringWrite, "gatheringWrite");
        this.preparedPipeline = Objects.requireNonNull(
                compiledPipeline,
                "compiledPipeline"
        ).copyForClient();
        byte[] pipelineBytes = preparedPipeline.bytesForWrite(
                Objects.requireNonNull(random, "random")
        );
        this.firstWritePrefix = ByteBuffer.wrap(
                Objects.requireNonNull(prefix, "prefix")
        ).asReadOnlyBuffer();
        this.pipelineWriteBuffer = ByteBuffer.wrap(pipelineBytes);
        this.pipelineWriteBuffer.position(this.pipelineWriteBuffer.limit());
        this.gatheringWriteBuffers = new ByteBuffer[]{firstWritePrefix, pipelineWriteBuffer};
        ReplyLimits requiredReplyLimits = Objects.requireNonNull(replyLimits, "replyLimits");
        this.readBuffer = ByteBuffer.allocate(requiredReplyLimits.readBufferBytes());
        this.replyDecoder = new IncrementalRespReplyDecoder(
                requiredReplyLimits.maxBulkBytes(),
                requiredReplyLimits.maxLineBytes(),
                requiredReplyLimits.maxArrayLength(),
                requiredReplyLimits.maxDepth()
        );
        this.prefixPending = prefixReplies;
        this.phase = Phase.CONNECTING;
    }

    void register(SelectionKey key, boolean connected) {
        this.selectionKey = Objects.requireNonNull(key, "key");
        this.phase = connected ? Phase.READY : Phase.CONNECTING;
    }

    void beginBatch(int pipeline, BenchmarkRandom random) {
        if (pipeline <= 0) {
            throw new IllegalArgumentException("pipeline must be > 0");
        }
        if (phase != Phase.READY) {
            throw new IllegalStateException("client must be ready to begin a batch");
        }
        if (firstBatch) {
            firstBatch = false;
        } else {
            preparedPipeline.bytesForWrite(Objects.requireNonNull(random, "random"));
        }
        pipelineWriteBuffer.clear();
        pendingReplies = pipeline;
        batchLatencyCaptured = false;
        batchLatencyMicros = 0;
        phase = Phase.WRITING;
    }

    boolean writeBatch() throws IOException {
        long positionsBeforeWrite = (long) firstWritePrefix.position()
                + pipelineWriteBuffer.position();
        boolean complete = writeBatch(
                gatheringWrite,
                gatheringWriteBuffers,
                pipelineWriteBuffer
        );
        lastWriteBytes = (long) firstWritePrefix.position()
                + pipelineWriteBuffer.position()
                - positionsBeforeWrite;
        return complete;
    }

    static boolean writeBatch(
            GatheringWrite write,
            ByteBuffer[] buffers,
            ByteBuffer terminalBuffer
    ) throws IOException {
        Objects.requireNonNull(write, "write").write(
                Objects.requireNonNull(buffers, "buffers")
        );
        return !Objects.requireNonNull(terminalBuffer, "terminalBuffer").hasRemaining();
    }

    long lastWriteBytes() {
        return lastWriteBytes;
    }

    boolean needsBatchLatency() {
        return !batchLatencyCaptured;
    }

    void captureBatchLatency(long nowNanos) {
        batchLatencyMicros = Math.max(0L, nowNanos - batchStartNanos) / 1_000L;
        batchLatencyCaptured = true;
    }

    void ensureReadCapacity() throws IOException {
        if (readBuffer.hasRemaining()) {
            return;
        }
        throw new IOException("benchmark reply decoder left the read buffer full");
    }

    void awaitRead() {
        phase = Phase.READING;
        selectionKey.interestOps(SelectionKey.OP_READ);
    }

    void readyForNextBatch() {
        phase = Phase.READY;
        selectionKey.interestOps(SelectionKey.OP_WRITE);
    }

    void suspend() {
        if (selectionKey.isValid()) {
            selectionKey.interestOps(0);
        }
    }

    @Override
    public void close() throws IOException {
        phase = Phase.DONE;
        if (selectionKey != null) {
            selectionKey.cancel();
        }
        channel.close();
    }
}
