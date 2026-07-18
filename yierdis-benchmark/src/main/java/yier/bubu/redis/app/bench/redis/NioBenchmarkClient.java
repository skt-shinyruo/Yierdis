package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Objects;

final class NioBenchmarkClient implements AutoCloseable {
    private static final int INITIAL_READ_BUFFER_BYTES = 64 * 1024;

    enum Phase {
        CONNECTING,
        READY,
        WRITING,
        READING,
        DONE
    }

    final SocketChannel channel;
    SelectionKey selectionKey;
    final PreparedPipeline preparedPipeline;
    final ByteBuffer firstWritePrefix;
    final ByteBuffer[] gatheringWriteBuffers;
    final ByteBuffer pipelineWriteBuffer;
    ByteBuffer readBuffer;
    final IncrementalRespReplyDecoder replyDecoder;

    int prefixPending;
    int pendingReplies;
    long batchStartNanos;
    long batchLatencyMicros;
    Phase phase;

    private boolean firstBatch = true;
    private boolean batchLatencyCaptured;

    NioBenchmarkClient(
            SocketChannel channel,
            PreparedPipeline compiledPipeline,
            byte[] prefix,
            int prefixReplies,
            BenchmarkRandom random
    ) {
        this.channel = Objects.requireNonNull(channel, "channel");
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
        this.readBuffer = ByteBuffer.allocate(INITIAL_READ_BUFFER_BYTES);
        this.replyDecoder = new IncrementalRespReplyDecoder(
                RespProtocolLimits.DEFAULT_MAX_BULK_BYTES,
                RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES,
                RespProtocolLimits.DEFAULT_MAX_ARGS,
                IncrementalRespReplyDecoder.MAX_SUPPORTED_DEPTH
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
        channel.write(gatheringWriteBuffers);
        return !pipelineWriteBuffer.hasRemaining();
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
        int currentCapacity = readBuffer.capacity();
        int maximumCapacity = RespProtocolLimits.DEFAULT_MAX_BULK_BYTES;
        if (currentCapacity >= maximumCapacity) {
            throw new IOException("benchmark reply exceeds the read buffer limit");
        }
        int nextCapacity = (int) Math.min(
                (long) maximumCapacity,
                (long) currentCapacity * 2L
        );
        ByteBuffer larger = ByteBuffer.allocate(nextCapacity);
        readBuffer.flip();
        larger.put(readBuffer);
        readBuffer = larger;
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
