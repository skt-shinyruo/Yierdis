package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class NioBenchmarkRunner implements BenchmarkCaseExecutor {
    private static final byte[] AUTH = ascii("AUTH");
    private static final byte[] SELECT = ascii("SELECT");
    private static final byte[] EMPTY_PREFIX = new byte[0];
    private static final Duration DEFAULT_NO_PROGRESS_TIMEOUT = Duration.ofSeconds(30);
    private static final ClientWriteFactory DEFAULT_CLIENT_WRITE_FACTORY =
            channel -> channel::write;

    private final BenchmarkClock clock;
    private final NioBenchmarkClient.ReplyLimits replyLimits;
    private final BenchmarkClock progressClock;
    private final Duration noProgressTimeout;
    private final long noProgressTimeoutNanos;
    private final ClientCleanup clientCleanup;
    private final ClientWriteFactory clientWriteFactory;

    public NioBenchmarkRunner() {
        this(
                BenchmarkClock.system(),
                NioBenchmarkClient.ReplyLimits.defaults(),
                BenchmarkClock.system(),
                DEFAULT_NO_PROGRESS_TIMEOUT,
                NioBenchmarkClient::close,
                DEFAULT_CLIENT_WRITE_FACTORY
        );
    }

    public NioBenchmarkRunner(BenchmarkClock clock) {
        this(
                clock,
                NioBenchmarkClient.ReplyLimits.defaults(),
                BenchmarkClock.system(),
                DEFAULT_NO_PROGRESS_TIMEOUT,
                NioBenchmarkClient::close,
                DEFAULT_CLIENT_WRITE_FACTORY
        );
    }

    NioBenchmarkRunner(
            BenchmarkClock clock,
            NioBenchmarkClient.ReplyLimits replyLimits
    ) {
        this(
                clock,
                replyLimits,
                BenchmarkClock.system(),
                DEFAULT_NO_PROGRESS_TIMEOUT,
                NioBenchmarkClient::close,
                DEFAULT_CLIENT_WRITE_FACTORY
        );
    }

    NioBenchmarkRunner(
            BenchmarkClock clock,
            NioBenchmarkClient.ReplyLimits replyLimits,
            BenchmarkClock progressClock,
            Duration noProgressTimeout
    ) {
        this(
                clock,
                replyLimits,
                progressClock,
                noProgressTimeout,
                NioBenchmarkClient::close,
                DEFAULT_CLIENT_WRITE_FACTORY
        );
    }

    NioBenchmarkRunner(
            BenchmarkClock clock,
            NioBenchmarkClient.ReplyLimits replyLimits,
            BenchmarkClock progressClock,
            Duration noProgressTimeout,
            ClientCleanup clientCleanup
    ) {
        this(
                clock,
                replyLimits,
                progressClock,
                noProgressTimeout,
                clientCleanup,
                DEFAULT_CLIENT_WRITE_FACTORY
        );
    }

    NioBenchmarkRunner(
            BenchmarkClock clock,
            NioBenchmarkClient.ReplyLimits replyLimits,
            BenchmarkClock progressClock,
            Duration noProgressTimeout,
            ClientCleanup clientCleanup,
            ClientWriteFactory clientWriteFactory
    ) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.replyLimits = Objects.requireNonNull(replyLimits, "replyLimits");
        this.progressClock = Objects.requireNonNull(progressClock, "progressClock");
        this.noProgressTimeout = Objects.requireNonNull(
                noProgressTimeout,
                "noProgressTimeout"
        );
        if (noProgressTimeout.isZero() || noProgressTimeout.isNegative()) {
            throw new IllegalArgumentException("noProgressTimeout must be > 0");
        }
        this.noProgressTimeoutNanos = noProgressTimeout.toNanos();
        this.clientCleanup = Objects.requireNonNull(clientCleanup, "clientCleanup");
        this.clientWriteFactory = Objects.requireNonNull(
                clientWriteFactory,
                "clientWriteFactory"
        );
    }

    @Override
    public BenchmarkStatistics execute(
            RedisBenchmarkCase testCase,
            BenchmarkConfig config,
            byte[] payload,
            BenchmarkRandom random
    ) throws Exception {
        RedisBenchmarkCase requiredCase = Objects.requireNonNull(testCase, "testCase");
        BenchmarkConfig requiredConfig = Objects.requireNonNull(config, "config");
        byte[] requiredPayload = Objects.requireNonNull(payload, "payload");
        BenchmarkRandom requiredRandom = Objects.requireNonNull(random, "random");

        PreparedPipeline compiledPipeline = requiredCase.template().prepare(
                requiredConfig.pipeline(),
                requiredPayload,
                requiredConfig.keyspace()
        );
        PreparedPrefix prefix = preparePrefix(requiredConfig);
        BenchmarkLatencyRecorder latencyRecorder =
                new BenchmarkLatencyRecorder(requiredConfig.precision());
        List<NioBenchmarkClient> clients = new ArrayList<>(requiredConfig.clients());
        RunState state = new RunState(requiredConfig.requests(), requiredConfig.pipeline());

        Throwable failure = null;
        try (Selector selector = Selector.open()) {
            registerClients(
                    selector,
                    clients,
                    compiledPipeline,
                    prefix,
                    requiredConfig,
                    requiredRandom
            );
            state.measuredStartNanos = clock.nanoTime();
            state.lastProgressNanos = progressClock.nanoTime();
            runSelector(
                    selector,
                    clients,
                    compiledPipeline,
                    prefix,
                    requiredConfig,
                    requiredCase.replyExpectation(),
                    requiredRandom,
                    latencyRecorder,
                    state
            );
        } catch (Throwable executionFailure) {
            failure = executionFailure;
        }
        try {
            closeClients(clients);
        } catch (Throwable closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else if (failure != closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            throwExecutionFailure(requiredCase, requiredConfig, state, failure);
        }

        long elapsedNanos = Math.max(0L, state.stopNanos - state.measuredStartNanos);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
        return BenchmarkStatistics.from(
                requiredConfig.requests(),
                state.completedReplies,
                state.issued,
                state.histogramSamples,
                elapsedMillis,
                latencyRecorder.summary()
        );
    }

    private void registerClients(
            Selector selector,
            List<NioBenchmarkClient> clients,
            PreparedPipeline compiledPipeline,
            PreparedPrefix prefix,
            BenchmarkConfig config,
            BenchmarkRandom random
    ) throws IOException {
        InetSocketAddress address = new InetSocketAddress(config.host(), config.port());
        for (int index = 0; index < config.clients(); index++) {
            clients.add(registerClient(
                    selector,
                    address,
                    compiledPipeline,
                    prefix,
                    random
            ));
        }
    }

    private NioBenchmarkClient registerClient(
            Selector selector,
            InetSocketAddress address,
            PreparedPipeline compiledPipeline,
            PreparedPrefix prefix,
            BenchmarkRandom random
    ) throws IOException {
        SocketChannel channel = SocketChannel.open();
        try {
            channel.configureBlocking(false);
            channel.socket().setTcpNoDelay(true);
            NioBenchmarkClient client = new NioBenchmarkClient(
                    channel,
                    clientWriteFactory.create(channel),
                    compiledPipeline,
                    prefix.bytes(),
                    prefix.replyCount(),
                    random,
                    replyLimits
            );
            boolean connected = channel.connect(address);
            int interestOps = connected ? SelectionKey.OP_WRITE : SelectionKey.OP_CONNECT;
            SelectionKey key = channel.register(selector, interestOps, client);
            client.register(key, connected);
            return client;
        } catch (IOException | RuntimeException failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw new IOException("connect failure: " + failureDetail(failure), failure);
        } catch (Error failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private void runSelector(
            Selector selector,
            List<NioBenchmarkClient> clients,
            PreparedPipeline compiledPipeline,
            PreparedPrefix prefix,
            BenchmarkConfig config,
            BenchmarkReplyExpectation expectation,
            BenchmarkRandom random,
            BenchmarkLatencyRecorder latencyRecorder,
            RunState state
    ) throws IOException {
        while (!state.stopping) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException("benchmark execution was interrupted");
            }
            int readyKeys = selector.select(selectTimeoutMillis(state));
            if (readyKeys == 0) {
                throwIfProgressTimedOut(state);
                continue;
            }
            Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
            while (selectedKeys.hasNext()) {
                SelectionKey key = selectedKeys.next();
                selectedKeys.remove();
                if (!key.isValid()) {
                    continue;
                }
                NioBenchmarkClient client = (NioBenchmarkClient) key.attachment();
                if (state.thresholdClient != null && client != state.thresholdClient) {
                    client.suspend();
                    continue;
                }
                if (key.isConnectable()) {
                    finishConnect(client, random, state);
                }
                if (key.isValid() && key.isWritable()) {
                    writeIfReady(client, random, state);
                }
                if (key.isValid() && key.isReadable()) {
                    readReplies(
                            selector,
                            client,
                            clients,
                            compiledPipeline,
                            prefix,
                            config,
                            expectation,
                            random,
                            latencyRecorder,
                            state
                    );
                }
                if (state.stopping) {
                    break;
                }
            }
            if (!state.stopping) {
                throwIfProgressTimedOut(state);
            }
        }
    }

    private void finishConnect(
            NioBenchmarkClient client,
            BenchmarkRandom random,
            RunState state
    ) throws IOException {
        try {
            if (!client.channel.finishConnect()) {
                return;
            }
        } catch (IOException failure) {
            throw new IOException("connect failure: " + failureDetail(failure), failure);
        }
        markProgress(state);
        client.phase = NioBenchmarkClient.Phase.READY;
        client.selectionKey.interestOps(SelectionKey.OP_WRITE);
        writeIfReady(client, random, state);
    }

    private void writeIfReady(
            NioBenchmarkClient client,
            BenchmarkRandom random,
            RunState state
    ) throws IOException {
        if (client.phase == NioBenchmarkClient.Phase.READY) {
            if (state.issued >= state.requested) {
                client.suspend();
                return;
            }
            state.issued += state.pipeline;
            client.beginBatch(state.pipeline, random);
            client.batchStartNanos = clock.nanoTime();
        }
        if (client.phase == NioBenchmarkClient.Phase.WRITING) {
            boolean complete = client.writeBatch();
            if (client.lastWriteBytes() > 0) {
                markProgress(state);
            }
            if (complete) {
                client.awaitRead();
            }
        }
    }

    private void readReplies(
            Selector selector,
            NioBenchmarkClient client,
            List<NioBenchmarkClient> clients,
            PreparedPipeline compiledPipeline,
            PreparedPrefix prefix,
            BenchmarkConfig config,
            BenchmarkReplyExpectation expectation,
            BenchmarkRandom random,
            BenchmarkLatencyRecorder latencyRecorder,
            RunState state
    ) throws IOException {
        if (client.needsBatchLatency()) {
            client.captureBatchLatency(clock.nanoTime());
        }
        client.ensureReadCapacity();
        int read = client.channel.read(client.readBuffer);
        if (read < 0) {
            throw new EOFException("disconnect with benchmark replies outstanding");
        }
        if (read == 0) {
            return;
        }
        markProgress(state);

        client.readBuffer.flip();
        try {
            while (true) {
                BenchmarkRespReply reply = client.replyDecoder.tryDecode(client.readBuffer);
                if (reply == null) {
                    return;
                }
                if (client.prefixPending > 0) {
                    validatePrefixReply(reply);
                    client.prefixPending--;
                    continue;
                }
                if (client.pendingReplies <= 0) {
                    throw new IOException("benchmark server sent an unexpected extra reply");
                }
                validateMeasuredReply(expectation, reply);
                state.completedReplies++;
                if (state.histogramSamples < state.requested) {
                    latencyRecorder.recordMicros(client.batchLatencyMicros);
                    state.histogramSamples++;
                    if (state.histogramSamples == state.requested) {
                        state.thresholdClient = client;
                        suspendOtherClients(clients, client);
                    }
                }
                client.pendingReplies--;
                if (client.pendingReplies == 0) {
                    if (client.readBuffer.hasRemaining()) {
                        throw new IOException(
                                "benchmark server sent an unexpected extra reply"
                        );
                    }
                    if (state.thresholdClient == client) {
                        state.stopNanos = clock.nanoTime();
                        state.stopping = true;
                    } else if (!config.keepAlive()) {
                        replaceClient(
                                selector,
                                clients,
                                client,
                                compiledPipeline,
                                prefix,
                                config,
                                random,
                                state
                        );
                    } else {
                        client.readyForNextBatch();
                    }
                    return;
                }
            }
        } finally {
            client.readBuffer.compact();
        }
    }

    private void replaceClient(
            Selector selector,
            List<NioBenchmarkClient> clients,
            NioBenchmarkClient oldClient,
            PreparedPipeline compiledPipeline,
            PreparedPrefix prefix,
            BenchmarkConfig config,
            BenchmarkRandom random,
            RunState state
    ) throws IOException {
        NioBenchmarkClient replacement = registerClient(
                selector,
                new InetSocketAddress(config.host(), config.port()),
                compiledPipeline,
                prefix,
                random
        );
        if (replacement.phase == NioBenchmarkClient.Phase.READY) {
            markProgress(state);
        }
        clients.add(replacement);
        oldClient.close();
        clients.remove(oldClient);
    }

    private static void suspendOtherClients(
            List<NioBenchmarkClient> clients,
            NioBenchmarkClient thresholdClient
    ) {
        for (NioBenchmarkClient client : clients) {
            if (client != thresholdClient && client.phase != NioBenchmarkClient.Phase.DONE) {
                client.suspend();
            }
        }
    }

    private static void validatePrefixReply(BenchmarkRespReply reply) throws IOException {
        validateReply(BenchmarkReplyExpectation.OK, reply);
    }

    private static void validateMeasuredReply(
            BenchmarkReplyExpectation expectation,
            BenchmarkRespReply reply
    ) throws IOException {
        validateReply(expectation, reply);
    }

    private static void validateReply(
            BenchmarkReplyExpectation expectation,
            BenchmarkRespReply reply
    ) throws IOException {
        String detail = expectation.failureDetail(reply);
        if (detail != null) {
            throw new IOException(detail);
        }
    }

    private static void throwExecutionFailure(
            RedisBenchmarkCase testCase,
            BenchmarkConfig config,
            RunState state,
            Throwable failure
    ) throws BenchmarkExecutionException {
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof BenchmarkExecutionException executionFailure) {
            throw executionFailure;
        }
        throw new BenchmarkExecutionException(
                testCase.title(),
                state.completedReplies,
                config.requests(),
                failureDetail(failure),
                failure
        );
    }

    private static String failureDetail(Throwable failure) {
        String message = failure.getMessage();
        if (message == null || message.isBlank()) {
            return failure.getClass().getSimpleName();
        }
        return message;
    }

    private long selectTimeoutMillis(RunState state) throws IOException {
        long remainingNanos = remainingProgressNanos(state);
        long timeoutMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
        if (TimeUnit.MILLISECONDS.toNanos(timeoutMillis) < remainingNanos) {
            timeoutMillis++;
        }
        return Math.max(1L, timeoutMillis);
    }

    private long remainingProgressNanos(RunState state) throws IOException {
        long elapsedNanos = progressClock.nanoTime() - state.lastProgressNanos;
        if (elapsedNanos >= noProgressTimeoutNanos) {
            throw noProgressTimeout();
        }
        return noProgressTimeoutNanos - Math.max(0L, elapsedNanos);
    }

    private void throwIfProgressTimedOut(RunState state) throws IOException {
        remainingProgressNanos(state);
    }

    private void markProgress(RunState state) {
        state.lastProgressNanos = progressClock.nanoTime();
    }

    private IOException noProgressTimeout() {
        return new IOException("no progress timeout after " + noProgressTimeout);
    }

    private static PreparedPrefix preparePrefix(BenchmarkConfig config) {
        List<byte[]> frames = new ArrayList<>(2);
        if (!config.password().isEmpty()) {
            List<byte[]> arguments = new ArrayList<>(3);
            arguments.add(AUTH);
            if (!config.username().isEmpty()) {
                arguments.add(utf8(config.username()));
            }
            arguments.add(utf8(config.password()));
            frames.add(RespClientCodec.encodeCommand(arguments));
        }
        if (config.database() != 0) {
            frames.add(RespClientCodec.encodeCommand(List.of(
                    SELECT,
                    ascii(Integer.toString(config.database()))
            )));
        }
        if (frames.isEmpty()) {
            return new PreparedPrefix(EMPTY_PREFIX, 0);
        }
        int length = 0;
        for (byte[] frame : frames) {
            length = Math.addExact(length, frame.length);
        }
        byte[] bytes = new byte[length];
        int offset = 0;
        for (byte[] frame : frames) {
            System.arraycopy(frame, 0, bytes, offset, frame.length);
            offset += frame.length;
        }
        return new PreparedPrefix(bytes, frames.size());
    }

    private void closeClients(List<NioBenchmarkClient> clients) throws Throwable {
        Throwable failure = null;
        for (NioBenchmarkClient client : clients) {
            try {
                clientCleanup.close(client);
            } catch (Throwable e) {
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private record PreparedPrefix(byte[] bytes, int replyCount) {
    }

    @FunctionalInterface
    interface ClientCleanup {
        void close(NioBenchmarkClient client) throws IOException;
    }

    @FunctionalInterface
    interface ClientWriteFactory {
        NioBenchmarkClient.GatheringWrite create(SocketChannel channel);
    }

    private static final class RunState {
        private final int requested;
        private final int pipeline;

        private long issued;
        private long completedReplies;
        private long histogramSamples;
        private long measuredStartNanos;
        private long lastProgressNanos;
        private long stopNanos;
        private NioBenchmarkClient thresholdClient;
        private boolean stopping;

        private RunState(int requested, int pipeline) {
            this.requested = requested;
            this.pipeline = pipeline;
        }
    }
}
