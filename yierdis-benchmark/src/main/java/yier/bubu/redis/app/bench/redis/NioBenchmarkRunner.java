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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class NioBenchmarkRunner implements BenchmarkCaseExecutor {
    private static final byte[] AUTH = ascii("AUTH");
    private static final byte[] SELECT = ascii("SELECT");
    private static final byte[] EMPTY_PREFIX = new byte[0];

    private final BenchmarkClock clock;

    public NioBenchmarkRunner() {
        this(BenchmarkClock.system());
    }

    public NioBenchmarkRunner(BenchmarkClock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
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
        if (!requiredConfig.keepAlive()) {
            throw new UnsupportedOperationException("non-keepalive execution is not implemented");
        }

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
            runSelector(
                    selector,
                    clients,
                    requiredCase.replyExpectation(),
                    requiredRandom,
                    latencyRecorder,
                    state
            );
        } finally {
            closeClients(clients);
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
            SocketChannel channel = SocketChannel.open();
            try {
                channel.configureBlocking(false);
                channel.socket().setTcpNoDelay(true);
                NioBenchmarkClient client = new NioBenchmarkClient(
                        channel,
                        compiledPipeline,
                        prefix.bytes(),
                        prefix.replyCount(),
                        random
                );
                boolean connected = channel.connect(address);
                int interestOps = connected ? SelectionKey.OP_WRITE : SelectionKey.OP_CONNECT;
                SelectionKey key = channel.register(selector, interestOps, client);
                client.register(key, connected);
                clients.add(client);
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }
    }

    private void runSelector(
            Selector selector,
            List<NioBenchmarkClient> clients,
            BenchmarkReplyExpectation expectation,
            BenchmarkRandom random,
            BenchmarkLatencyRecorder latencyRecorder,
            RunState state
    ) throws IOException {
        while (!state.stopping) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException("benchmark execution was interrupted");
            }
            selector.select();
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
                    readReplies(client, clients, expectation, latencyRecorder, state);
                }
                if (state.stopping) {
                    break;
                }
            }
        }
    }

    private void finishConnect(
            NioBenchmarkClient client,
            BenchmarkRandom random,
            RunState state
    ) throws IOException {
        if (!client.channel.finishConnect()) {
            return;
        }
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
                client.close();
                return;
            }
            state.issued += state.pipeline;
            client.beginBatch(state.pipeline, random);
            client.batchStartNanos = clock.nanoTime();
        }
        if (client.phase == NioBenchmarkClient.Phase.WRITING && client.writeBatch()) {
            client.awaitRead();
        }
    }

    private void readReplies(
            NioBenchmarkClient client,
            List<NioBenchmarkClient> clients,
            BenchmarkReplyExpectation expectation,
            BenchmarkLatencyRecorder latencyRecorder,
            RunState state
    ) throws IOException {
        if (client.needsBatchLatency()) {
            client.captureBatchLatency(clock.nanoTime());
        }
        client.ensureReadCapacity();
        int read = client.channel.read(client.readBuffer);
        if (read < 0) {
            throw new EOFException("benchmark connection closed with replies outstanding");
        }
        if (read == 0) {
            return;
        }

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
                    if (state.thresholdClient == client) {
                        state.stopNanos = clock.nanoTime();
                        state.stopping = true;
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
        if (reply.kind() != BenchmarkRespReply.Kind.SIMPLE_STRING || !"OK".equals(reply.text())) {
            throw new IOException("AUTH/SELECT prefix did not receive an OK reply");
        }
    }

    private static void validateMeasuredReply(
            BenchmarkReplyExpectation expectation,
            BenchmarkRespReply reply
    ) throws IOException {
        if (reply.kind() == BenchmarkRespReply.Kind.ERROR) {
            throw new IOException("benchmark command received a RESP error");
        }
        boolean matches = switch (expectation) {
            case PONG -> reply.kind() == BenchmarkRespReply.Kind.SIMPLE_STRING
                    && "PONG".equals(reply.text());
            case OK -> reply.kind() == BenchmarkRespReply.Kind.SIMPLE_STRING
                    && "OK".equals(reply.text());
            case INTEGER -> reply.kind() == BenchmarkRespReply.Kind.INTEGER;
            case BULK_OR_NULL -> reply.kind() == BenchmarkRespReply.Kind.BULK_STRING
                    || reply.kind() == BenchmarkRespReply.Kind.NULL_BULK;
            case ARRAY -> reply.kind() == BenchmarkRespReply.Kind.ARRAY;
        };
        if (!matches) {
            throw new IOException("benchmark reply does not match " + expectation);
        }
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

    private static void closeClients(List<NioBenchmarkClient> clients) throws IOException {
        IOException failure = null;
        for (NioBenchmarkClient client : clients) {
            try {
                client.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
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

    private static final class RunState {
        private final int requested;
        private final int pipeline;

        private long issued;
        private long completedReplies;
        private long histogramSamples;
        private long measuredStartNanos;
        private long stopNanos;
        private NioBenchmarkClient thresholdClient;
        private boolean stopping;

        private RunState(int requested, int pipeline) {
            this.requested = requested;
            this.pipeline = pipeline;
        }
    }
}
