package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

final class ScriptedRespServer implements AutoCloseable {
    private static final byte[] PONG = ascii("+PONG\r\n");
    private static final AtomicInteger SERVER_IDS = new AtomicInteger();

    private final ServerSocket serverSocket;
    private final int expectedClients;
    private final ResponseScript responseScript;
    private final ExecutorService executor;
    private final AtomicInteger acceptedConnections = new AtomicInteger();
    private final AtomicInteger commands = new AtomicInteger();
    private final AtomicInteger sentReplies = new AtomicInteger();
    private final AtomicInteger clockCalls = new AtomicInteger();
    private final AtomicIntegerArray commandsPerConnection;
    private final AtomicReferenceArray<Connection> connections;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Object stateChanged = new Object();

    private volatile boolean closed;

    private ScriptedRespServer(int expectedClients, ResponseScript responseScript) throws IOException {
        if (expectedClients <= 0) {
            throw new IllegalArgumentException("expectedClients must be > 0");
        }
        this.expectedClients = expectedClients;
        this.responseScript = Objects.requireNonNull(responseScript, "responseScript");
        this.commandsPerConnection = new AtomicIntegerArray(expectedClients);
        this.connections = new AtomicReferenceArray<>(expectedClients);
        int serverId = SERVER_IDS.incrementAndGet();
        this.executor = Executors.newCachedThreadPool(task -> {
            Thread thread = new Thread(task, "scripted-resp-server-" + serverId);
            thread.setDaemon(true);
            return thread;
        });
        this.serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        responseScript.attach(this);
        execute(this::acceptClients);
    }

    static ScriptedRespServer immediatePong(int expectedClients) throws IOException {
        return immediate(expectedClients, PONG);
    }

    static ScriptedRespServer batchedPong(int expectedClients, int batchSize) throws IOException {
        return batchedPong(expectedClients, batchSize, () -> {
        });
    }

    static ScriptedRespServer batchedPong(
            int expectedClients,
            int batchSize,
            Runnable immediatelyBeforeResponse
    ) throws IOException {
        return batched(expectedClients, PONG, batchSize, immediatelyBeforeResponse);
    }

    static ScriptedRespServer immediate(int expectedClients, byte[] reply) throws IOException {
        return new ScriptedRespServer(expectedClients, new ImmediateScript(reply));
    }

    static ScriptedRespServer batched(
            int expectedClients,
            byte[] reply,
            int batchSize,
            Runnable immediatelyBeforeResponse
    ) throws IOException {
        return new ScriptedRespServer(
                expectedClients,
                new BatchedScript(reply, batchSize, immediatelyBeforeResponse)
        );
    }

    static ScriptedRespServer fragmented(
            int expectedClients,
            byte[] reply,
            int splitAt,
            Runnable betweenFragments
    ) throws IOException {
        return new ScriptedRespServer(
                expectedClients,
                new FragmentedScript(reply, splitAt, betweenFragments)
        );
    }

    static ScriptedRespServer error(int expectedClients, String message) throws IOException {
        Objects.requireNonNull(message, "message");
        return immediate(expectedClients, ascii("-" + message + "\r\n"));
    }

    static ScriptedRespServer closeAfterCommand(
            int expectedClients,
            int connectionCommandNumber
    ) throws IOException {
        return new ScriptedRespServer(
                expectedClients,
                new CloseScript(connectionCommandNumber)
        );
    }

    static ScriptedRespServer thresholdBoundaryScenario() throws IOException {
        return new ScriptedRespServer(3, new ThresholdBoundaryScript());
    }

    int port() {
        return serverSocket.getLocalPort();
    }

    String host() {
        return serverSocket.getInetAddress().getHostAddress();
    }

    int acceptedConnectionCount() {
        return acceptedConnections.get();
    }

    int commandCount() {
        return commands.get();
    }

    int sentReplyCount() {
        return sentReplies.get();
    }

    boolean stallWasReleased() {
        return responseScript.stallWasReleased();
    }

    BenchmarkClock coordinatedClock(BenchmarkClock delegate) {
        Objects.requireNonNull(delegate, "delegate");
        return () -> {
            long now = delegate.nanoTime();
            clockCalls.incrementAndGet();
            signalStateChanged();
            return now;
        };
    }

    List<Integer> awaitCommandCountsPerConnection(
            int expectedTotal,
            int expectedConnectionCount,
            Duration timeout
    ) throws InterruptedException {
        awaitState(
                () -> commands.get() >= expectedTotal
                        && acceptedConnections.get() >= expectedConnectionCount,
                timeout,
                "command counts"
        );
        List<Integer> counts = new ArrayList<>(expectedConnectionCount);
        for (int index = 0; index < expectedConnectionCount; index++) {
            counts.add(commandsPerConnection.get(index));
        }
        Collections.sort(counts);
        return List.copyOf(counts);
    }

    void awaitSentReplies(int expected, Duration timeout) throws InterruptedException {
        awaitState(() -> sentReplies.get() >= expected, timeout, "sent replies");
    }

    void releaseStall() throws IOException {
        responseScript.releaseStall();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        signalStateChanged();
        IOException closeFailure = null;
        try {
            serverSocket.close();
        } catch (IOException e) {
            closeFailure = e;
        }
        for (int index = 0; index < connections.length(); index++) {
            Connection connection = connections.get(index);
            if (connection == null) {
                continue;
            }
            try {
                connection.close();
            } catch (IOException e) {
                if (closeFailure == null) {
                    closeFailure = e;
                } else {
                    closeFailure.addSuppressed(e);
                }
            }
        }
        executor.shutdownNow();
        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private void acceptClients() {
        try {
            for (int connectionId = 0; connectionId < expectedClients && !closed; connectionId++) {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                Connection connection = new Connection(connectionId, socket);
                connections.set(connectionId, connection);
                acceptedConnections.incrementAndGet();
                signalStateChanged();
                execute(() -> readCommands(connection));
            }
        } catch (SocketException e) {
            if (!closed) {
                recordFailure(e);
            }
        } catch (Throwable e) {
            recordFailure(e);
        }
    }

    private void readCommands(Connection connection) {
        try {
            InputStream input = connection.input();
            while (!closed) {
                int first = input.read();
                if (first < 0) {
                    return;
                }
                if (first == '*') {
                    readRespArray(input);
                } else {
                    readInline(input, first);
                }
                int connectionCommands = commandsPerConnection.incrementAndGet(connection.id());
                int totalCommands = commands.incrementAndGet();
                signalStateChanged();
                responseScript.onCommand(connection, connectionCommands, totalCommands);
            }
        } catch (SocketException e) {
            if (!closed && !connection.isClosed()) {
                recordFailure(e);
            }
        } catch (Throwable e) {
            if (!closed) {
                recordFailure(e);
            }
        }
    }

    private void readRespArray(InputStream input) throws IOException {
        long argumentCount = readLongLine(input, "array length");
        if (argumentCount <= 0 || argumentCount > RespProtocolLimits.DEFAULT_MAX_ARGS) {
            throw new IOException("invalid RESP command argument count: " + argumentCount);
        }
        for (long argument = 0; argument < argumentCount; argument++) {
            if (input.read() != '$') {
                throw new IOException("RESP command argument must be a bulk string");
            }
            long length = readLongLine(input, "bulk length");
            if (length < 0 || length > RespProtocolLimits.DEFAULT_MAX_BULK_BYTES) {
                throw new IOException("invalid RESP command bulk length: " + length);
            }
            input.skipNBytes(length);
            requireCrlf(input);
        }
    }

    private void readInline(InputStream input, int first) throws IOException {
        int previous = first;
        int length = 1;
        while (true) {
            int current = input.read();
            if (current < 0) {
                throw new EOFException("unexpected EOF in inline command");
            }
            length++;
            if (length > RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES) {
                throw new IOException("inline command exceeds protocol limit");
            }
            if (previous == '\r' && current == '\n') {
                return;
            }
            if (current == '\n') {
                throw new IOException("inline LF is not preceded by CR");
            }
            previous = current;
        }
    }

    private long readLongLine(InputStream input, String field) throws IOException {
        int current = input.read();
        if (current < 0) {
            throw new EOFException("unexpected EOF before " + field);
        }
        boolean negative = current == '-';
        if (negative) {
            current = input.read();
        }
        if (current < '0' || current > '9') {
            throw new IOException("invalid " + field);
        }
        long value = 0;
        while (current >= '0' && current <= '9') {
            int digit = current - '0';
            if (value > (Long.MAX_VALUE - digit) / 10) {
                throw new IOException(field + " overflows a signed long");
            }
            value = value * 10 + digit;
            current = input.read();
        }
        if (current != '\r' || input.read() != '\n') {
            throw new IOException("invalid terminator after " + field);
        }
        return negative ? -value : value;
    }

    private static void requireCrlf(InputStream input) throws IOException {
        if (input.read() != '\r' || input.read() != '\n') {
            throw new IOException("expected CRLF after bulk string");
        }
    }

    private void execute(Runnable task) {
        executor.execute(() -> {
            try {
                task.run();
            } catch (Throwable e) {
                if (!closed) {
                    recordFailure(e);
                }
            }
        });
    }

    private Connection connection(int id) {
        Connection connection = connections.get(id);
        if (connection == null) {
            throw new IllegalStateException("connection " + id + " has not been accepted");
        }
        return connection;
    }

    private boolean everyConnectionHasAtLeast(int expectedCommands) {
        if (acceptedConnections.get() < expectedClients) {
            return false;
        }
        for (int index = 0; index < expectedClients; index++) {
            if (commandsPerConnection.get(index) < expectedCommands) {
                return false;
            }
        }
        return true;
    }

    private int awaitClockCallAfter(int observedCalls, Duration timeout) throws InterruptedException {
        awaitState(() -> clockCalls.get() > observedCalls, timeout, "runner clock call");
        return clockCalls.get();
    }

    private void awaitState(StateProbe probe, Duration timeout, String description)
            throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (stateChanged) {
            while (!probe.isSatisfied()) {
                throwIfFailed();
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new AssertionError("timed out waiting for " + description);
                }
                TimeUnit.NANOSECONDS.timedWait(stateChanged, remaining);
            }
        }
        throwIfFailed();
    }

    private void recordFailure(Throwable throwable) {
        failure.compareAndSet(null, throwable);
        signalStateChanged();
    }

    private void throwIfFailed() {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throw new AssertionError("scripted RESP server failed", throwable);
        }
    }

    private void signalStateChanged() {
        synchronized (stateChanged) {
            stateChanged.notifyAll();
        }
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] repeated(byte[] bytes, int count) {
        byte[] repeated = new byte[Math.multiplyExact(bytes.length, count)];
        for (int index = 0; index < count; index++) {
            System.arraycopy(bytes, 0, repeated, index * bytes.length, bytes.length);
        }
        return repeated;
    }

    @FunctionalInterface
    private interface StateProbe {
        boolean isSatisfied();
    }

    private interface ResponseScript {
        default void attach(ScriptedRespServer server) {
        }

        void onCommand(Connection connection, int connectionCommandCount, int totalCommandCount)
                throws Exception;

        default boolean stallWasReleased() {
            return false;
        }

        default void releaseStall() throws IOException {
            throw new UnsupportedOperationException("this response script has no stall");
        }
    }

    private final class Connection implements AutoCloseable {
        private final int id;
        private final Socket socket;
        private final InputStream input;
        private final OutputStream output;
        private final Object writeLock = new Object();

        private Connection(int id, Socket socket) throws IOException {
            this.id = id;
            this.socket = socket;
            this.input = socket.getInputStream();
            this.output = socket.getOutputStream();
        }

        int id() {
            return id;
        }

        InputStream input() {
            return input;
        }

        boolean isClosed() {
            return socket.isClosed();
        }

        void sendReplies(byte[] reply, int count) throws IOException {
            byte[] response = repeated(reply, count);
            synchronized (writeLock) {
                output.write(response);
                output.flush();
            }
            sentReplies.addAndGet(count);
            signalStateChanged();
        }

        void sendFragments(byte[] first, byte[] second, Runnable betweenFragments)
                throws IOException {
            synchronized (writeLock) {
                output.write(first);
                output.flush();
                betweenFragments.run();
                output.write(second);
                output.flush();
            }
            sentReplies.incrementAndGet();
            signalStateChanged();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private static final class ImmediateScript implements ResponseScript {
        private final byte[] reply;

        private ImmediateScript(byte[] reply) {
            this.reply = Objects.requireNonNull(reply, "reply").clone();
        }

        @Override
        public void onCommand(Connection connection, int connectionCommandCount, int totalCommandCount)
                throws IOException {
            connection.sendReplies(reply, 1);
        }
    }

    private static final class BatchedScript implements ResponseScript {
        private final byte[] reply;
        private final int batchSize;
        private final Runnable immediatelyBeforeResponse;

        private BatchedScript(byte[] reply, int batchSize, Runnable immediatelyBeforeResponse) {
            this.reply = Objects.requireNonNull(reply, "reply").clone();
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be > 0");
            }
            this.batchSize = batchSize;
            this.immediatelyBeforeResponse = Objects.requireNonNull(
                    immediatelyBeforeResponse,
                    "immediatelyBeforeResponse"
            );
        }

        @Override
        public void onCommand(Connection connection, int connectionCommandCount, int totalCommandCount)
                throws IOException {
            if (connectionCommandCount % batchSize == 0) {
                immediatelyBeforeResponse.run();
                connection.sendReplies(reply, batchSize);
            }
        }
    }

    private static final class FragmentedScript implements ResponseScript {
        private final byte[] first;
        private final byte[] second;
        private final Runnable betweenFragments;

        private FragmentedScript(byte[] reply, int splitAt, Runnable betweenFragments) {
            Objects.requireNonNull(reply, "reply");
            if (splitAt <= 0 || splitAt >= reply.length) {
                throw new IllegalArgumentException("splitAt must be within reply");
            }
            this.first = new byte[splitAt];
            this.second = new byte[reply.length - splitAt];
            System.arraycopy(reply, 0, first, 0, first.length);
            System.arraycopy(reply, splitAt, second, 0, second.length);
            this.betweenFragments = Objects.requireNonNull(betweenFragments, "betweenFragments");
        }

        @Override
        public void onCommand(Connection connection, int connectionCommandCount, int totalCommandCount)
                throws IOException {
            connection.sendFragments(first, second, betweenFragments);
        }
    }

    private static final class CloseScript implements ResponseScript {
        private final int connectionCommandNumber;

        private CloseScript(int connectionCommandNumber) {
            if (connectionCommandNumber <= 0) {
                throw new IllegalArgumentException("connectionCommandNumber must be > 0");
            }
            this.connectionCommandNumber = connectionCommandNumber;
        }

        @Override
        public void onCommand(Connection connection, int connectionCommandCount, int totalCommandCount)
                throws IOException {
            if (connectionCommandCount == connectionCommandNumber) {
                connection.close();
            }
        }
    }

    private static final class ThresholdBoundaryScript implements ResponseScript {
        private static final Duration COORDINATION_TIMEOUT = Duration.ofSeconds(5);

        private final AtomicBoolean coordinationStarted = new AtomicBoolean();
        private final AtomicBoolean stallReleased = new AtomicBoolean();
        private ScriptedRespServer server;

        @Override
        public void attach(ScriptedRespServer server) {
            this.server = server;
        }

        @Override
        public void onCommand(Connection connection, int connectionCommandCount, int totalCommandCount) {
            if (totalCommandCount >= 9
                    && server.everyConnectionHasAtLeast(3)
                    && coordinationStarted.compareAndSet(false, true)) {
                server.execute(this::coordinateReplies);
            }
        }

        @Override
        public boolean stallWasReleased() {
            return stallReleased.get();
        }

        @Override
        public void releaseStall() throws IOException {
            if (stallReleased.compareAndSet(false, true)) {
                server.connection(1).sendReplies(PONG, 2);
            }
        }

        private void coordinateReplies() {
            try {
                int observedClockCalls = server.clockCalls.get();
                server.connection(0).sendReplies(PONG, 3);
                observedClockCalls = server.awaitClockCallAfter(
                        observedClockCalls,
                        COORDINATION_TIMEOUT
                );
                server.connection(1).sendReplies(PONG, 1);
                server.awaitClockCallAfter(observedClockCalls, COORDINATION_TIMEOUT);
                server.connection(2).sendReplies(PONG, 3);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("threshold scenario coordination was interrupted", e);
            } catch (IOException e) {
                throw new AssertionError("threshold scenario could not send replies", e);
            }
        }
    }
}
