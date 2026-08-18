package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.ByteArrayOutputStream;
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
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

final class ScriptedRespServer implements AutoCloseable {
    private static final byte[] PONG = ascii("+PONG\r\n");
    private static final byte[] OK = ascii("+OK\r\n");
    private static final int MAX_CAPTURED_ARGUMENT_BYTES = 1024;
    private static final AtomicInteger SERVER_IDS = new AtomicInteger();

    private final ServerSocket serverSocket;
    private final int expectedClients;
    private final ResponseScript responseScript;
    private final ExecutorService executor;
    private final AtomicInteger acceptedConnections = new AtomicInteger();
    private final AtomicInteger commands = new AtomicInteger();
    private final AtomicInteger sentReplies = new AtomicInteger();
    private final AtomicInteger measuredCommandReplies = new AtomicInteger();
    private final AtomicInteger clockCalls = new AtomicInteger();
    private final List<Connection> connections = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Object stateChanged = new Object();

    private volatile boolean closed;

    private ScriptedRespServer(int expectedClients, ResponseScript responseScript) throws IOException {
        if (expectedClients <= 0) {
            throw new IllegalArgumentException("expectedClients must be > 0");
        }
        this.expectedClients = expectedClients;
        this.responseScript = Objects.requireNonNull(responseScript, "responseScript");
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
        byte[] response = Objects.requireNonNull(reply, "reply").clone();
        return new ScriptedRespServer(
                expectedClients,
                (connection, command, connectionCount, totalCount) ->
                        connection.sendReplies(response, 1)
        );
    }

    static ScriptedRespServer respondingWith(String reply) throws IOException {
        return immediate(
                1,
                Objects.requireNonNull(reply, "reply")
                        .getBytes(StandardCharsets.US_ASCII)
        );
    }

    static ScriptedRespServer coalescedPongAndExtraReply() throws IOException {
        return immediate(1, ascii("+PONG\r\n+PONG\r\n"));
    }

    static ScriptedRespServer authAndSelectAware() throws IOException {
        return new ScriptedRespServer(1, new AuthAndSelectScript(null));
    }

    static ScriptedRespServer authAndSelectAware(Runnable beforePrefixReply)
            throws IOException {
        return new ScriptedRespServer(
                1,
                new AuthAndSelectScript(
                        Objects.requireNonNull(beforePrefixReply, "beforePrefixReply")
                )
        );
    }

    static ScriptedRespServer rejectingAuth(String reply) throws IOException {
        byte[] response = Objects.requireNonNull(reply, "reply")
                .getBytes(StandardCharsets.US_ASCII);
        return new ScriptedRespServer(1, (connection, command, connectionCount, totalCount) -> {
            if (command.name().equals("AUTH")) {
                connection.sendReplies(response, 1);
            }
        });
    }

    static ScriptedRespServer neverResponding() throws IOException {
        return new ScriptedRespServer(1, (connection, command, connectionCount, totalCount) -> {
        });
    }

    static ScriptedRespServer batched(
            int expectedClients,
            byte[] reply,
            int batchSize,
            Runnable immediatelyBeforeResponse
    ) throws IOException {
        byte[] response = Objects.requireNonNull(reply, "reply").clone();
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
        Objects.requireNonNull(immediatelyBeforeResponse, "immediatelyBeforeResponse");
        return new ScriptedRespServer(
                expectedClients,
                (connection, command, connectionCount, totalCount) -> {
                    if (connectionCount % batchSize == 0) {
                        immediatelyBeforeResponse.run();
                        connection.sendReplies(response, batchSize);
                    }
                }
        );
    }

    static ScriptedRespServer fragmentingEveryByte(String reply) throws IOException {
        byte[] response = Objects.requireNonNull(reply, "reply")
                .getBytes(StandardCharsets.US_ASCII);
        return new ScriptedRespServer(
                1,
                (connection, command, connectionCount, totalCount) ->
                        connection.sendEveryByte(response)
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
        if (connectionCommandNumber <= 0) {
            throw new IllegalArgumentException("connectionCommandNumber must be > 0");
        }
        return new ScriptedRespServer(
                expectedClients,
                (connection, command, connectionCount, totalCount) -> {
                    if (connectionCount == connectionCommandNumber) {
                        connection.close();
                    }
                }
        );
    }

    static ScriptedRespServer closingAfterCommands(int connectionCommandNumber)
            throws IOException {
        return closeAfterCommand(1, connectionCommandNumber);
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

    int measuredCommandReplies() {
        return measuredCommandReplies.get();
    }

    boolean stallWasReleased() {
        return responseScript.stallWasReleased();
    }

    LongSupplier coordinatedClock(LongSupplier delegate) {
        Objects.requireNonNull(delegate, "delegate");
        return () -> {
            long now = delegate.getAsLong();
            clockCalls.incrementAndGet();
            signalStateChanged();
            return now;
        };
    }

    int awaitAcceptedConnections(int expected, Duration timeout) throws InterruptedException {
        awaitState(
                () -> acceptedConnections.get() >= expected,
                timeout,
                "accepted connections"
        );
        return acceptedConnections.get();
    }

    List<String> firstConnectionCommandPrefix(int length) {
        return connection(0).commandNamesPrefix(length);
    }

    List<String> firstConnectionCommandArguments(int commandIndex) {
        return connection(0).commandArguments(commandIndex);
    }

    boolean everyConnectionStartsWith(List<String> expectedPrefix) {
        Objects.requireNonNull(expectedPrefix, "expectedPrefix");
        for (Connection connection : connectionSnapshot()) {
            if (connection.commandCount() == 0) {
                continue;
            }
            if (!connection.commandNamesPrefix(expectedPrefix.size()).equals(expectedPrefix)) {
                return false;
            }
        }
        return true;
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
        synchronized (stateChanged) {
            for (int index = 0; index < expectedConnectionCount; index++) {
                counts.add(connections.get(index).commandCount());
            }
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
        for (Connection connection : connectionSnapshot()) {
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
            while (!closed) {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                Connection connection = new Connection(socket);
                synchronized (stateChanged) {
                    connections.add(connection);
                    acceptedConnections.incrementAndGet();
                    stateChanged.notifyAll();
                }
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
        boolean commandFrameStarted = false;
        try {
            InputStream input = connection.input();
            while (!closed) {
                int first = input.read();
                if (first < 0) {
                    return;
                }
                commandFrameStarted = true;
                Command command;
                if (first == '*') {
                    command = readRespArray(input);
                } else {
                    command = readInline(input, first);
                }
                commandFrameStarted = false;
                connection.recordCommand(command);
                int connectionCommands = connection.incrementCommandCount();
                int totalCommands = commands.incrementAndGet();
                signalStateChanged();
                responseScript.onCommand(
                        connection,
                        command,
                        connectionCommands,
                        totalCommands
                );
            }
        } catch (SocketException e) {
            // 命令边界或回写阶段的 reset 可能来自 runner 达到回复阈值后的清理；帧内 reset 则表示命令被截断。
            if (!closed && commandFrameStarted) {
                recordFailure(e);
            }
        } catch (Throwable e) {
            if (!closed) {
                recordFailure(e);
            }
        }
    }

    private Command readRespArray(InputStream input) throws IOException {
        long argumentCount = readLongLine(input, "array length");
        if (argumentCount <= 0 || argumentCount > RespProtocolLimits.DEFAULT_MAX_ARGS) {
            throw new IOException("invalid RESP command argument count: " + argumentCount);
        }
        List<String> arguments = new ArrayList<>((int) argumentCount);
        for (long argument = 0; argument < argumentCount; argument++) {
            if (input.read() != '$') {
                throw new IOException("RESP command argument must be a bulk string");
            }
            long length = readLongLine(input, "bulk length");
            if (length < 0 || length > RespProtocolLimits.DEFAULT_MAX_BULK_BYTES) {
                throw new IOException("invalid RESP command bulk length: " + length);
            }
            if (length <= MAX_CAPTURED_ARGUMENT_BYTES) {
                byte[] bytes = input.readNBytes((int) length);
                if (bytes.length != length) {
                    throw new EOFException("unexpected EOF in RESP command bulk string");
                }
                arguments.add(new String(bytes, StandardCharsets.UTF_8));
            } else {
                input.skipNBytes(length);
                arguments.add("");
            }
            requireCrlf(input);
        }
        return new Command(arguments);
    }

    private Command readInline(InputStream input, int first) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        line.write(first);
        while (true) {
            int current = input.read();
            if (current < 0) {
                throw new EOFException("unexpected EOF in inline command");
            }
            if (current == '\r') {
                if (input.read() != '\n') {
                    throw new IOException("inline CR is not followed by LF");
                }
                break;
            }
            if (current == '\n') {
                throw new IOException("inline LF is not preceded by CR");
            }
            line.write(current);
            if (line.size() > RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES) {
                throw new IOException("inline command exceeds protocol limit");
            }
        }
        String text = line.toString(StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) {
            throw new IOException("inline command must not be empty");
        }
        return new Command(List.of(text.split("\\s+")));
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
        synchronized (stateChanged) {
            if (id < 0 || id >= connections.size()) {
                throw new IllegalStateException("connection " + id + " has not been accepted");
            }
            return connections.get(id);
        }
    }

    private List<Connection> connectionSnapshot() {
        synchronized (stateChanged) {
            return List.copyOf(connections);
        }
    }

    private boolean everyConnectionHasAtLeast(int expectedCommands) {
        if (acceptedConnections.get() < expectedClients) {
            return false;
        }
        for (int index = 0; index < expectedClients; index++) {
            if (connection(index).commandCount() < expectedCommands) {
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

    private record Command(List<String> arguments) {
        private Command {
            arguments = List.copyOf(arguments);
            if (arguments.isEmpty() || arguments.getFirst().isEmpty()) {
                throw new IllegalArgumentException("command name must not be empty");
            }
        }

        String name() {
            return arguments.getFirst().toUpperCase(Locale.ROOT);
        }
    }

    private interface ResponseScript {
        default void attach(ScriptedRespServer server) {
        }

        void onCommand(
                Connection connection,
                Command command,
                int connectionCommandCount,
                int totalCommandCount
        ) throws Exception;

        default boolean stallWasReleased() {
            return false;
        }

        default void releaseStall() throws IOException {
            throw new UnsupportedOperationException("this response script has no stall");
        }
    }

    private final class Connection implements AutoCloseable {
        private final Socket socket;
        private final InputStream input;
        private final OutputStream output;
        private final Object writeLock = new Object();
        private final AtomicInteger commandCount = new AtomicInteger();
        private final List<Command> receivedCommands = new ArrayList<>();

        private Connection(Socket socket) throws IOException {
            this.socket = socket;
            this.input = socket.getInputStream();
            this.output = socket.getOutputStream();
        }

        InputStream input() {
            return input;
        }

        void recordCommand(Command command) {
            synchronized (stateChanged) {
                receivedCommands.add(command);
                stateChanged.notifyAll();
            }
        }

        int incrementCommandCount() {
            return commandCount.incrementAndGet();
        }

        int commandCount() {
            return commandCount.get();
        }

        List<String> commandNamesPrefix(int length) {
            synchronized (stateChanged) {
                if (length < 0 || receivedCommands.size() < length) {
                    throw new IllegalArgumentException("command prefix is not available");
                }
                return receivedCommands.subList(0, length).stream()
                        .map(Command::name)
                        .toList();
            }
        }

        List<String> commandArguments(int index) {
            synchronized (stateChanged) {
                return receivedCommands.get(index).arguments();
            }
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

        void sendEveryByte(byte[] reply) throws IOException {
            synchronized (writeLock) {
                for (byte value : reply) {
                    output.write(value);
                    output.flush();
                }
            }
            sentReplies.incrementAndGet();
            signalStateChanged();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private static final class AuthAndSelectScript implements ResponseScript {
        private static final Duration COORDINATION_TIMEOUT = Duration.ofSeconds(5);

        private final Runnable beforePrefixReply;
        private ScriptedRespServer server;

        private AuthAndSelectScript(Runnable beforePrefixReply) {
            this.beforePrefixReply = beforePrefixReply;
        }

        @Override
        public void attach(ScriptedRespServer server) {
            this.server = server;
        }

        @Override
        public void onCommand(
                Connection connection,
                Command command,
                int connectionCommandCount,
                int totalCommandCount
        ) throws IOException, InterruptedException {
            switch (command.name()) {
                case "AUTH" -> sendPrefixReply(connection);
                case "SELECT" -> connection.sendReplies(OK, 1);
                case "PING" -> {
                    connection.sendReplies(PONG, 1);
                    server.measuredCommandReplies.incrementAndGet();
                }
                default -> throw new IOException("unexpected command: " + command.name());
            }
        }

        private void sendPrefixReply(Connection connection)
                throws IOException, InterruptedException {
            if (beforePrefixReply == null) {
                connection.sendReplies(OK, 1);
                return;
            }
            int observedClockCalls = server.clockCalls.get();
            beforePrefixReply.run();
            connection.sendReplies(OK, 1);
            server.awaitClockCallAfter(observedClockCalls, COORDINATION_TIMEOUT);
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
        public void onCommand(
                Connection connection,
                Command command,
                int connectionCommandCount,
                int totalCommandCount
        ) {
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
