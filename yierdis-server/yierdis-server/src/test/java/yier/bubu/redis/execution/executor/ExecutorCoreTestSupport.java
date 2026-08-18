package yier.bubu.redis.execution.executor;

import java.util.function.BiFunction;

import org.junit.Assert;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ReplyReservationResult;
import yier.bubu.redis.execution.api.ReplyReservationSink;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ValidationResult;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

final class ExecutorCoreTestSupport {
    private ExecutorCoreTestSupport() {
    }

    static TestConnection newConnection(String connectionId) {
        return new TestConnection(connectionId);
    }

    static ManualOwnerExecutor manualOwnerExecutor() {
        return new ManualOwnerExecutor();
    }

    static void startExecutor(CommandExecutor<?> executor, ManualOwnerExecutor ownerExecutor) {
        Thread startThread = new Thread(executor::start);
        startThread.start();
        long deadlineNanos = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(1);
        while (ownerExecutor.pendingTasks() == 0 && startThread.isAlive() && System.nanoTime() < deadlineNanos) {
            Thread.onSpinWait();
        }
        Assert.assertTrue("owner executor should receive bind task during start", ownerExecutor.pendingTasks() > 0);
        ownerExecutor.runAll();
        try {
            startThread.join(1000);
        } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for executor start", e);
        }
        Assert.assertFalse("executor.start should complete after owner tasks run", startThread.isAlive());
    }

    static BiFunction<CommandSession, BytesSink, RedisReplyWriter> simpleReplyWriterFactory() {
        return (session, out) -> new SimpleReplyWriter(out);
    }

    static BiFunction<CommandSession, ReplyShape, ReplyPlan> simpleReplySizer() {
        return (session, shape) -> ReplyPlan.exact(64L, shape.retainedSourceBytes());
    }

    static BiFunction<CommandSession, ExecutionRequest, PreparedCommand> simpleCommandEngine() {
        return (session, request) -> {
            if (asciiEqualsIgnoreCase(request, 0, "PING")) {
                return fixed(
                        ReplyShapes.simpleString("PONG"),
                        context -> CommandResult.reply(RedisReplies.simpleString("PONG")));
            }
            if (asciiEqualsIgnoreCase(request, 0, "QUIT")) {
                return fixed(
                        ReplyShapes.simpleString("OK"),
                        context -> CommandResult.closeAfterReply(RedisReplies.simpleString("OK")));
            }
            return fixed(
                    ReplyShapes.error("ERR unsupported test command"),
                    context -> CommandResult.error("ERR unsupported test command"));
        };
    }

    static PreparedCommand fixed(
            ReplyShape shape,
            Function<CommandSession, CommandResult> execution
    ) {
        return fixed(shape, execution, () -> { });
    }

    static PreparedCommand fixed(
            ReplyShape shape,
            Function<CommandSession, CommandResult> execution,
            Runnable closeAction
    ) {
        Objects.requireNonNull(shape, "shape");
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(closeAction, "closeAction");
        return new PreparedCommand() {
            private boolean closed;

            @Override
            public ReplyShape reservationShape() {
                return shape;
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return ValidationResult.VALID;
            }

            @Override
            public CommandResult execute(CommandSession context) {
                return execution.apply(context);
            }

            @Override
            public void close() {
                if (closed) {
                    return;
                }
                closed = true;
                closeAction.run();
            }
        };
    }

    static <C extends ExecutionConnection> void publish(
            CommandExecutor<C> executor,
            C connection,
            ExecutionRequest request,
            ExecutionReply reply
    ) {
        ExecutorAdmissionAttempt<C> attempt = executor.tryAcquire(connection, request.retainedBytes());
        Assert.assertTrue(attempt instanceof ExecutorAdmissionAttempt.Acquired<C>);
        ((ExecutorAdmissionAttempt.Acquired<C>) attempt).admission().publish(request, reply);
    }

    static ExecutionReply ioReply(RecordingIoAdapter io, TestConnection connection) {
        return new IoExecutionReply(io, connection);
    }

    static SerialOwnerExecutor serialOwner(Executor delegate) {
        return new DelegatingSerialOwnerExecutor(delegate);
    }

    private static boolean asciiEqualsIgnoreCase(ExecutionRequest request, int index, String expectedUpperAscii) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(expectedUpperAscii, "expectedUpperAscii");
        if (request.isNull(index) || request.len(index) != expectedUpperAscii.length()) {
            return false;
        }
        for (int i = 0; i < expectedUpperAscii.length(); i++) {
            int actual = request.byteAt(index, i) & 0xFF;
            if (actual >= 'a' && actual <= 'z') {
                actual -= 32;
            }
            if (actual != expectedUpperAscii.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}

final class TestConnection implements ExecutionConnection {
    private final String connectionId;
    private final CommandSession session = new TestSession();
    private final ExecutionConnectionContext context;

    TestConnection(String connectionId) {
        this(connectionId, new ExecutionConnectionContext());
    }

    TestConnection(String connectionId, ExecutionConnectionContext context) {
        this.connectionId = connectionId;
        this.context = context;
    }

    String connectionId() {
        return connectionId;
    }

    @Override
    public CommandSession session() {
        return session;
    }

    @Override
    public ExecutionConnectionContext context() {
        return context;
    }

    @Override
    public boolean markClosing() {
        return context.markClosing();
    }

    private static final class TestSession implements CommandSession {
        private final TransactionState transaction = new TestTransactionState();

        @Override
        public int dbIndex() {
            return 0;
        }

        @Override
        public void setDbIndex(int dbIndex) {
        }

        @Override
        public String clientName() {
            return null;
        }

        @Override
        public void setClientName(String clientName) {
        }

        @Override
        public TransactionState transaction() {
            return transaction;
        }

        @Override
        public ConnectionStatsView connectionStats() {
            return null;
        }

        @Override
        public int respVersion() {
            return 2;
        }

        @Override
        public void setRespVersion(int respVersion) {
        }
    }

    private static final class TestTransactionState implements TransactionState {
        @Override
        public boolean active() {
            return false;
        }

        @Override
        public boolean aborted() {
            return false;
        }

        @Override
        public void begin() {
        }

        @Override
        public void markAborted() {
        }

        @Override
        public String tryEnqueue(ExecutionRequest request) {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void forEachQueued(Consumer<? super ExecutionRequest> visitor) {
            Objects.requireNonNull(visitor, "visitor");
        }

        @Override
        public List<ExecutionRequest> drain() {
            return List.of();
        }

        @Override
        public void discard() {
        }

    }
}

final class DelegatingSerialOwnerExecutor implements SerialOwnerExecutor {
    private final Executor delegate;
    private final AtomicInteger runningActions = new AtomicInteger();
    private volatile Thread ownerThread;

    DelegatingSerialOwnerExecutor(Executor delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command, "command");
        delegate.execute(() -> {
            Thread current = Thread.currentThread();
            Thread established = ownerThread;
            if (established == null) {
                ownerThread = current;
            } else if (established != current) {
                throw new IllegalStateException("serial owner changed physical thread");
            }
            if (!runningActions.compareAndSet(0, 1)) {
                throw new IllegalStateException("serial owner actions overlapped");
            }
            try {
                command.run();
            } finally {
                runningActions.set(0);
            }
        });
    }

    @Override
    public boolean inOwnerThread() {
        return runningActions.get() == 1 && Thread.currentThread() == ownerThread;
    }
}

final class IoExecutionReply implements ExecutionReply {
    private final RecordingIoAdapter io;
    private final TestConnection connection;
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private boolean closeAfterReply;

    IoExecutionReply(RecordingIoAdapter io, TestConnection connection) {
        this.io = Objects.requireNonNull(io, "io");
        this.connection = Objects.requireNonNull(connection, "connection");
    }

    @Override
    public ReplyReservationResult tryReserve(ReplyPlan plan) {
        return ReplyReservationResult.RESERVED;
    }

    @Override
    public Runnable onCapacityAvailable(Runnable wakeup) {
        return null;
    }

    @Override
    public BytesSink sink() {
        return bytes::write;
    }

    @Override
    public void markReady(boolean closeAfterReply) {
        this.closeAfterReply = closeAfterReply;
        io.recordReplyReady(connection, this);
    }

    @Override
    public void cancel() {
    }

    @Override
    public boolean hasWrittenBytes() {
        return bytes.size() > 0;
    }

    @Override
    public void markResultUnknown() {
    }

    String bytes() {
        return bytes.toString(StandardCharsets.UTF_8);
    }

    boolean closeAfterReply() {
        return closeAfterReply;
    }
}

final class ManualOwnerExecutor implements SerialOwnerExecutor {
    private final List<Runnable> tasks = new ArrayList<>();
    private Thread ownerThread;
    private boolean runningAction;

    @Override
    public void execute(Runnable command) {
        tasks.add(Objects.requireNonNull(command, "command"));
    }

    @Override
    public boolean inOwnerThread() {
        return runningAction && Thread.currentThread() == ownerThread;
    }

    int pendingTasks() {
        return tasks.size();
    }

    void runAll() {
        Thread caller = Thread.currentThread();
        if (ownerThread == null) {
            ownerThread = caller;
        } else if (ownerThread != caller) {
            throw new IllegalStateException("manual owner changed physical thread");
        }
        while (!tasks.isEmpty()) {
            List<Runnable> pending = new ArrayList<>(tasks);
            tasks.clear();
            for (Runnable task : pending) {
                if (runningAction) {
                    throw new IllegalStateException("serial owner actions overlapped");
                }
                runningAction = true;
                try {
                    task.run();
                } finally {
                    runningAction = false;
                }
            }
        }
    }
}

final class TrackingExecutionRequest implements ExecutionRequest {
    private final byte[][] argv;
    private final int retainedBytes;
    private final boolean failOnCommandRead;
    private final Runnable closeAction;
    private final AtomicInteger closeCalls = new AtomicInteger();

    private TrackingExecutionRequest(
            byte[][] argv,
            int retainedBytes,
            boolean failOnCommandRead,
            Runnable closeAction
    ) {
        this.argv = argv;
        this.retainedBytes = retainedBytes;
        this.failOnCommandRead = failOnCommandRead;
        this.closeAction = Objects.requireNonNull(closeAction, "closeAction");
    }

    static TrackingExecutionRequest ofUtf8(String cmd, String... args) {
        return fromUtf8(false, () -> { }, cmd, args);
    }

    static TrackingExecutionRequest ofUtf8(
            String cmd,
            Runnable closeAction,
            String... args
    ) {
        return fromUtf8(false, closeAction, cmd, args);
    }

    static TrackingExecutionRequest failingOnCommandRead(String cmd, String... args) {
        return fromUtf8(true, () -> { }, cmd, args);
    }

    private static TrackingExecutionRequest fromUtf8(
            boolean failOnCommandRead,
            Runnable closeAction,
            String cmd,
            String... args
    ) {
        byte[][] argv = new byte[args.length + 1][];
        int retainedBytes = 0;

        argv[0] = utf8(cmd);
        retainedBytes += argv[0].length;
        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                continue;
            }
            argv[i + 1] = utf8(args[i]);
            retainedBytes += argv[i + 1].length;
        }
        return new TrackingExecutionRequest(argv, retainedBytes, failOnCommandRead, closeAction);
    }

    int closeCalls() {
        return closeCalls.get();
    }

    @Override
    public int argc() {
        return argv.length;
    }

    @Override
    public boolean isNull(int index) {
        return argv[index] == null;
    }

    @Override
    public int len(int index) {
        byte[] arg = argv[index];
        return arg == null ? -1 : arg.length;
    }

    @Override
    public byte byteAt(int index, int offset) {
        if (failOnCommandRead && index == 0) {
            throw new RuntimeException("boom");
        }
        return argv[index][offset];
    }

    @Override
    public void copyToByteArray(int index, byte[] dst, int dstOff) {
        byte[] arg = argv[index];
        System.arraycopy(arg, 0, dst, dstOff, arg.length);
    }

    @Override
    public byte[] toByteArray(int index) {
        byte[] arg = argv[index];
        return arg == null ? null : arg.clone();
    }

    @Override
    public int retainedBytes() {
        return retainedBytes;
    }

    @Override
    public void close() {
        closeCalls.incrementAndGet();
        closeAction.run();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

final class SimpleReplyWriter implements RedisReplyWriter {
    private final BytesSink out;

    SimpleReplyWriter(BytesSink out) {
        this.out = Objects.requireNonNull(out, "out");
    }

    @Override
    public void simpleString(String value) {
        write(value == null ? "(null)" : value);
    }

    @Override
    public void error(String message) {
        write("ERR " + message);
    }

    @Override
    public void controlError(String message) {
        if (out instanceof ReplyReservationSink reservationSink) {
            reservationSink.useControlReservation();
        }
        write(message == null ? "ERR internal error" : message);
    }

    @Override
    public void integer(long value) {
        write(Long.toString(value));
    }

    @Override
    public void booleanValue(boolean value) {
        write(Boolean.toString(value));
    }

    @Override
    public void doubleValue(double value) {
        write(Double.toString(value));
    }

    @Override
    public void bigNumberAscii(String value) {
        write(value);
    }

    @Override
    public void verbatimString(String format, byte[] data) {
        write(new String(data == null ? new byte[0] : data, StandardCharsets.UTF_8));
    }

    @Override
    public void blobError(String message) {
        write("ERR " + message);
    }

    @Override
    public void bulkString(byte[] data) {
        write(data == null ? "(null)" : new String(data, StandardCharsets.UTF_8));
    }

    @Override
    public void bulkString(byte[] data, int off, int len) {
        write(data == null ? "(null)" : new String(data, off, len, StandardCharsets.UTF_8));
    }

    @Override
    public void bulkString(yier.bubu.redis.bytes.BytesSlice slice) {
        if (slice == null) {
            write("(null)");
            return;
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        slice.writeTo(bytes::write);
        write(bytes.toString(StandardCharsets.UTF_8));
    }

    @Override
    public void bulkStringLongAscii(long value) {
        write(Long.toString(value));
    }

    @Override
    public void nullValue() {
        write("(null)");
    }

    @Override
    public void nullArray() {
        write("(null)");
    }

    @Override
    public void arrayHeader(int count) {
        throw new UnsupportedOperationException("arrayHeader");
    }

    @Override
    public void mapHeader(int pairs) {
        throw new UnsupportedOperationException("mapHeader");
    }

    @Override
    public void setHeader(int count) {
        throw new UnsupportedOperationException("setHeader");
    }

    @Override
    public void pushHeader(int count) {
        throw new UnsupportedOperationException("pushHeader");
    }

    @Override
    public void attributeHeader(int pairs) {
        throw new UnsupportedOperationException("attributeHeader");
    }

    private void write(String value) {
        byte[] bytes = (value + "\n").getBytes(StandardCharsets.UTF_8);
        out.writeBytes(bytes, 0, bytes.length);
    }
}

final class RecordingIoAdapter implements ExecutionIoAdapter<TestConnection> {
    private final Map<TestConnection, ConnectionState> states = new IdentityHashMap<>();
    private final List<String> executionOrder = new ArrayList<>();
    private RuntimeException closeFailure;

    @Override
    public boolean isActive(TestConnection connection) {
        return true;
    }

    @Override
    public boolean isWritable(TestConnection connection) {
        return state(connection).writable;
    }

    @Override
    public void disableInput(TestConnection connection) {
        state(connection).inputDisabled = true;
    }

    @Override
    public void enableInput(TestConnection connection) {
        state(connection).inputEnabledAgain = true;
    }

    @Override
    public void onClose(TestConnection connection, Runnable callback) {
        state(connection).closeCallback = callback;
    }

    @Override
    public void closeConnection(TestConnection connection) {
        state(connection).closeCalls++;
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    void recordReplyReady(TestConnection connection, IoExecutionReply reply) {
        state(connection).readyReply = Objects.requireNonNull(reply, "reply");
        executionOrder.add(connection.connectionId());
    }

    String replyBytes(TestConnection connection) {
        IoExecutionReply reply = state(connection).readyReply;
        return reply == null ? "" : reply.bytes();
    }

    boolean replyCloseAfterReply(TestConnection connection) {
        IoExecutionReply reply = state(connection).readyReply;
        return reply != null && reply.closeAfterReply();
    }

    boolean inputDisabled(TestConnection connection) {
        return state(connection).inputDisabled;
    }

    boolean inputEnabledAgain(TestConnection connection) {
        return state(connection).inputEnabledAgain;
    }

    List<String> executionOrder() {
        return List.copyOf(executionOrder);
    }

    int closeCalls(TestConnection connection) {
        return state(connection).closeCalls;
    }

    void failCloseConnectionWith(RuntimeException failure) {
        closeFailure = Objects.requireNonNull(failure, "failure");
    }

    void setWritable(TestConnection connection, boolean writable) {
        state(connection).writable = writable;
    }

    void fireClosed(TestConnection connection) {
        state(connection).closeCallback.run();
    }

    private ConnectionState state(TestConnection connection) {
        return states.computeIfAbsent(connection, ignored -> new ConnectionState());
    }

    private static final class ConnectionState {
        private Runnable closeCallback = () -> {};
        private IoExecutionReply readyReply;
        private boolean writable = true;
        private boolean inputDisabled;
        private boolean inputEnabledAgain;
        private int closeCalls;
    }
}
