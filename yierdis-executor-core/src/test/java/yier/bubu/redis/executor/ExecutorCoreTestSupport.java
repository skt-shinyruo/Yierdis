package yier.bubu.redis.executor;

import org.junit.Assert;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ReplyWriterFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

final class ExecutorCoreTestSupport {
    private ExecutorCoreTestSupport() {
    }

    static TestConnection newConnection(String connectionId) {
        return new TestConnection(connectionId, new ExecutionConnectionContext(new DefaultExecutionSession(4, 128)));
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

    static ReplyWriterFactory simpleReplyWriterFactory() {
        return SimpleReplyWriter::new;
    }

    static CommandExecutionEngine simpleCommandEngine() {
        return (request, ctx) -> {
            if (asciiEqualsIgnoreCase(request, 0, "PING")) {
                ctx.out().simpleString("PONG");
                return;
            }
            if (asciiEqualsIgnoreCase(request, 0, "QUIT")) {
                ctx.out().simpleString("OK");
                ctx.out().requestCloseAfterReply();
                return;
            }
            ctx.out().error("ERR unsupported test command");
        };
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
    private final ExecutionConnectionContext context;

    TestConnection(String connectionId, ExecutionConnectionContext context) {
        this.connectionId = connectionId;
        this.context = context;
    }

    @Override
    public String connectionId() {
        return connectionId;
    }

    @Override
    public ExecutionConnectionContext context() {
        return context;
    }
}

final class ManualOwnerExecutor implements Executor {
    private final List<Runnable> tasks = new ArrayList<>();

    @Override
    public void execute(Runnable command) {
        tasks.add(Objects.requireNonNull(command, "command"));
    }

    int pendingTasks() {
        return tasks.size();
    }

    void runAll() {
        while (!tasks.isEmpty()) {
            List<Runnable> pending = new ArrayList<>(tasks);
            tasks.clear();
            for (Runnable task : pending) {
                task.run();
            }
        }
    }
}

final class TrackingExecutionRequest implements ExecutionRequest {
    private final byte[][] argv;
    private final int retainedBytes;
    private final boolean failOnCommandRead;
    private final AtomicInteger closeCalls = new AtomicInteger();

    private TrackingExecutionRequest(byte[][] argv, int retainedBytes, boolean failOnCommandRead) {
        this.argv = argv;
        this.retainedBytes = retainedBytes;
        this.failOnCommandRead = failOnCommandRead;
    }

    static TrackingExecutionRequest ofUtf8(String cmd, String... args) {
        return fromUtf8(false, cmd, args);
    }

    static TrackingExecutionRequest failingOnCommandRead(String cmd, String... args) {
        return fromUtf8(true, cmd, args);
    }

    private static TrackingExecutionRequest fromUtf8(boolean failOnCommandRead, String cmd, String... args) {
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
        return new TrackingExecutionRequest(argv, retainedBytes, failOnCommandRead);
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
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

final class SimpleReplyWriter implements ReplyWriter {
    private final BytesSink out;
    private boolean closeAfterReplyRequested;

    SimpleReplyWriter(BytesSink out) {
        this.out = Objects.requireNonNull(out, "out");
    }

    @Override
    public void requestCloseAfterReply() {
        closeAfterReplyRequested = true;
    }

    @Override
    public boolean closeAfterReplyRequested() {
        return closeAfterReplyRequested;
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
    public void protocolError(String message) {
        write(message == null ? "Protocol error" : message);
    }

    @Override
    public void internalError(String message) {
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
    public void bulkStringArray(List<byte[]> values) {
        throw new UnsupportedOperationException("bulkStringArray");
    }

    @Override
    public void emptyArray() {
        write("[]");
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
        out.writeBytes((value + "\n").getBytes(StandardCharsets.UTF_8));
    }
}

final class RecordingIoAdapter implements ExecutionIoAdapter<TestConnection> {
    private final Map<TestConnection, ConnectionState> states = new IdentityHashMap<>();
    private final List<String> lastFlushedConnectionIds = new ArrayList<>();
    private final List<String> executionOrder = new ArrayList<>();
    private int flushCalls;

    @Override
    public boolean isActive(TestConnection connection) {
        return state(connection).active;
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
    public BytesSink newReplySink(TestConnection connection) {
        ConnectionState state = state(connection);
        state.bytes.reset();
        state.closeAfterReply = false;
        return state.bytes::write;
    }

    @Override
    public void writeBufferedReply(TestConnection connection, boolean closeAfterReply) {
        state(connection).closeAfterReply = closeAfterReply;
        executionOrder.add(connection.connectionId());
    }

    @Override
    public void flushPending(Iterable<TestConnection> touchedConnections) {
        flushCalls++;
        lastFlushedConnectionIds.clear();
        for (TestConnection connection : touchedConnections) {
            state(connection).flushCount++;
            lastFlushedConnectionIds.add(connection.connectionId());
        }
    }

    String bufferedReply(TestConnection connection) {
        return state(connection).bytes.toString();
    }

    boolean closeAfterReply(TestConnection connection) {
        return state(connection).closeAfterReply;
    }

    boolean inputDisabled(TestConnection connection) {
        return state(connection).inputDisabled;
    }

    boolean inputEnabledAgain(TestConnection connection) {
        return state(connection).inputEnabledAgain;
    }

    int flushCalls() {
        return flushCalls;
    }

    String lastFlushedConnectionId() {
        return lastFlushedConnectionIds.isEmpty() ? null : lastFlushedConnectionIds.get(lastFlushedConnectionIds.size() - 1);
    }

    List<String> lastFlushedConnectionIds() {
        return List.copyOf(lastFlushedConnectionIds);
    }

    List<String> executionOrder() {
        return List.copyOf(executionOrder);
    }

    int flushCount(TestConnection connection) {
        return state(connection).flushCount;
    }

    void setActive(TestConnection connection, boolean active) {
        state(connection).active = active;
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
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private Runnable closeCallback = () -> {};
        private boolean active = true;
        private boolean writable = true;
        private boolean closeAfterReply;
        private boolean inputDisabled;
        private boolean inputEnabledAgain;
        private int flushCount;
    }
}
