package yier.bubu.redis.executor;

import yier.bubu.redis.bytes.BytesSink;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class ExecutorCoreTestSupport {
    private ExecutorCoreTestSupport() {
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

final class RecordingIoAdapter implements ExecutionIoAdapter<TestConnection> {
    private final Map<String, ConnectionState> states = new HashMap<>();
    private final List<String> lastFlushedConnectionIds = new ArrayList<>();
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
        return state(connection).bytes::write;
    }

    @Override
    public void writeBufferedReply(TestConnection connection, boolean closeAfterReply) {
        state(connection).closeAfterReply = closeAfterReply;
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
        return states.computeIfAbsent(connection.connectionId(), ignored -> new ConnectionState());
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
