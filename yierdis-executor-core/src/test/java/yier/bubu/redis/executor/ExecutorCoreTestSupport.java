package yier.bubu.redis.executor;

import yier.bubu.redis.bytes.BytesSink;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

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
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final List<String> flushedConnectionIds = new ArrayList<>();
    private Runnable closeCallback = () -> {};
    private boolean active = true;
    private boolean writable = true;
    private boolean closeAfterReply;
    private boolean inputDisabled;
    private boolean inputEnabledAgain;

    @Override
    public boolean isActive(TestConnection connection) {
        return active;
    }

    @Override
    public boolean isWritable(TestConnection connection) {
        return writable;
    }

    @Override
    public void disableInput(TestConnection connection) {
        inputDisabled = true;
    }

    @Override
    public void enableInput(TestConnection connection) {
        inputEnabledAgain = true;
    }

    @Override
    public void onClose(TestConnection connection, Runnable callback) {
        this.closeCallback = callback;
    }

    @Override
    public BytesSink newReplySink(TestConnection connection) {
        return bytes::write;
    }

    @Override
    public void writeBufferedReply(TestConnection connection, boolean closeAfterReply) {
        this.closeAfterReply = closeAfterReply;
    }

    @Override
    public void flushPending(Iterable<TestConnection> touchedConnections) {
        for (TestConnection connection : touchedConnections) {
            flushedConnectionIds.add(connection.connectionId());
        }
    }

    String bufferedReply() {
        return bytes.toString();
    }

    boolean closeAfterReply() {
        return closeAfterReply;
    }

    boolean inputDisabled() {
        return inputDisabled;
    }

    boolean inputEnabledAgain() {
        return inputEnabledAgain;
    }

    int flushCalls() {
        return flushedConnectionIds.size();
    }

    String lastFlushedConnectionId() {
        return flushedConnectionIds.isEmpty() ? null : flushedConnectionIds.get(flushedConnectionIds.size() - 1);
    }

    void setActive(boolean active) {
        this.active = active;
    }

    void setWritable(boolean writable) {
        this.writable = writable;
    }

    void fireClosed() {
        closeCallback.run();
    }
}
