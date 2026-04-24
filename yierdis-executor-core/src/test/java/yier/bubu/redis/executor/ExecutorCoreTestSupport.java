package yier.bubu.redis.executor;

import yier.bubu.redis.bytes.BytesSink;

import java.io.ByteArrayOutputStream;

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
    private Runnable closeCallback = () -> {};
    private boolean closeAfterReply;

    @Override
    public boolean isActive(TestConnection connection) {
        return true;
    }

    @Override
    public boolean isWritable(TestConnection connection) {
        return true;
    }

    @Override
    public void disableInput(TestConnection connection) {
    }

    @Override
    public void enableInput(TestConnection connection) {
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
    }

    String bufferedReply() {
        return bytes.toString();
    }

    boolean closeAfterReply() {
        return closeAfterReply;
    }

    void fireClosed() {
        closeCallback.run();
    }
}
