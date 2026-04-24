package yier.bubu.redis.executor;

import yier.bubu.redis.bytes.BytesSink;

public interface ExecutionIoAdapter<C extends ExecutionConnection> {
    boolean isActive(C connection);

    boolean isWritable(C connection);

    void disableInput(C connection);

    void enableInput(C connection);

    void onClose(C connection, Runnable callback);

    BytesSink newReplySink(C connection);

    void writeBufferedReply(C connection, boolean closeAfterReply);

    void flushPending(Iterable<C> touchedConnections);
}
