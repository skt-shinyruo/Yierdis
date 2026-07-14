package yier.bubu.redis.execution.executor;

import yier.bubu.redis.bytes.BytesSink;

public interface ExecutionIoAdapter<C extends ExecutionConnection> {
    boolean isActive(C connection);

    boolean isWritable(C connection);

    void disableInput(C connection);

    void enableInput(C connection);

    void onClose(C connection, Runnable callback);

    default void closeConnection(C connection) {
    }

    BytesSink newReplySink(C connection);

    void writeBufferedReply(C connection, boolean closeAfterReply);

    void flushPending(Iterable<C> touchedConnections);
}
