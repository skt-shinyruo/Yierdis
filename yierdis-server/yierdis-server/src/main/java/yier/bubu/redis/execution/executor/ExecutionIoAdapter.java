package yier.bubu.redis.execution.executor;

public interface ExecutionIoAdapter {
    boolean isActive(ExecutionConnection connection);

    boolean isWritable(ExecutionConnection connection);

    void disableInput(ExecutionConnection connection);

    void enableInput(ExecutionConnection connection);

    void onClose(ExecutionConnection connection, Runnable callback);

    default void closeConnection(ExecutionConnection connection) {
    }
}
