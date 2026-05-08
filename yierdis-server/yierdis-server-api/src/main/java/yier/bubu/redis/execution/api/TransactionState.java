package yier.bubu.redis.execution.api;

import java.util.List;

/**
 * Connection-scoped transaction state (MULTI/EXEC/DISCARD) abstraction.
 * <p>
 * This lives in the contract module so that the command layer can access connection state without depending on
 * transport/server implementations.
 */
public interface TransactionState {
    boolean active();

    void begin();

    void discard();

    void enqueue(ExecutionRequest request);

    default boolean aborted() {
        return false;
    }

    default void markAborted() {
        // no-op
    }

    default String tryEnqueue(ExecutionRequest request) {
        if (request == null) {
            return null;
        }
        enqueue(request);
        return null;
    }

    int size();

    List<ExecutionRequest> drain();
}
