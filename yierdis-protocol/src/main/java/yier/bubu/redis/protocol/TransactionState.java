package yier.bubu.redis.protocol;

import java.util.List;

/**
 * Connection-scoped transaction state (MULTI/EXEC/DISCARD) abstraction.
 * <p>
 * This lives in the protocol module so that the command layer can access connection state without depending on
 * transport/server implementations.
 */
public interface TransactionState {
    boolean active();

    void begin();

    void discard();

    void enqueue(byte[][] argv);

    default boolean aborted() {
        return false;
    }

    default void markAborted() {
        // no-op
    }

    default String tryEnqueue(byte[][] argv) {
        enqueue(argv);
        return null;
    }

    int size();

    List<byte[][]> drain();
}

