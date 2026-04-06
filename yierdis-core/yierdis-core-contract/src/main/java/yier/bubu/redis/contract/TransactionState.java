package yier.bubu.redis.contract;

import java.util.ArrayList;
import java.util.Arrays;
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

    default void enqueue(ExecutionRequest request) {
        if (request == null) {
            return;
        }
        enqueue(copyArgv(request));
    }

    @Deprecated(forRemoval = false)
    default void enqueue(byte[][] argv) {
        if (argv == null) {
            return;
        }
        enqueue(ByteArrayExecutionRequest.copyOf(Arrays.asList(argv)));
    }

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
        return tryEnqueue(copyArgv(request));
    }

    @Deprecated(forRemoval = false)
    default String tryEnqueue(byte[][] argv) {
        enqueue(argv);
        return null;
    }

    int size();

    List<?> drain();

    default List<ExecutionRequest> drainRequests() {
        List<?> drained = drain();
        if (drained == null || drained.isEmpty()) {
            return List.of();
        }
        ArrayList<ExecutionRequest> requests = new ArrayList<>(drained.size());
        for (Object entry : drained) {
            requests.add(asExecutionRequest(entry));
        }
        return requests;
    }

    private static ExecutionRequest asExecutionRequest(Object entry) {
        if (entry instanceof ExecutionRequest request) {
            return request;
        }
        if (entry instanceof byte[][] argv) {
            return ByteArrayExecutionRequest.copyOf(Arrays.asList(argv));
        }
        throw new IllegalStateException("Unsupported queued request type: " + (entry == null ? "null" : entry.getClass().getName()));
    }

    private static byte[][] copyArgv(ExecutionRequest request) {
        int argc = request.argc();
        byte[][] argv = new byte[argc][];
        for (int i = 0; i < argc; i++) {
            if (request.isNull(i)) {
                continue;
            }
            int len = request.len(i);
            if (len < 0) {
                continue;
            }
            byte[] copy = new byte[len];
            if (len > 0) {
                request.copyToByteArray(i, copy, 0);
            }
            argv[i] = copy;
        }
        return argv;
    }
}
