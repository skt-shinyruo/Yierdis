package yier.bubu.redis.execution.api;

import java.util.List;
import java.util.function.Consumer;

public interface TransactionState {
    boolean active();

    boolean aborted();

    void begin();

    void markAborted();

    String tryEnqueue(ExecutionRequest request);

    int size();

    void forEachQueued(Consumer<? super ExecutionRequest> visitor);

    List<ExecutionRequest> drain();

    void discard();

}
