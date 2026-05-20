package yier.bubu.redis.command.kernel;

import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;

/**
 * Command-core change observation contract.
 */
@FunctionalInterface
public interface CommandChangeObserver {
    CommandChangeObserver NOOP = (dbIndex, request) -> {
    };

    void onCommandChange(int dbIndex, ExecutionRequest request);

    default void observeExecution(Runnable action) {
        Objects.requireNonNull(action, "action").run();
    }

    static CommandChangeObserver noop() {
        return NOOP;
    }
}
