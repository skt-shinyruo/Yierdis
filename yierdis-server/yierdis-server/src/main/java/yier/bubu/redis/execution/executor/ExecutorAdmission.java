package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public final class ExecutorAdmission<C extends ExecutionConnection> implements AutoCloseable {
    private enum State {
        OPEN,
        PUBLISHED,
        CLOSED
    }

    private final CommandExecutorSubmitter<C> owner;
    private final C connection;
    private final int retainedBytes;
    private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);

    ExecutorAdmission(CommandExecutorSubmitter<C> owner, C connection, int retainedBytes) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.connection = Objects.requireNonNull(connection, "connection");
        this.retainedBytes = retainedBytes;
    }

    public C connection() {
        return connection;
    }

    public int retainedBytes() {
        return retainedBytes;
    }

    public void publish(ExecutionRequest request, ExecutionReply reply) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(reply, "reply");
        if (!state.compareAndSet(State.OPEN, State.PUBLISHED)) {
            throw new IllegalStateException("executor admission is no longer publishable");
        }
        owner.publish(this, request, reply);
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.OPEN, State.CLOSED)) {
            owner.releaseUnpublished(this);
        }
    }
}
