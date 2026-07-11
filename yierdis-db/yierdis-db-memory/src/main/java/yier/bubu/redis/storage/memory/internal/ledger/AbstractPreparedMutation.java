package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.Objects;
import yier.bubu.redis.storage.api.MutationOutcome;

public abstract class AbstractPreparedMutation<T> implements PreparedDbMutation<T> {
    private final long actualDeltaBytes;
    private final long stagedNonNativeGrowthBytes;
    private final MutationOutcome outcome;
    private State state = State.PREPARED;

    protected AbstractPreparedMutation(
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome
    ) {
        if (stagedNonNativeGrowthBytes < 0) {
            throw new IllegalArgumentException("stagedNonNativeGrowthBytes must be >= 0");
        }
        this.actualDeltaBytes = actualDeltaBytes;
        this.stagedNonNativeGrowthBytes = stagedNonNativeGrowthBytes;
        this.outcome = Objects.requireNonNull(outcome, "outcome");
    }

    @Override
    public final long actualDeltaBytes() {
        return actualDeltaBytes;
    }

    @Override
    public final long stagedNonNativeGrowthBytes() {
        return stagedNonNativeGrowthBytes;
    }

    @Override
    public final MutationOutcome outcome() {
        return outcome;
    }

    @Override
    public final T commit() {
        requireState(State.PREPARED, "commit");
        state = State.COMMITTING;
        T result = commitPrepared();
        state = State.COMMITTED;
        return result;
    }

    @Override
    public final void releaseSuperseded() {
        if (state == State.RELEASED) {
            return;
        }
        requireState(State.COMMITTED, "release superseded resources");
        releaseSupersededPrepared();
        state = State.RELEASED;
    }

    @Override
    public final void abort() {
        if (state != State.PREPARED) {
            return;
        }
        state = State.ABORTED;
        abortPrepared();
    }

    protected abstract T commitPrepared();

    protected abstract void releaseSupersededPrepared();

    protected abstract void abortPrepared();

    private void requireState(State expected, String operation) {
        if (state != expected) {
            throw new IllegalStateException(
                    "cannot " + operation + " prepared mutation in state " + state
            );
        }
    }

    private enum State {
        PREPARED,
        COMMITTING,
        COMMITTED,
        RELEASED,
        ABORTED
    }
}
