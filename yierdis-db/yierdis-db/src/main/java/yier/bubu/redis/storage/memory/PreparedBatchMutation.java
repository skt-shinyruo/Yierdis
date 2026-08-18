package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.storage.memory.internal.ledger.AbstractPreparedMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;

final class PreparedBatchMutation<T> extends AbstractPreparedMutation<T> {
    private final PreparedDbMutation<?>[] changes;
    private final T result;

    PreparedBatchMutation(
            PreparedDbMutation<?>[] changes,
            int count,
            T result,
            long actualDeltaBytes
    ) {
        super(actualDeltaBytes, 0L);
        Objects.requireNonNull(changes, "changes");
        if (count < 0 || count > changes.length) {
            throw new IllegalArgumentException("invalid prepared change count");
        }
        this.changes = new PreparedDbMutation<?>[count];
        for (int index = 0; index < count; index++) {
            this.changes[index] = Objects.requireNonNull(changes[index], "change");
        }
        this.result = result;
    }

    @Override
    protected T commitPrepared() {
        for (PreparedDbMutation<?> change : changes) {
            change.commit();
        }
        return result;
    }

    @Override
    protected void releaseSupersededPrepared() {
        for (PreparedDbMutation<?> change : changes) {
            change.releaseSuperseded();
        }
    }

    @Override
    protected void abortPrepared() {
        for (PreparedDbMutation<?> change : changes) {
            change.abort();
        }
    }
}
