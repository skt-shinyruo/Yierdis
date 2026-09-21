package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.Objects;

public final class PreparedCallbackMutation<T> extends AbstractPreparedMutation<T> {
    private static final Runnable NOOP = () -> {
    };

    private final T result;
    private final Runnable commit;
    private final boolean trimNativePagesAfterCommit;
    private Runnable releaseSuperseded;
    private Runnable abort;

    public PreparedCallbackMutation(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            Runnable commit,
            Runnable releaseSuperseded,
            Runnable abort,
            boolean trimNativePagesAfterCommit
    ) {
        super(actualDeltaBytes, stagedNonNativeGrowthBytes);
        this.result = result;
        this.commit = Objects.requireNonNull(commit, "commit");
        this.trimNativePagesAfterCommit = trimNativePagesAfterCommit;
        this.releaseSuperseded = releaseSuperseded == null ? NOOP : releaseSuperseded;
        this.abort = abort == null ? NOOP : abort;
    }

    @Override
    public boolean shouldTrimNativePagesAfterCommit() {
        return trimNativePagesAfterCommit;
    }

    @Override
    protected T commitPrepared() {
        commit.run();
        return result;
    }

    @Override
    protected void releaseSupersededPrepared() {
        Runnable callback = releaseSuperseded;
        releaseSuperseded = NOOP;
        callback.run();
    }

    @Override
    protected void abortPrepared() {
        Runnable callback = abort;
        abort = NOOP;
        callback.run();
    }
}
