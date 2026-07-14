package yier.bubu.redis.runtime.api;

import java.util.Objects;
import yier.bubu.redis.common.command.CommandRecordView;
import yier.bubu.redis.execution.api.ExecutionRecord;

/**
 * 交付给变更 sink 的事件视图。
 *
 * <p>普通公开构造函数持有防御性快照。commit stream 使用的借用构造只在 callback 返回前有效，
 * 由 {@link #close()} 失效该视图。</p>
 */
public final class YierdisChangeEvent implements AutoCloseable {
    private final ExecutionRecord record;
    private final YierdisChangeKind kind;
    private final boolean synthetic;
    private final long sequence;
    private final long committedMemoryDelta;
    private final long commitAttemptTimestampMillis;
    private final AutoCloseable callbackLifetime;
    private boolean closed;

    public YierdisChangeEvent(ExecutionRecord record) {
        this(record, YierdisChangeKind.USER_COMMAND, false);
    }

    public YierdisChangeEvent(ExecutionRecord record, YierdisChangeKind kind, boolean synthetic) {
        this(record, kind, synthetic, 0L, 0L, 0L, null);
    }

    public static YierdisChangeEvent borrowed(
            long sequence,
            int dbIndex,
            YierdisChangeKind kind,
            boolean synthetic,
            CommandRecordView request,
            long committedMemoryDelta,
            long commitAttemptTimestampMillis,
            AutoCloseable callbackLifetime
    ) {
        return new YierdisChangeEvent(
                ExecutionRecord.borrowed(dbIndex, request),
                kind,
                synthetic,
                sequence,
                committedMemoryDelta,
                commitAttemptTimestampMillis,
                callbackLifetime
        );
    }

    private YierdisChangeEvent(
            ExecutionRecord record,
            YierdisChangeKind kind,
            boolean synthetic,
            long sequence,
            long committedMemoryDelta,
            long commitAttemptTimestampMillis,
            AutoCloseable callbackLifetime
    ) {
        this.record = Objects.requireNonNull(record, "record");
        this.kind = kind == null ? YierdisChangeKind.USER_COMMAND : kind;
        this.synthetic = synthetic;
        this.sequence = sequence;
        this.committedMemoryDelta = committedMemoryDelta;
        this.commitAttemptTimestampMillis = commitAttemptTimestampMillis;
        this.callbackLifetime = callbackLifetime;
    }

    public ExecutionRecord record() {
        return record;
    }

    public int dbIndex() {
        return record.dbIndex();
    }

    public YierdisChangeKind kind() {
        return kind;
    }

    public boolean synthetic() {
        return synthetic;
    }

    public long sequence() {
        return sequence;
    }

    public long committedMemoryDelta() {
        return committedMemoryDelta;
    }

    public long commitAttemptTimestampMillis() {
        return commitAttemptTimestampMillis;
    }

    public CommandRecordView request() {
        return record.request();
    }

    @Override
    public void close() {
        if (closed || callbackLifetime == null) {
            closed = true;
            return;
        }
        closed = true;
        try {
            callbackLifetime.close();
        } catch (Exception e) {
            throw new IllegalStateException("failed to close change event callback view", e);
        }
    }
}
