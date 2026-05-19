package yier.bubu.redis.runtime.api;

import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRecord;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.storage.api.DbChange;
import yier.bubu.redis.storage.api.DbChangeKind;
import yier.bubu.redis.storage.api.DbChangeListener;

import java.util.List;

public final class YierdisChangeEventBridge {
    private YierdisChangeEventBridge() {
    }

    public static DbChangeListener forSink(YierdisChangeSink sink) {
        YierdisChangeSink safeSink = sink == null ? YierdisChangeSink.NOOP : sink;
        if (safeSink == YierdisChangeSink.NOOP) {
            return DbChangeListener.NOOP;
        }
        return change -> emit(safeSink, change);
    }

    private static void emit(YierdisChangeSink sink, DbChange change) {
        if (change == null) {
            return;
        }
        try {
            sink.onChange(new YierdisChangeEvent(
                    new ExecutionRecord(Math.max(0, change.dbIndex()), executionRequest(change)),
                    changeKind(change.kind()),
                    true
            ));
        } catch (Throwable ignored) {
            // best-effort: event consumer failures must not affect DB mutation paths
        }
    }

    private static ExecutionRequest executionRequest(DbChange change) {
        return ByteArrayExecutionRequest.copyOf(List.of(change.commandArgv()));
    }

    private static YierdisChangeKind changeKind(DbChangeKind kind) {
        if (kind == DbChangeKind.EXPIRED) {
            return YierdisChangeKind.EXPIRED;
        }
        if (kind == DbChangeKind.EVICTED) {
            return YierdisChangeKind.EVICTED;
        }
        return YierdisChangeKind.USER_COMMAND;
    }
}
