package yier.bubu.redis.app.server;

import yier.bubu.redis.command.kernel.CommandChangeObserver;
import yier.bubu.redis.execution.api.ExecutionRecord;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.runtime.api.YierdisChangeEvent;
import yier.bubu.redis.runtime.api.YierdisChangeEventBridge;
import yier.bubu.redis.runtime.api.YierdisChangeSink;
import yier.bubu.redis.storage.api.DbChangeContext;
import yier.bubu.redis.storage.api.DbChangeListener;

final class RuntimeChangeSinkCommandChangeObserver implements CommandChangeObserver {
    private final YierdisChangeSink sink;
    private final DbChangeListener dbChangeListener;

    private RuntimeChangeSinkCommandChangeObserver(YierdisChangeSink sink) {
        this.sink = sink == null ? YierdisChangeSink.NOOP : sink;
        this.dbChangeListener = YierdisChangeEventBridge.forSink(this.sink);
    }

    static CommandChangeObserver fromSink(YierdisChangeSink sink) {
        YierdisChangeSink safeSink = sink == null ? YierdisChangeSink.NOOP : sink;
        if (safeSink == YierdisChangeSink.NOOP) {
            return CommandChangeObserver.NOOP;
        }
        return new RuntimeChangeSinkCommandChangeObserver(safeSink);
    }

    @Override
    public void observeExecution(Runnable action) {
        try (DbChangeContext.Scope ignored = DbChangeContext.open(dbChangeListener)) {
            action.run();
        }
    }

    @Override
    public void onCommandChange(int dbIndex, ExecutionRequest request) {
        try {
            sink.onChange(new YierdisChangeEvent(new ExecutionRecord(dbIndex, request)));
        } catch (Throwable ignored) {
            // best-effort: event consumer failures must not affect command execution.
        }
    }
}
