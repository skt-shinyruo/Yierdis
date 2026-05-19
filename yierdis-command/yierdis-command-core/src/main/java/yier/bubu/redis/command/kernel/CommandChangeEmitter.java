package yier.bubu.redis.command.kernel;

import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRecord;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.runtime.api.YierdisChangeEvent;
import yier.bubu.redis.runtime.api.YierdisChangeEventBridge;
import yier.bubu.redis.runtime.api.YierdisChangeSink;
import yier.bubu.redis.storage.api.DbChangeContext;
import yier.bubu.redis.storage.api.DbChangeListener;

import java.util.Objects;

final class CommandChangeEmitter {
    private final YierdisChangeSink sink;
    private final DbChangeListener dbChangeListener;
    private final boolean enabled;

    private CommandChangeEmitter(YierdisChangeSink sink) {
        this.sink = sink == null ? YierdisChangeSink.NOOP : sink;
        this.dbChangeListener = YierdisChangeEventBridge.forSink(this.sink);
        this.enabled = this.sink != YierdisChangeSink.NOOP;
    }

    static CommandChangeEmitter noop() {
        return new CommandChangeEmitter(YierdisChangeSink.NOOP);
    }

    static CommandChangeEmitter fromOptions(YierdisCommandProcessorOptions options) {
        return new CommandChangeEmitter(options == null ? YierdisChangeSink.NOOP : options.changeSink());
    }

    static CommandChangeEmitter fromSink(YierdisChangeSink sink) {
        return new CommandChangeEmitter(sink);
    }

    void execute(ExecutionRequest request, CommandContext ctx, Runnable action) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(action, "action");

        ctx.clearMutationOutcome();
        if (enabled) {
            try (DbChangeContext.Scope ignored = DbChangeContext.open(dbChangeListener)) {
                action.run();
            }
        } else {
            action.run();
        }

        if (enabled && ctx.changedAny()) {
            emitUserCommandChange(request, ctx);
        }
    }

    private void emitUserCommandChange(ExecutionRequest request, CommandContext ctx) {
        int dbIndex = Math.max(0, ctx.dbIndexSession().dbIndex());
        try {
            sink.onChange(new YierdisChangeEvent(new ExecutionRecord(dbIndex, request)));
        } catch (Throwable ignored) {
            // best-effort: event consumer failures must not affect command execution.
        }
    }
}
