package yier.bubu.redis.command.kernel;

import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;

final class CommandChangeEmitter {
    private final CommandChangeObserver observer;
    private final boolean enabled;

    private CommandChangeEmitter(CommandChangeObserver observer) {
        this.observer = observer == null ? CommandChangeObserver.NOOP : observer;
        this.enabled = this.observer != CommandChangeObserver.NOOP;
    }

    static CommandChangeEmitter noop() {
        return new CommandChangeEmitter(CommandChangeObserver.NOOP);
    }

    static CommandChangeEmitter fromOptions(YierdisCommandProcessorOptions options) {
        return new CommandChangeEmitter(options == null ? CommandChangeObserver.NOOP : options.changeObserver());
    }

    static CommandChangeEmitter fromObserver(CommandChangeObserver observer) {
        return new CommandChangeEmitter(observer);
    }

    void execute(ExecutionRequest request, CommandContext ctx, Runnable action) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(action, "action");

        ctx.clearMutationOutcome();
        if (enabled) {
            observer.observeExecution(action);
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
            observer.onCommandChange(dbIndex, request);
        } catch (Throwable ignored) {
            // best-effort: event consumer failures must not affect command execution.
        }
    }
}
