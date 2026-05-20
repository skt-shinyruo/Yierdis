package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandChangeObserver;
import yier.bubu.redis.command.kernel.YierdisCommandProcessorOptions;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.execution.api.ExecutionRecord;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.runtime.api.YierdisChangeEvent;
import yier.bubu.redis.runtime.api.YierdisChangeEventBridge;
import yier.bubu.redis.runtime.api.YierdisChangeSink;
import yier.bubu.redis.storage.api.DbChangeContext;
import yier.bubu.redis.storage.api.DbChangeListener;
import yier.bubu.redis.storage.api.DbEngine;

import java.util.ArrayList;
import java.util.List;

public final class TestCommandProcessors {
    private TestCommandProcessors() {
    }

    public static YierdisFastCommandProcessor forDb(DbEngine db, CommandModule... extraModules) {
        return create(YierdisChangeSink.NOOP, DefaultCommandModules.create(db), extraModules);
    }

    public static YierdisFastCommandProcessor forDbWithChangeSink(
            DbEngine db,
            YierdisChangeSink changeSink,
            CommandModule... extraModules
    ) {
        return create(changeSink, DefaultCommandModules.create(db), extraModules);
    }

    public static YierdisFastCommandProcessor forRouter(
            YierdisDbRouter dbRouter,
            CommandModule... extraModules
    ) {
        return create(YierdisChangeSink.NOOP, DefaultCommandModules.create(dbRouter, null), extraModules);
    }

    private static YierdisFastCommandProcessor create(
            YierdisChangeSink changeSink,
            CommandModule defaults,
            CommandModule... extraModules
    ) {
        List<CommandModule> modules = new ArrayList<>();
        modules.add(defaults);
        if (extraModules != null) {
            for (CommandModule extraModule : extraModules) {
                modules.add(extraModule);
            }
        }
        YierdisCommandProcessorOptions options = YierdisCommandProcessorOptions.builder()
                .changeObserver(changeObserver(changeSink))
                .build();
        return new YierdisFastCommandProcessor(options, modules);
    }

    private static CommandChangeObserver changeObserver(YierdisChangeSink changeSink) {
        YierdisChangeSink safeSink = changeSink == null ? YierdisChangeSink.NOOP : changeSink;
        if (safeSink == YierdisChangeSink.NOOP) {
            return CommandChangeObserver.NOOP;
        }
        return new ChangeSinkObserver(safeSink);
    }

    private static final class ChangeSinkObserver implements CommandChangeObserver {
        private final YierdisChangeSink sink;
        private final DbChangeListener dbChangeListener;

        private ChangeSinkObserver(YierdisChangeSink sink) {
            this.sink = sink;
            this.dbChangeListener = YierdisChangeEventBridge.forSink(sink);
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
                sink.onChange(new YierdisChangeEvent(new ExecutionRecord(Math.max(0, dbIndex), request)));
            } catch (Throwable ignored) {
                // best-effort: event consumer failures must not affect command execution.
            }
        }
    }
}
