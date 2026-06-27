package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandChangeObserver;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.YierdisCommandProcessorOptions;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.execution.api.DbIndexSession;
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
import java.util.Objects;

public final class TestCommandComposition {
    private TestCommandComposition() {
    }

    public static YierdisFastCommandProcessor createProcessor(DbEngine db, CommandModule... extraModules) {
        return createProcessor(singleDbRouter(db), YierdisChangeSink.NOOP, extraModules);
    }

    public static YierdisFastCommandProcessor createProcessor(
            YierdisDbRouter dbRouter,
            YierdisChangeSink changeSink,
            CommandModule... extraModules
    ) {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(options(changeSink), registry);
        CommandRegistries.registerTransactionSupport(registry, processor::execute);
        List<CommandModule> modules = new ArrayList<>();
        modules.add(DefaultCommandModules.create(dbRouter, null));
        if (extraModules != null) {
            for (CommandModule extraModule : extraModules) {
                modules.add(extraModule);
            }
        }
        CommandRegistries.registerInto(registry, modules);
        return processor;
    }

    private static YierdisDbRouter singleDbRouter(DbEngine db) {
        DbEngine fixed = Objects.requireNonNull(db, "db");
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(DbIndexSession session) {
                return fixed;
            }

            @Override
            public int databases() {
                return 1;
            }
        };
    }

    private static YierdisCommandProcessorOptions options(YierdisChangeSink changeSink) {
        return YierdisCommandProcessorOptions.builder()
                .changeObserver(changeObserver(changeSink))
                .build();
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
