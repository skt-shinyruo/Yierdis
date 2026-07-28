package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandHandler;
import yier.bubu.redis.command.api.CommandInvocation;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;

import java.util.Objects;

// TODO(Task 15): 所有命令模块迁移到 CommandSpec 后删除 LegacyCommandAdapter.java。
final class LegacyCommandAdapter {
    private LegacyCommandAdapter() {
    }

    static CommandSpec adapt(CommandDefinition<?> definition) {
        CommandDefinition<?> legacy = Objects.requireNonNull(definition, "definition");
        return new CommandSpec(legacy.syntax(), new LegacyHandler<>(legacy));
    }

    static CommandDefinition<?> definitionOf(CommandSpec spec) {
        if (spec.handler() instanceof LegacyHandler<?> legacyHandler) {
            return legacyHandler.definition;
        }
        return null;
    }

    private static final class LegacyHandler<T> implements CommandHandler {
        private final CommandDefinition<T> definition;

        private LegacyHandler(CommandDefinition<T> definition) {
            this.definition = definition;
        }

        @Override
        public CommandInvocation parse(CommandArgs args) throws CommandParseException {
            CommandParseResult<T> parsed = definition.parse(args.request());
            if (!parsed.ok()) {
                throw new CommandParseException(parsed.error().toReplyMessage());
            }
            T value = parsed.value();
            return session -> prepare(value, session);
        }

        private PreparedCommand prepare(T parsed, CommandSession session) {
            return definition.preparer().prepare(parsed, new CommandPreparationContext(session));
        }
    }
}
