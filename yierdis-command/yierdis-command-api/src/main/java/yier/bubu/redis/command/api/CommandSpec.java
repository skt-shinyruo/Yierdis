package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;

/**
 * Unified command registration shape: parser + handler + metadata + MULTI policy.
 */
public final class CommandSpec<T> {
    private final CommandParser<T> parser;
    private final CommandHandler<T> handler;
    private final CommandDescriptor descriptor;
    private final String disallowedInMultiError;

    private CommandSpec(
            CommandParser<T> parser,
            CommandHandler<T> handler,
            CommandDescriptor descriptor,
            String disallowedInMultiError
    ) {
        this.parser = Objects.requireNonNull(parser, "parser");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.disallowedInMultiError = disallowedInMultiError;
    }

    public static <T> CommandSpec<T> of(
            CommandDescriptor descriptor,
            CommandParser<T> parser,
            CommandHandler<T> handler
    ) {
        return new CommandSpec<>(parser, handler, descriptor, null);
    }

    public static <T> CommandSpec<T> disallowedInMulti(
            CommandDescriptor descriptor,
            CommandParser<T> parser,
            CommandHandler<T> handler,
            String errorMessage
    ) {
        return new CommandSpec<>(parser, handler, descriptor, errorMessage);
    }

    public CommandParseResult<T> parse(ExecutionRequest request) {
        return parser.parse(ArgReader.of(request));
    }

    public void executeParsed(Object parsed, CommandContext ctx) {
        @SuppressWarnings("unchecked")
        T typed = (T) parsed;
        handler.execute(typed, ctx);
    }

    public CommandDescriptor descriptor() {
        return descriptor;
    }

    public String disallowedInMultiError() {
        return disallowedInMultiError;
    }
}
