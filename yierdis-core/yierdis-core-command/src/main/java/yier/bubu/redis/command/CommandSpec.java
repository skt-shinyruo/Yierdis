package yier.bubu.redis.command;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Objects;

/**
 * Unified command registration shape: parser + handler + metadata + MULTI policy.
 */
public final class CommandSpec<T> {
    private final CommandParser<T> parser;
    private final CommandHandler<T> handler;
    private final CommandDescriptor descriptor;
    private final String disallowedInMultiError;

    public CommandSpec(
            CommandModule.Handler handler,
            CommandDescriptor descriptor,
            String disallowedInMultiError
    ) {
        this(
                legacyParser(),
                legacyHandler(handler),
                descriptor,
                disallowedInMultiError
        );
    }

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

    CommandParseResult<T> parse(ExecutionRequest request) {
        return parser.parse(ArgReader.of(request));
    }

    void executeParsed(Object parsed, CommandContext ctx) {
        @SuppressWarnings("unchecked")
        T typed = (T) parsed;
        handler.execute(typed, ctx);
    }

    CommandModule.Handler handler() {
        return (request, ctx) -> {
            CommandParseResult<T> result = parse(request);
            if (!result.ok()) {
                ctx.out().error(result.error().toReplyMessage());
                return;
            }
            handler.execute(result.value(), ctx);
        };
    }

    public CommandDescriptor descriptor() {
        return descriptor;
    }

    public String disallowedInMultiError() {
        return disallowedInMultiError;
    }

    @SuppressWarnings("unchecked")
    private static <T> CommandParser<T> legacyParser() {
        return args -> (CommandParseResult<T>) CommandParseResult.ok(args.request());
    }

    @SuppressWarnings("unchecked")
    private static <T> CommandHandler<T> legacyHandler(CommandModule.Handler handler) {
        Objects.requireNonNull(handler, "handler");
        return (CommandHandler<T>) (CommandHandler<ExecutionRequest>) handler::execute;
    }
}
