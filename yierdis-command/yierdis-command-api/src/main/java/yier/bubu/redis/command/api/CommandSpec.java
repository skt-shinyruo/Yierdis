package yier.bubu.redis.command.api;

import java.util.Objects;
import yier.bubu.redis.execution.api.ExecutionRequest;

public final class CommandSpec<T> {
    private final CommandSyntax syntax;
    private final CommandParser<T> parser;
    private final CommandHandler<T> handler;
    private final CommandReplyPlanner replyPlanner;

    private CommandSpec(
            CommandSyntax syntax,
            CommandParser<T> parser,
            CommandHandler<T> handler,
            CommandReplyPlanner replyPlanner
    ) {
        this.syntax = Objects.requireNonNull(syntax, "syntax");
        this.parser = Objects.requireNonNull(parser, "parser");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.replyPlanner = replyPlanner;
    }

    public static <T> CommandSpec<T> of(
            CommandSyntax syntax,
            CommandParser<T> parser,
            CommandHandler<T> handler
    ) {
        return new CommandSpec<>(syntax, parser, handler, null);
    }

    public CommandSpec<T> withReplyPlanner(CommandReplyPlanner planner) {
        return new CommandSpec<>(syntax, parser, handler,
                Objects.requireNonNull(planner, "planner"));
    }

    public CommandParseResult<T> parse(ExecutionRequest request) {
        ArgReader args = ArgReader.of(Objects.requireNonNull(request, "request"));
        CommandParseError arityError = syntax.arity().validate(syntax.nameLower(), args);
        return arityError == null
                ? parser.parse(args)
                : CommandParseResult.error(arityError);
    }

    public void executeParsed(Object parsed, yier.bubu.redis.execution.api.CommandContext context) {
        @SuppressWarnings("unchecked")
        T typed = (T) parsed;
        handler.execute(typed, context);
    }

    public yier.bubu.redis.execution.api.ReplyPlan planReply(ExecutionRequest request) {
        if (replyPlanner == null) {
            return yier.bubu.redis.execution.api.ReplyPlan.maximum();
        }
        yier.bubu.redis.execution.api.ReplyPlan plan = replyPlanner.plan(request);
        return plan == null ? yier.bubu.redis.execution.api.ReplyPlan.maximum() : plan;
    }

    public CommandSyntax syntax() {
        return syntax;
    }
}
