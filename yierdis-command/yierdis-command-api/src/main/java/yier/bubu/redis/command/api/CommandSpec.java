package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyPlan;

import java.util.Objects;

/**
 * Unified command registration shape: parser + handler + metadata + MULTI policy.
 */
public final class CommandSpec<T> {
    private final CommandParser<T> parser;
    private final CommandHandler<T> handler;
    private final CommandDescriptor descriptor;
    private final String disallowedInMultiError;
    private final CommandReplyPlanner replyPlanner;

    private CommandSpec(
            CommandParser<T> parser,
            CommandHandler<T> handler,
            CommandDescriptor descriptor,
            String disallowedInMultiError,
            CommandReplyPlanner replyPlanner
    ) {
        this.parser = Objects.requireNonNull(parser, "parser");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.disallowedInMultiError = disallowedInMultiError;
        this.replyPlanner = replyPlanner;
    }

    public static <T> CommandSpec<T> of(
            CommandDescriptor descriptor,
            CommandParser<T> parser,
            CommandHandler<T> handler
    ) {
        return new CommandSpec<>(parser, handler, descriptor, null, null);
    }

    public static <T> CommandSpec<T> disallowedInMulti(
            CommandDescriptor descriptor,
            CommandParser<T> parser,
            CommandHandler<T> handler,
            String errorMessage
    ) {
        return new CommandSpec<>(parser, handler, descriptor, errorMessage, null);
    }

    /**
     * 返回带 request-only 回复计划器的新 spec。
     */
    public CommandSpec<T> withReplyPlanner(CommandReplyPlanner planner) {
        return new CommandSpec<>(parser, handler, descriptor, disallowedInMultiError,
                Objects.requireNonNull(planner, "planner"));
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

    /**
     * 计算无需执行 parser 或 handler 即可证明的回复上界。
     */
    public ReplyPlan planReply(ExecutionRequest request) {
        Objects.requireNonNull(request, "request");
        if (replyPlanner == null) {
            return ReplyPlan.maximum();
        }
        ReplyPlan plan = replyPlanner.plan(request);
        return plan == null ? ReplyPlan.maximum() : plan;
    }
}
