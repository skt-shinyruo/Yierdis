package yier.bubu.redis.command.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyPlan;

import java.util.List;

public class CommandSpecTest {
    @Test
    public void replyPlannerReadsTheOriginalRequestWithoutRunningTheParser() {
        CommandParser<ExecutionRequest> parser = args -> {
            throw new AssertionError("reply planning must not run the command parser");
        };
        ExecutionRequest[] plannedRequest = new ExecutionRequest[1];
        CommandSpec<ExecutionRequest> spec = CommandSpec.of(
                syntax("PLANNED", CommandArity.exact(1)),
                parser,
                (request, ctx) -> { }
        ).withReplyPlanner(request -> {
            plannedRequest[0] = request;
            return ReplyPlan.exact(17L, 3L);
        });
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("PLANNED", List.of());
        try {
            Assert.assertEquals(ReplyPlan.exact(17L, 3L), spec.planReply(request));
            Assert.assertSame(request, plannedRequest[0]);
        } finally {
            request.close();
        }
    }

    @Test
    public void missingOrNullReplyPlanFallsBackToMaximum() {
        CommandSpec<ExecutionRequest> unplanned = CommandSpec.of(
                syntax("UNPLANNED", CommandArity.exact(1)),
                CommandParsers.request(),
                (request, ctx) -> { }
        );
        CommandSpec<ExecutionRequest> nullPlan = unplanned.withReplyPlanner(request -> null);
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("UNPLANNED", List.of());
        try {
            Assert.assertEquals(ReplyPlan.maximum(), unplanned.planReply(request));
            Assert.assertEquals(ReplyPlan.maximum(), nullPlan.planReply(request));
        } finally {
            request.close();
        }
    }

    @Test
    public void specValidatesItsSyntaxBeforeInvokingTheCustomParser() {
        java.util.concurrent.atomic.AtomicInteger parserCalls =
                new java.util.concurrent.atomic.AtomicInteger();
        CommandSyntax syntax = new CommandSyntax(
                "AUTH", CommandArity.min(2), CommandKeySpec.NONE,
                TransactionPolicy.QUEUEABLE);
        CommandSpec<ArgReader> spec = CommandSpec.of(
                syntax,
                args -> {
                    parserCalls.incrementAndGet();
                    return CommandParseResult.ok(args);
                },
                (args, context) -> { }
        );

        CommandParseResult<ArgReader> invalid = spec.parse(request("AUTH"));
        Assert.assertFalse(invalid.ok());
        Assert.assertEquals(0, parserCalls.get());
        Assert.assertSame(syntax, spec.syntax());
    }

    private static CommandSyntax syntax(String name, CommandArity arity) {
        return new CommandSyntax(name, arity, CommandKeySpec.NONE, TransactionPolicy.QUEUEABLE);
    }

    private static ByteArrayExecutionRequest request(String... args) {
        return ByteArrayExecutionRequest.fromUtf8(args[0], List.of(args).subList(1, args.length));
    }
}
