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
                CommandDescriptor.of(1, 0, 0, 0),
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
                CommandDescriptor.of(1, 0, 0, 0),
                CommandParsers.exactRequest(1, "unplanned"),
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
}
