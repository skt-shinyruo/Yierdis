package yier.bubu.redis.testutil;

import java.util.Objects;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;

public final class TestPreparedCommands {
    private TestPreparedCommands() {
    }

    public static PreparedCommand simpleString(String value) {
        String response = Objects.requireNonNull(value, "value");
        return new PreparedCommand() {
            @Override
            public ReplyShape replyShape() {
                return ReplyShapes.simpleString(response);
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return ValidationResult.VALID;
            }

            @Override
            public void execute(CommandExecutionContext context) {
                context.reply().simpleString(response);
            }

            @Override
            public void close() {
            }
        };
    }
}
