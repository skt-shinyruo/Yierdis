package yier.bubu.redis.command.kernel;

import java.util.Objects;
import java.util.function.Consumer;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;

final class PreparedCommands {
    private PreparedCommands() {
    }

    static PreparedCommand fixed(ReplyShape shape, Consumer<CommandExecutionContext> execution) {
        Objects.requireNonNull(shape, "shape");
        Objects.requireNonNull(execution, "execution");
        return new PreparedCommand() {
            @Override
            public ReplyShape replyShape() {
                return shape;
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return ValidationResult.VALID;
            }

            @Override
            public void execute(CommandExecutionContext context) {
                execution.accept(context);
            }

            @Override
            public void close() {
            }
        };
    }

    static PreparedCommand error(String message) {
        return fixed(ReplyShapes.error(message), context -> context.reply().error(message));
    }

    static PreparedCommand error(String message, Runnable beforeReply) {
        Objects.requireNonNull(beforeReply, "beforeReply");
        return fixed(ReplyShapes.error(message), context -> {
            beforeReply.run();
            context.reply().error(message);
        });
    }
}
