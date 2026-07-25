package yier.bubu.redis.command.defaults.list;

import java.util.Objects;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.BulkStringReplyAdapter;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

public final class ListCommands implements CommandModule {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public ListCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandDefinition<>(syntax("LPUSH", CommandArity.min(3)),
                CommandParsers.request(), (request, context) -> push(request, true)));
        registration.register(new CommandDefinition<>(syntax("RPUSH", CommandArity.min(3)),
                CommandParsers.request(), (request, context) -> push(request, false)));
        registration.register(new CommandDefinition<>(syntax("LRANGE", CommandArity.exact(4)),
                CommandParsers.request(), this::lrange));
        registration.register(new CommandDefinition<>(syntax("LPOP", CommandArity.oneOf(2, 3)),
                CommandParsers.request(), (request, context) -> pop(request, context, true)));
        registration.register(new CommandDefinition<>(syntax("RPOP", CommandArity.oneOf(2, 3)),
                CommandParsers.request(), (request, context) -> pop(request, context, false)));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private PreparedCommand push(ExecutionRequest request, boolean left) {
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            int valuesLen = request.argc() - 2;
            support.sliceResetFromRequest(request, 2, valuesLen);
            try {
                long length = (left
                        ? support.commandDb(execution).writes().lists()
                                .lpush(request.readOnlyByteArray(1), support.slice())
                        : support.commandDb(execution).writes().lists()
                                .rpush(request.readOnlyByteArray(1), support.slice())).value();
                execution.reply().integer(length);
            } finally {
                support.clearScratch(valuesLen);
            }
        });
    }

    private PreparedCommand lrange(ExecutionRequest request, CommandPreparationContext context) {
        int start = CommandSupport.parseIntClamped(request, 2, "start");
        int stop = CommandSupport.parseIntClamped(request, 3, "stop");
        return CommandSupport.sequence(support.commandDb(context).reads().lists()
                .lrange(request.readOnlyByteArray(1), start, stop));
    }

    private PreparedCommand pop(
            ExecutionRequest request,
            CommandPreparationContext context,
            boolean left
    ) {
        boolean hasCount = request.argc() == 3;
        int count = 1;
        if (hasCount) {
            long parsed = CommandSupport.parseLong(request, 2, "count");
            if (parsed < 0L) {
                throw new IllegalArgumentException("value is not an integer or out of range");
            }
            count = parsed > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) parsed;
        }
        PreparedMutation<PoppedValueSequence> mutation = support.commandDb(context).writes().lists()
                .preparePop(request.readOnlyByteArray(1), count, left);
        PoppedValueSequence preview = mutation.preview();
        ReplyShape shape = popShape(preview, hasCount);
        return new PreparedPop(mutation, preview, hasCount, shape);
    }

    private static ReplyShape popShape(PoppedValueSequence preview, boolean hasCount) {
        if (preview == null || preview.isNull()) {
            return hasCount ? ReplyShapes.nullArray() : ReplyShapes.nullValue();
        }
        if (hasCount) {
            return ReplyShapes.sequence(
                    preview.elementCount(),
                    preview.retainedMemoryBytes(),
                    consumer -> preview.visitElementLengths(consumer::accept)
            );
        }
        int[] payloadLength = {-1};
        preview.visitElementLengths(length -> payloadLength[0] = length);
        return payloadLength[0] < 0
                ? ReplyShapes.nullValue()
                : ReplyShapes.bulkString(payloadLength[0], preview.retainedMemoryBytes());
    }

    private static final class PreparedPop implements PreparedCommand {
        private final PreparedMutation<PoppedValueSequence> mutation;
        private final PoppedValueSequence preview;
        private final boolean hasCount;
        private final ReplyShape shape;
        private boolean closed;

        private PreparedPop(
                PreparedMutation<PoppedValueSequence> mutation,
                PoppedValueSequence preview,
                boolean hasCount,
                ReplyShape shape
        ) {
            this.mutation = Objects.requireNonNull(mutation, "mutation");
            this.preview = preview;
            this.hasCount = hasCount;
            this.shape = Objects.requireNonNull(shape, "shape");
        }

        @Override
        public ReplyShape replyShape() {
            return shape;
        }

        @Override
        public ValidationResult validateBeforeExecute() {
            return mutation.isCurrent() ? ValidationResult.VALID : ValidationResult.STALE;
        }

        @Override
        public void execute(CommandExecutionContext context) {
            mutation.commit(context.mutationContext());
            if (preview == null || preview.isNull()) {
                if (hasCount) {
                    context.reply().nullArray();
                } else {
                    context.reply().nullValue();
                }
                return;
            }
            if (hasCount) {
                context.reply().arrayHeader(preview.elementCount());
            }
            preview.emitTo(new BulkStringReplyAdapter(context.reply()));
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                mutation.close();
            }
        }
    }
}
