package yier.bubu.redis.command.defaults.list;

import java.util.Objects;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.BulkStringReplyAdapter;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
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
                CommandParsers.args(), (args, context) -> push(args, true)));
        registration.register(new CommandDefinition<>(syntax("RPUSH", CommandArity.min(3)),
                CommandParsers.args(), (args, context) -> push(args, false)));
        registration.register(new CommandDefinition<>(syntax("LRANGE", CommandArity.exact(4)),
                CommandParsers.args(), this::lrange));
        registration.register(new CommandDefinition<>(syntax("LPOP", CommandArity.oneOf(2, 3)),
                CommandParsers.args(), (args, context) -> pop(args, context, true)));
        registration.register(new CommandDefinition<>(syntax("RPOP", CommandArity.oneOf(2, 3)),
                CommandParsers.args(), (args, context) -> pop(args, context, false)));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private PreparedCommand push(ArgReader args, boolean left) {
        ExecutionRequest request = args.request();
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

    private PreparedCommand lrange(ArgReader args, CommandPreparationContext context) {
        int start = args.intClampedAt(2);
        int stop = args.intClampedAt(3);
        return CommandSupport.sequence(support.commandDb(context).reads().lists()
                .lrange(args.bytes(1), start, stop));
    }

    private PreparedCommand pop(
            ArgReader args,
            CommandPreparationContext context,
            boolean left
    ) {
        boolean hasCount = args.argc() == 3;
        int count = 1;
        if (hasCount) {
            long parsed = args.longAt(2);
            if (parsed < 0L) {
                throw new IllegalArgumentException("value is not an integer or out of range");
            }
            count = parsed > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) parsed;
        }
        PreparedMutation<PoppedValueSequence> mutation = support.commandDb(context).writes().lists()
                .preparePop(args.bytes(1), count, left);
        PoppedValueSequence preview = mutation.preview();
        ReplyShape shape = popShape(preview, hasCount);
        return CommandSupport.preparedMutation(shape, mutation, execution -> {
            if (preview == null || preview.isNull()) {
                if (hasCount) {
                    execution.reply().nullArray();
                } else {
                    execution.reply().nullValue();
                }
                return;
            }
            if (hasCount) {
                execution.reply().arrayHeader(preview.elementCount());
            }
            preview.emitTo(new BulkStringReplyAdapter(execution.reply()));
        });
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

}
