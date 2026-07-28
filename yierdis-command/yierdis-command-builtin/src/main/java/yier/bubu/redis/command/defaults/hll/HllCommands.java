package yier.bubu.redis.command.defaults.hll;

import java.util.Objects;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShapes;

public final class HllCommands implements CommandModule {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);
    private static final CommandKeySpec MULTI_KEYS = new CommandKeySpec(1, -1, 1);

    private final CommandSupport support;

    public HllCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandDefinition<>(syntax("PFADD", CommandArity.min(3), KEY),
                CommandParsers.args(), this::pfadd));
        registration.register(new CommandDefinition<>(syntax("PFCOUNT", CommandArity.min(2), MULTI_KEYS),
                CommandParsers.args(), this::pfcount));
        registration.register(new CommandDefinition<>(syntax("PFMERGE", CommandArity.min(3), MULTI_KEYS),
                CommandParsers.args(), this::pfmerge));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity, CommandKeySpec keys) {
        return new CommandSyntax(nameUpper, arity, keys, TransactionPolicy.QUEUEABLE);
    }

    private PreparedCommand pfadd(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            int elementsLen = request.argc() - 2;
            support.sliceResetFromRequest(request, 2, elementsLen);
            try {
                long changed = support.commandDb(execution).writes().hll()
                        .pfadd(request.readOnlyByteArray(1), support.slice())
                        .value();
                execution.reply().integer(changed);
            } finally {
                support.clearScratch(elementsLen);
            }
        });
    }

    private PreparedCommand pfcount(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        int len = request.argc() - 1;
        long count;
        support.sliceResetFromRequest(request, 1, len);
        try {
            count = support.commandDb(context).reads().hll().pfcount(support.slice());
        } finally {
            support.clearScratch(len);
        }
        return CommandSupport.fixed(ReplyShapes.integer(count), execution -> execution.reply().integer(count));
    }

    private PreparedCommand pfmerge(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        return CommandSupport.fixed(ReplyShapes.simpleString("OK"), execution -> {
            int sourcesLen = request.argc() - 2;
            support.sliceResetFromRequest(request, 2, sourcesLen);
            try {
                support.commandDb(execution).writes().hll()
                        .pfmerge(request.readOnlyByteArray(1), support.slice());
                execution.reply().simpleString("OK");
            } finally {
                support.clearScratch(sourcesLen);
            }
        });
    }
}
