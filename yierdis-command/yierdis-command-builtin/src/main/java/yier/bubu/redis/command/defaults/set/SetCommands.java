package yier.bubu.redis.command.defaults.set;

import java.util.Objects;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CollectionScanCommandSupport;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShapes;

public final class SetCommands implements CommandModule {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public SetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandDefinition<>(syntax("SADD", CommandArity.min(3)),
                CommandParsers.args(), this::sadd));
        registration.register(new CommandDefinition<>(syntax("SREM", CommandArity.min(3)),
                CommandParsers.args(), this::srem));
        registration.register(new CommandDefinition<>(syntax("SMEMBERS", CommandArity.exact(2)),
                CommandParsers.args(), this::smembers));
        registration.register(new CommandDefinition<>(syntax("SISMEMBER", CommandArity.exact(3)),
                CommandParsers.args(), this::sismember));
        registration.register(new CommandDefinition<>(syntax("SCARD", CommandArity.exact(2)),
                CommandParsers.args(), this::scard));
        registration.register(new CommandDefinition<>(syntax("SSCAN", CommandArity.min(3)),
                args -> CollectionScanCommandSupport.parse(args, false), this::sscan));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private PreparedCommand sadd(ArgReader args, CommandPreparationContext context) {
        return changingMembers(args, true);
    }

    private PreparedCommand srem(ArgReader args, CommandPreparationContext context) {
        return changingMembers(args, false);
    }

    private PreparedCommand changingMembers(ArgReader args, boolean add) {
        ExecutionRequest request = args.request();
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            int membersLen = request.argc() - 2;
            support.sliceResetFromRequest(request, 2, membersLen);
            try {
                long changed = add
                        ? support.commandDb(execution).writes().sets()
                                .sadd(request.readOnlyByteArray(1), support.slice()).value()
                        : support.commandDb(execution).writes().sets()
                                .srem(request.readOnlyByteArray(1), support.slice()).value();
                execution.reply().integer(changed);
            } finally {
                support.clearScratch(membersLen);
            }
        });
    }

    private PreparedCommand smembers(ArgReader args, CommandPreparationContext context) {
        return CommandSupport.sequence(support.commandDb(context).reads().sets()
                .smembers(args.bytes(1)));
    }

    private PreparedCommand sismember(ArgReader args, CommandPreparationContext context) {
        long result = support.commandDb(context).reads().sets()
                .sismember(args.bytes(1), args.bytes(2)) ? 1L : 0L;
        return CommandSupport.fixed(ReplyShapes.integer(result), execution -> execution.reply().integer(result));
    }

    private PreparedCommand scard(ArgReader args, CommandPreparationContext context) {
        long count = support.commandDb(context).reads().sets().scard(args.bytes(1));
        return CommandSupport.fixed(ReplyShapes.integer(count), execution -> execution.reply().integer(count));
    }

    private PreparedCommand sscan(
            CollectionScanCommandSupport.Arguments args,
            CommandPreparationContext context
    ) {
        return CollectionScanCommandSupport.prepareReply(support.commandDb(context).reads().sets().sscan(
                args.key(), args.cursor(), args.match(), args.count()
        ));
    }
}
