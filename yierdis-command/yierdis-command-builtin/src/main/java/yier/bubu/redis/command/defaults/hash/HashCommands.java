package yier.bubu.redis.command.defaults.hash;

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

public final class HashCommands implements CommandModule {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public HashCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandDefinition<>(syntax("HSET", CommandArity.pairTail(4, 2)),
                CommandParsers.args(), this::hset));
        registration.register(new CommandDefinition<>(syntax("HGET", CommandArity.exact(3)),
                CommandParsers.request(), this::hget));
        registration.register(new CommandDefinition<>(syntax("HGETALL", CommandArity.exact(2)),
                CommandParsers.request(), this::hgetall));
        registration.register(new CommandDefinition<>(syntax("HLEN", CommandArity.exact(2)),
                CommandParsers.request(), this::hlen));
        registration.register(new CommandDefinition<>(syntax("HDEL", CommandArity.min(3)),
                CommandParsers.request(), this::hdel));
        registration.register(new CommandDefinition<>(syntax("HSCAN", CommandArity.min(3)),
                args -> CollectionScanCommandSupport.parse(args, true), this::hscan));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private PreparedCommand hset(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            int pairsLen = request.argc() - 2;
            support.sliceResetFromRequest(request, 2, pairsLen);
            try {
                long added = support.commandDb(execution).writes().hashes()
                        .hset(request.readOnlyByteArray(1), support.slice()).value();
                execution.reply().integer(added);
            } finally {
                support.clearScratch(pairsLen);
            }
        });
    }

    private PreparedCommand hget(ExecutionRequest request, CommandPreparationContext context) {
        return CommandSupport.byteValue(support.commandDb(context).reads().hashes()
                .hget(request.readOnlyByteArray(1), request.readOnlyByteArray(2)));
    }

    private PreparedCommand hgetall(ExecutionRequest request, CommandPreparationContext context) {
        return CommandSupport.byteMap(support.commandDb(context).reads().hashes()
                .hgetall(request.readOnlyByteArray(1)));
    }

    private PreparedCommand hlen(ExecutionRequest request, CommandPreparationContext context) {
        long length = support.commandDb(context).reads().hashes().hlen(request.readOnlyByteArray(1));
        return CommandSupport.fixed(ReplyShapes.integer(length), execution -> execution.reply().integer(length));
    }

    private PreparedCommand hdel(ExecutionRequest request, CommandPreparationContext context) {
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            int fieldsLen = request.argc() - 2;
            support.sliceResetFromRequest(request, 2, fieldsLen);
            try {
                long deleted = support.commandDb(execution).writes().hashes()
                        .hdel(request.readOnlyByteArray(1), support.slice()).value();
                execution.reply().integer(deleted);
            } finally {
                support.clearScratch(fieldsLen);
            }
        });
    }

    private PreparedCommand hscan(
            CollectionScanCommandSupport.Arguments args,
            CommandPreparationContext context
    ) {
        return CollectionScanCommandSupport.prepareReply(support.commandDb(context).reads().hashes().hscan(
                args.key(), args.cursor(), args.match(), args.count(), args.noValues()
        ));
    }
}
