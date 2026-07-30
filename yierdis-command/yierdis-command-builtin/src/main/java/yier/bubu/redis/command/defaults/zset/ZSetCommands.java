package yier.bubu.redis.command.defaults.zset;

import java.util.Objects;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CollectionScanCommandSupport;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;

public final class ZSetCommands implements CommandModule {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public ZSetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandDefinition<>(syntax("ZADD", CommandArity.pairTail(4, 2)),
                CommandParsers.args(), this::zadd));
        registration.register(new CommandDefinition<>(syntax("ZRANGE", CommandArity.range(4, 6)),
                this::parseZRange, this::zrange));
        registration.register(new CommandDefinition<>(syntax("ZREVRANGE", CommandArity.oneOf(4, 5)),
                this::parseZRevRange, this::zrange));
        registration.register(new CommandDefinition<>(syntax("ZRANGEBYSCORE", CommandArity.min(4)),
                args -> parseZRangeByScore(args, false), this::zrangebyscore));
        registration.register(new CommandDefinition<>(syntax("ZREVRANGEBYSCORE", CommandArity.min(4)),
                args -> parseZRangeByScore(args, true), this::zrevrangebyscore));
        registration.register(new CommandDefinition<>(syntax("ZREMRANGEBYSCORE", CommandArity.exact(4)),
                this::parseZRemRangeByScore, this::zremrangebyscore));
        registration.register(new CommandDefinition<>(syntax("ZREMRANGEBYRANK", CommandArity.exact(4)),
                this::parseZRemRangeByRank, this::zremrangebyrank));
        registration.register(new CommandDefinition<>(syntax("ZREM", CommandArity.min(3)),
                CommandParsers.args(), this::zrem));
        registration.register(new CommandDefinition<>(syntax("ZSCAN", CommandArity.min(3)),
                this::parseZScan, this::zscan));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private CommandParseResult<CollectionScanCommandSupport.Arguments> parseZScan(ArgReader args) {
        try {
            return CommandParseResult.ok(CollectionScanCommandSupport.parse(
                    CommandArgs.of(args.request()), false));
        } catch (CommandParseException failure) {
            return CommandParseResult.error(CommandParseError.custom(failure.replyMessage()));
        }
    }

    private PreparedCommand zadd(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            int pairsLen = request.argc() - 2;
            support.sliceResetFromRequest(request, 2, pairsLen);
            try {
                long added = support.commandDb(execution).writes().zsets()
                        .zadd(request.readOnlyByteArray(1), support.slice())
                        .value();
                execution.reply().integer(added);
            } finally {
                support.clearScratch(pairsLen);
            }
        });
    }

    private record ZRangeArgs(byte[] key, long start, long stop, boolean withScores, boolean rev) {
    }

    private CommandParseResult<ZRangeArgs> parseZRange(ArgReader args) {
        long start;
        long stop;
        try {
            start = args.longAt(2);
            stop = args.longAt(3);
        } catch (IllegalArgumentException e) {
            return CommandParseResult.error(CommandParseError.integerOutOfRange());
        }
        boolean withScores = false;
        boolean rev = false;
        for (int i = 4; i < args.argc(); i++) {
            if (args.is(i, "WITHSCORES")) {
                if (withScores) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                withScores = true;
                continue;
            }
            if (args.is(i, "REV")) {
                if (rev) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                rev = true;
                continue;
            }
            return CommandParseResult.error(CommandParseError.syntax());
        }
        return CommandParseResult.ok(new ZRangeArgs(args.bytes(1), start, stop, withScores, rev));
    }

    private CommandParseResult<ZRangeArgs> parseZRevRange(ArgReader args) {
        long start;
        long stop;
        try {
            start = args.longAt(2);
            stop = args.longAt(3);
        } catch (IllegalArgumentException e) {
            return CommandParseResult.error(CommandParseError.integerOutOfRange());
        }
        boolean withScores = args.argc() == 5;
        if (withScores && !args.is(4, "WITHSCORES")) {
            return CommandParseResult.error(CommandParseError.syntax());
        }
        return CommandParseResult.ok(new ZRangeArgs(args.bytes(1), start, stop, withScores, true));
    }

    private PreparedCommand zrange(ZRangeArgs args, CommandPreparationContext context) {
        ByteSequenceSource sequence = args.rev()
                ? support.commandDb(context).reads().zsets()
                        .zrevrange(args.key(), args.start(), args.stop(), args.withScores())
                : support.commandDb(context).reads().zsets()
                        .zrange(args.key(), args.start(), args.stop(), args.withScores());
        return CommandSupport.sequence(sequence);
    }

    private record ZRangeByScoreArgs(
            byte[] key,
            CommandSupport.ScoreBound min,
            CommandSupport.ScoreBound max,
            boolean withScores,
            long offset,
            long count
    ) {
    }

    private CommandParseResult<ZRangeByScoreArgs> parseZRangeByScore(ArgReader args, boolean reverse) {
        CommandSupport.ScoreBound first;
        CommandSupport.ScoreBound second;
        try {
            first = CommandSupport.parseScoreBound(args.bytes(2));
            second = CommandSupport.parseScoreBound(args.bytes(3));
        } catch (YierdisCommandException e) {
            return CommandParseResult.error(CommandParseError.custom(e.getMessage()));
        }
        CommandSupport.ScoreBound min = reverse ? second : first;
        CommandSupport.ScoreBound max = reverse ? first : second;
        boolean withScores = false;
        long offset = 0L;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < args.argc()) {
            if (args.is(i, "WITHSCORES")) {
                if (withScores) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                withScores = true;
                i++;
                continue;
            }
            if (args.is(i, "LIMIT")) {
                if (i + 2 >= args.argc()) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                try {
                    offset = args.nonNegativeLongAt(i + 1);
                    long requestedCount = args.longAt(i + 2);
                    count = requestedCount < 0L ? Long.MAX_VALUE : requestedCount;
                } catch (IllegalArgumentException e) {
                    return CommandParseResult.error(CommandParseError.integerOutOfRange());
                }
                i += 3;
                continue;
            }
            return CommandParseResult.error(CommandParseError.syntax());
        }
        return CommandParseResult.ok(new ZRangeByScoreArgs(args.bytes(1), min, max, withScores, offset, count));
    }

    private PreparedCommand zrangebyscore(ZRangeByScoreArgs args, CommandPreparationContext context) {
        ByteSequenceSource sequence = support.commandDb(context).reads().zsets().zrangeByScore(
                args.key(),
                args.min().value,
                args.min().exclusive,
                args.max().value,
                args.max().exclusive,
                args.withScores(),
                args.offset(),
                args.count()
        );
        return CommandSupport.sequence(sequence);
    }

    private PreparedCommand zrevrangebyscore(ZRangeByScoreArgs args, CommandPreparationContext context) {
        ByteSequenceSource sequence = support.commandDb(context).reads().zsets().zrevrangeByScore(
                args.key(),
                args.min().value,
                args.min().exclusive,
                args.max().value,
                args.max().exclusive,
                args.withScores(),
                args.offset(),
                args.count()
        );
        return CommandSupport.sequence(sequence);
    }

    private record ZScoreRemovalArgs(
            byte[] key,
            CommandSupport.ScoreBound min,
            CommandSupport.ScoreBound max
    ) {
    }

    private CommandParseResult<ZScoreRemovalArgs> parseZRemRangeByScore(ArgReader args) {
        try {
            return CommandParseResult.ok(new ZScoreRemovalArgs(
                    args.bytes(1),
                    CommandSupport.parseScoreBound(args.bytes(2)),
                    CommandSupport.parseScoreBound(args.bytes(3))
            ));
        } catch (YierdisCommandException e) {
            return CommandParseResult.error(CommandParseError.custom(e.getMessage()));
        }
    }

    private PreparedCommand zremrangebyscore(ZScoreRemovalArgs args, CommandPreparationContext context) {
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            long removed = support.commandDb(execution).writes().zsets().zremrangeByScore(
                    args.key(),
                    args.min().value,
                    args.min().exclusive,
                    args.max().value,
                    args.max().exclusive
            ).value();
            execution.reply().integer(removed);
        });
    }

    private record ZRankRemovalArgs(byte[] key, long start, long stop) {
    }

    private CommandParseResult<ZRankRemovalArgs> parseZRemRangeByRank(ArgReader args) {
        try {
            return CommandParseResult.ok(new ZRankRemovalArgs(
                    args.bytes(1),
                    args.longAt(2),
                    args.longAt(3)
            ));
        } catch (IllegalArgumentException e) {
            return CommandParseResult.error(CommandParseError.integerOutOfRange());
        }
    }

    private PreparedCommand zremrangebyrank(ZRankRemovalArgs args, CommandPreparationContext context) {
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            long removed = support.commandDb(execution).writes().zsets()
                    .zremrangeByRank(args.key(), args.start(), args.stop())
                    .value();
            execution.reply().integer(removed);
        });
    }

    private PreparedCommand zrem(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            int membersLen = request.argc() - 2;
            support.sliceResetFromRequest(request, 2, membersLen);
            try {
                long removed = support.commandDb(execution).writes().zsets()
                        .zrem(request.readOnlyByteArray(1), support.slice())
                        .value();
                execution.reply().integer(removed);
            } finally {
                support.clearScratch(membersLen);
            }
        });
    }

    private PreparedCommand zscan(
            CollectionScanCommandSupport.Arguments args,
            CommandPreparationContext context
    ) {
        return CollectionScanCommandSupport.prepareReply(support.commandDb(context).reads().zsets().zscan(
                args.key(),
                args.cursor(),
                args.match(),
                args.count()
        ));
    }
}
