package yier.bubu.redis.command.defaults.zset;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandInvocation;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CollectionScanCommandSupport;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.command.defaults.DbReplies;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;

public final class ZSetCommands implements CommandModule {
    private static final String SYNTAX_ERROR = "ERR syntax error";
    private static final String SCORE_ERROR = "ERR value is not a valid float";
    private static final String SCORE_BOUND_ERROR = "ERR min or max is not a float";
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public ZSetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("ZADD", CommandArity.pairTail(4, 2)), this::zadd));
        registration.register(new CommandSpec(syntax("ZRANGE", CommandArity.range(4, 6)), this::zrange));
        registration.register(new CommandSpec(syntax("ZREVRANGE", CommandArity.oneOf(4, 5)), this::zrevrange));
        registration.register(new CommandSpec(syntax("ZRANGEBYSCORE", CommandArity.min(4)),
                args -> zrangeByScore(args, false)));
        registration.register(new CommandSpec(syntax("ZREVRANGEBYSCORE", CommandArity.min(4)),
                args -> zrangeByScore(args, true)));
        registration.register(new CommandSpec(syntax("ZREMRANGEBYSCORE", CommandArity.exact(4)),
                this::zremrangebyscore));
        registration.register(new CommandSpec(syntax("ZREMRANGEBYRANK", CommandArity.exact(4)),
                this::zremrangebyrank));
        registration.register(new CommandSpec(syntax("ZREM", CommandArity.min(3)), this::zrem));
        registration.register(new CommandSpec(syntax("ZSCAN", CommandArity.min(3)), this::zscan));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private CommandInvocation zadd(CommandArgs args) throws CommandParseException {
        for (int index = 2; index < args.argc(); index += 2) {
            parseScore(args.bytes(index));
        }
        byte[] key = args.bytes(1);
        List<byte[]> pairs = args.byteArraysFrom(2);
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long added = support.commandDb(execution).writes().zsets().zadd(key, pairs).value();
            return CommandResult.reply(RedisReplies.integer(added));
        });
    }

    private CommandInvocation zrange(CommandArgs args) throws CommandParseException {
        long start = args.longAt(2);
        long stop = args.longAt(3);
        boolean withScores = false;
        boolean reverse = false;
        for (int index = 4; index < args.argc(); index++) {
            if (args.is(index, "WITHSCORES")) {
                if (withScores) {
                    throw syntaxFailure();
                }
                withScores = true;
            } else if (args.is(index, "REV")) {
                if (reverse) {
                    throw syntaxFailure();
                }
                reverse = true;
            } else {
                throw syntaxFailure();
            }
        }
        ZRangeArgs parsed = new ZRangeArgs(args.bytes(1), start, stop, withScores, reverse);
        return session -> prepareRange(parsed, session);
    }

    private CommandInvocation zrevrange(CommandArgs args) throws CommandParseException {
        long start = args.longAt(2);
        long stop = args.longAt(3);
        boolean withScores = args.argc() == 5;
        if (withScores && !args.is(4, "WITHSCORES")) {
            throw syntaxFailure();
        }
        ZRangeArgs parsed = new ZRangeArgs(
                args.bytes(1), start, stop, withScores, true);
        return session -> prepareRange(parsed, session);
    }

    private PreparedCommand prepareRange(
            ZRangeArgs args,
            yier.bubu.redis.execution.api.CommandSession session
    ) {
        return prepareSequence(() -> args.reverse()
                ? support.commandDb(session).reads().zsets()
                        .zrevrange(args.key(), args.start(), args.stop(), args.withScores())
                : support.commandDb(session).reads().zsets()
                        .zrange(args.key(), args.start(), args.stop(), args.withScores()));
    }

    private CommandInvocation zrangeByScore(CommandArgs args, boolean reverse)
            throws CommandParseException {
        ScoreBound first = parseScoreBound(args.bytes(2));
        ScoreBound second = parseScoreBound(args.bytes(3));
        ScoreBound min = reverse ? second : first;
        ScoreBound max = reverse ? first : second;
        boolean withScores = false;
        long offset = 0L;
        long count = Long.MAX_VALUE;

        int index = 4;
        while (index < args.argc()) {
            if (args.is(index, "WITHSCORES")) {
                if (withScores) {
                    throw syntaxFailure();
                }
                withScores = true;
                index++;
                continue;
            }
            if (args.is(index, "LIMIT")) {
                if (index + 2 >= args.argc()) {
                    throw syntaxFailure();
                }
                offset = args.nonNegativeLongAt(index + 1);
                long requestedCount = args.longAt(index + 2);
                count = requestedCount < 0L ? Long.MAX_VALUE : requestedCount;
                index += 3;
                continue;
            }
            throw syntaxFailure();
        }

        ZRangeByScoreArgs parsed = new ZRangeByScoreArgs(
                args.bytes(1), min, max, withScores, offset, count, reverse);
        return session -> prepareRangeByScore(parsed, session);
    }

    private PreparedCommand prepareRangeByScore(
            ZRangeByScoreArgs args,
            yier.bubu.redis.execution.api.CommandSession session
    ) {
        return prepareSequence(() -> args.reverse()
                ? support.commandDb(session).reads().zsets().zrevrangeByScore(
                        args.key(), args.min().value(), args.min().exclusive(),
                        args.max().value(), args.max().exclusive(),
                        args.withScores(), args.offset(), args.count())
                : support.commandDb(session).reads().zsets().zrangeByScore(
                        args.key(), args.min().value(), args.min().exclusive(),
                        args.max().value(), args.max().exclusive(),
                        args.withScores(), args.offset(), args.count()));
    }

    private CommandInvocation zremrangebyscore(CommandArgs args) throws CommandParseException {
        ScoreRemovalArgs parsed = new ScoreRemovalArgs(
                args.bytes(1), parseScoreBound(args.bytes(2)), parseScoreBound(args.bytes(3)));
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long removed = support.commandDb(execution).writes().zsets().zremrangeByScore(
                    parsed.key(), parsed.min().value(), parsed.min().exclusive(),
                    parsed.max().value(), parsed.max().exclusive()).value();
            return CommandResult.reply(RedisReplies.integer(removed));
        });
    }

    private CommandInvocation zremrangebyrank(CommandArgs args) throws CommandParseException {
        RankRemovalArgs parsed = new RankRemovalArgs(args.bytes(1), args.longAt(2), args.longAt(3));
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long removed = support.commandDb(execution).writes().zsets()
                    .zremrangeByRank(parsed.key(), parsed.start(), parsed.stop()).value();
            return CommandResult.reply(RedisReplies.integer(removed));
        });
    }

    private CommandInvocation zrem(CommandArgs args) {
        byte[] key = args.bytes(1);
        List<byte[]> members = args.byteArraysFrom(2);
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long removed = support.commandDb(execution).writes().zsets().zrem(key, members).value();
            return CommandResult.reply(RedisReplies.integer(removed));
        });
    }

    private CommandInvocation zscan(CommandArgs args) throws CommandParseException {
        CollectionScanCommandSupport.Arguments parsed = CollectionScanCommandSupport.parse(args, false);
        return session -> CollectionScanCommandSupport.prepareReply(
                support.commandDb(session).reads().zsets().zscan(
                        parsed.key(), parsed.cursor(), parsed.match(), parsed.count()));
    }

    private static PreparedCommand ownedSequence(ByteSequenceSource source) {
        RedisReply reply = DbReplies.sequence(source);
        return PreparedCommands.owned(CommandResult.reply(reply), source);
    }

    private static PreparedCommand prepareSequence(Supplier<ByteSequenceSource> read) {
        ByteSequenceSource source;
        try {
            source = read.get();
        } catch (IllegalArgumentException failure) {
            return PreparedCommands.ready(RedisReplies.error("ERR " + failure.getMessage()));
        }
        return ownedSequence(source);
    }

    private static double parseScore(byte[] raw) throws CommandParseException {
        if (raw == null) {
            throw new CommandParseException(SCORE_ERROR);
        }
        double value;
        try {
            value = Double.parseDouble(new String(raw, StandardCharsets.US_ASCII));
        } catch (NumberFormatException failure) {
            throw new CommandParseException(SCORE_ERROR);
        }
        if (!Double.isFinite(value)) {
            throw new CommandParseException(SCORE_ERROR);
        }
        return value;
    }

    private static ScoreBound parseScoreBound(byte[] raw) throws CommandParseException {
        if (raw == null || raw.length == 0) {
            throw new CommandParseException(SCORE_BOUND_ERROR);
        }

        int start = 0;
        boolean exclusive = false;
        if (raw[0] == '(') {
            exclusive = true;
            start = 1;
        } else if (raw[0] == '[') {
            start = 1;
        }
        if (start >= raw.length) {
            throw new CommandParseException(SCORE_BOUND_ERROR);
        }

        String value = new String(raw, start, raw.length - start, StandardCharsets.US_ASCII);
        if ("-inf".equalsIgnoreCase(value)) {
            return new ScoreBound(Double.NEGATIVE_INFINITY, exclusive);
        }
        if ("+inf".equalsIgnoreCase(value) || "inf".equalsIgnoreCase(value)) {
            return new ScoreBound(Double.POSITIVE_INFINITY, exclusive);
        }
        double parsed;
        try {
            parsed = Double.parseDouble(value);
        } catch (NumberFormatException failure) {
            throw new CommandParseException(SCORE_BOUND_ERROR);
        }
        if (!Double.isFinite(parsed)) {
            throw new CommandParseException(SCORE_BOUND_ERROR);
        }
        return new ScoreBound(parsed, exclusive);
    }

    private static CommandParseException syntaxFailure() {
        return new CommandParseException(SYNTAX_ERROR);
    }

    private record ZRangeArgs(byte[] key, long start, long stop, boolean withScores, boolean reverse) {
    }

    private record ZRangeByScoreArgs(
            byte[] key,
            ScoreBound min,
            ScoreBound max,
            boolean withScores,
            long offset,
            long count,
            boolean reverse
    ) {
    }

    private record ScoreRemovalArgs(byte[] key, ScoreBound min, ScoreBound max) {
    }

    private record RankRemovalArgs(byte[] key, long start, long stop) {
    }

    private record ScoreBound(double value, boolean exclusive) {
    }
}
