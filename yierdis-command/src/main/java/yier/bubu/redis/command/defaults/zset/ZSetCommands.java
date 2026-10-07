package yier.bubu.redis.command.defaults.zset;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.String2d;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import java.util.function.Function;
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
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.storage.api.ZAddOptions;
import yier.bubu.redis.storage.api.ZAddOutcome;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteValueSink;

public final class ZSetCommands {
    private static final String SYNTAX_ERROR = "ERR syntax error";
    private static final String SCORE_ERROR = "ERR value is not a valid float";
    private static final String SCORE_BOUND_ERROR = "ERR min or max is not a float";
    private static final String ZADD_NX_XX_ERROR = "ERR XX and NX options at the same time are not compatible";
    private static final String ZADD_GT_LT_NX_ERROR =
            "ERR GT, LT, and/or NX options at the same time are not compatible";
    private static final String ZADD_INCR_SINGLE_PAIR_ERROR =
            "ERR INCR option supports a single increment-element pair";
    // INCR 回复是按 Redis 双精度格式渲染的分数："inf"/long/Double.toString 最长 24 字节，预留取整上界。
    private static final int INCR_SCORE_REPLY_MAX_BYTES = 32;
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public ZSetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("ZADD", CommandArity.min(4)), this::zadd));
        registration.register(new CommandSpec(syntax("ZRANGE", CommandArity.range(4, 6)), this::zrange));
        registration.register(new CommandSpec(syntax("ZREVRANGE", CommandArity.oneOf(4, 5)), this::zrevrange));
        registration.register(new CommandSpec(syntax("ZRANGEBYSCORE", CommandArity.min(4)),
                args -> zrangeByScore(args, RangeDirection.FORWARD)));
        registration.register(new CommandSpec(syntax("ZREVRANGEBYSCORE", CommandArity.min(4)),
                args -> zrangeByScore(args, RangeDirection.REVERSE)));
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

    // flag 解析与 Redis zaddGenericCommand 同序：先数对（syntax error），再组合错误，最后校验分数，
    // 这样非法组合在 MULTI preflight 阶段就 EXECABORT，且错误文案与 Redis 一致。
    private Function<CommandSession, PreparedCommand> zadd(CommandArgs args) {
        int index = 2;
        boolean nx = false;
        boolean xx = false;
        boolean gt = false;
        boolean lt = false;
        boolean ch = false;
        boolean incr = false;
        while (index < args.argc()) {
            if (args.is(index, "NX")) {
                nx = true;
            } else if (args.is(index, "XX")) {
                xx = true;
            } else if (args.is(index, "GT")) {
                gt = true;
            } else if (args.is(index, "LT")) {
                lt = true;
            } else if (args.is(index, "CH")) {
                ch = true;
            } else if (args.is(index, "INCR")) {
                incr = true;
            } else {
                break;
            }
            index++;
        }
        int elements = args.argc() - index;
        if (elements == 0 || (elements & 1) != 0) {
            throw syntaxFailure();
        }
        if (nx && xx) {
            throw new CommandParseException(ZADD_NX_XX_ERROR);
        }
        if (((gt || lt) && nx) || (gt && lt)) {
            throw new CommandParseException(ZADD_GT_LT_NX_ERROR);
        }
        if (incr && elements > 2) {
            throw new CommandParseException(ZADD_INCR_SINGLE_PAIR_ERROR);
        }
        for (int pair = index; pair < args.argc(); pair += 2) {
            parseScore(args.bytes(pair));
        }
        byte[] key = args.bytes(1);
        List<byte[]> pairs = args.byteArraysFrom(index);
        ZAddOptions options = new ZAddOptions(nx, xx, gt, lt, incr);
        boolean incrMode = incr;
        boolean changedReply = ch;
        ReplyShape reservationShape = incrMode
                ? ReplyShapes.bulkString(INCR_SCORE_REPLY_MAX_BYTES, 0L)
                : ReplyShapes.integerUpperBound();
        return session -> CommandSupport.preparedAction(reservationShape, execution -> {
            ZAddOutcome outcome = support.commandDb(execution).zsets().zadd(key, pairs, options).value();
            if (incrMode) {
                byte[] newScore = outcome.newScore();
                return CommandResult.reply(newScore == null
                        ? RedisReplies.nullValue()
                        : RedisReplies.bulkString(newScore));
            }
            return CommandResult.reply(RedisReplies.integer(changedReply ? outcome.changed() : outcome.added()));
        });
    }

    private Function<CommandSession, PreparedCommand> zrange(CommandArgs args) {
        long start = args.longAt(2);
        long stop = args.longAt(3);
        boolean withScores = false;
        RangeDirection direction = RangeDirection.FORWARD;
        for (int index = 4; index < args.argc(); index++) {
            if (args.is(index, "WITHSCORES")) {
                if (withScores) {
                    throw syntaxFailure();
                }
                withScores = true;
            } else if (args.is(index, "REV")) {
                if (direction == RangeDirection.REVERSE) {
                    throw syntaxFailure();
                }
                direction = RangeDirection.REVERSE;
            } else {
                throw syntaxFailure();
            }
        }
        ZRangeArgs parsed = new ZRangeArgs(args.bytes(1), start, stop, withScores, direction);
        return session -> prepareRange(parsed, session);
    }

    private Function<CommandSession, PreparedCommand> zrevrange(CommandArgs args) {
        long start = args.longAt(2);
        long stop = args.longAt(3);
        boolean withScores = args.argc() == 5;
        if (withScores && !args.is(4, "WITHSCORES")) {
            throw syntaxFailure();
        }
        ZRangeArgs parsed = new ZRangeArgs(
                args.bytes(1), start, stop, withScores, RangeDirection.REVERSE);
        return session -> prepareRange(parsed, session);
    }

    private PreparedCommand prepareRange(
            ZRangeArgs args,
            yier.bubu.redis.execution.api.CommandSession session
    ) {
        return scoreReply(session, args.withScores(), () -> args.direction() == RangeDirection.REVERSE
                ? support.commandDb(session).zsets()
                        .zrevrange(args.key(), args.start(), args.stop(), args.withScores())
                : support.commandDb(session).zsets()
                        .zrange(args.key(), args.start(), args.stop(), args.withScores()));
    }

    private Function<CommandSession, PreparedCommand> zrangeByScore(
            CommandArgs args,
            RangeDirection direction
    ) {
        ScoreBound first = parseScoreBound(args.bytes(2));
        ScoreBound second = parseScoreBound(args.bytes(3));
        ScoreBound min = direction == RangeDirection.REVERSE ? second : first;
        ScoreBound max = direction == RangeDirection.REVERSE ? first : second;
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
                offset = args.longAt(index + 1);
                long requestedCount = args.longAt(index + 2);
                count = requestedCount < 0L ? Long.MAX_VALUE : requestedCount;
                index += 3;
                continue;
            }
            throw syntaxFailure();
        }

        ZRangeByScoreArgs parsed = new ZRangeByScoreArgs(
                args.bytes(1), min, max, withScores, offset, count, direction);
        return session -> prepareRangeByScore(parsed, session);
    }

    private PreparedCommand prepareRangeByScore(
            ZRangeByScoreArgs args,
            yier.bubu.redis.execution.api.CommandSession session
    ) {
        return scoreReply(session, args.withScores(), () -> args.direction() == RangeDirection.REVERSE
                ? support.commandDb(session).zsets().zrevrangeByScore(
                        args.key(), args.min().value(), args.min().exclusive(),
                        args.max().value(), args.max().exclusive(),
                        args.withScores(), args.offset(), args.count())
                : support.commandDb(session).zsets().zrangeByScore(
                        args.key(), args.min().value(), args.min().exclusive(),
                        args.max().value(), args.max().exclusive(),
                        args.withScores(), args.offset(), args.count()));
    }

    private Function<CommandSession, PreparedCommand> zremrangebyscore(CommandArgs args) {
        ScoreRemovalArgs parsed = new ScoreRemovalArgs(
                args.bytes(1), parseScoreBound(args.bytes(2)), parseScoreBound(args.bytes(3)));
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long removed = support.commandDb(execution).zsets().zremrangeByScore(
                    parsed.key(), parsed.min().value(), parsed.min().exclusive(),
                    parsed.max().value(), parsed.max().exclusive()).value();
            return CommandResult.reply(RedisReplies.integer(removed));
        });
    }

    private Function<CommandSession, PreparedCommand> zremrangebyrank(CommandArgs args) {
        RankRemovalArgs parsed = new RankRemovalArgs(args.bytes(1), args.longAt(2), args.longAt(3));
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long removed = support.commandDb(execution).zsets()
                    .zremrangeByRank(parsed.key(), parsed.start(), parsed.stop()).value();
            return CommandResult.reply(RedisReplies.integer(removed));
        });
    }

    private Function<CommandSession, PreparedCommand> zrem(CommandArgs args) {
        byte[] key = args.bytes(1);
        List<byte[]> members = args.byteArraysFrom(2);
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long removed = support.commandDb(execution).zsets().zrem(key, members).value();
            return CommandResult.reply(RedisReplies.integer(removed));
        });
    }

    private Function<CommandSession, PreparedCommand> zscan(CommandArgs args) {
        CollectionScanCommandSupport.Arguments parsed = CollectionScanCommandSupport.parse(args);
        return session -> CollectionScanCommandSupport.prepareReply(
                support.commandDb(session).zsets().zscan(
                        parsed.key(), parsed.cursor(), parsed.match(), parsed.count()));
    }

    private PreparedCommand scoreReply(
            CommandSession session,
            boolean withScores,
            Supplier<ByteSequenceSource> read
    ) {
        ByteSequenceSource source;
        try {
            source = read.get();
        } catch (IllegalArgumentException failure) {
            return PreparedCommands.ready(RedisReplies.error("ERR " + failure.getMessage()));
        }
        // RESP3 的 WITHSCORES 是 [member, score] 二元数组。分数仍是 bulk string，没有 RESP3 double。
        if (withScores && session.respVersion() >= 3) {
            return ownedNestedScores(source);
        }
        return ownedSequence(source);
    }

    private static PreparedCommand ownedNestedScores(ByteSequenceSource source) {
        ArrayList<byte[]> flat = new ArrayList<>();
        source.emitTo(new ByteValueSink() {
            @Override
            public void value(byte[] data) {
                flat.add(data == null ? new byte[0] : data.clone());
            }

            @Override
            public void value(byte[] data, int offset, int length) {
                byte[] copy = new byte[length];
                if (data != null && length > 0) {
                    System.arraycopy(data, offset, copy, 0, length);
                }
                flat.add(copy);
            }

            @Override
            public void value(BytesSlice slice) {
                byte[] copy = new byte[slice.length()];
                slice.getBytes(0, copy, 0, copy.length);
                flat.add(copy);
            }

            @Override
            public void longAscii(long value) {
                flat.add(Long.toString(value).getBytes(StandardCharsets.US_ASCII));
            }

            @Override
            public void nullValue() {
                flat.add(new byte[0]);
            }
        });
        if ((flat.size() & 1) != 0) {
            throw new IllegalStateException("WITHSCORES sequence must contain member/score pairs");
        }
        ArrayList<RedisReply> pairs = new ArrayList<>(flat.size() / 2);
        for (int index = 0; index < flat.size(); index += 2) {
            pairs.add(RedisReplies.array(List.of(
                    RedisReplies.bulkString(flat.get(index)),
                    RedisReplies.bulkString(flat.get(index + 1))
            )));
        }
        return PreparedCommands.owned(CommandResult.reply(RedisReplies.array(pairs)), source);
    }

    private static PreparedCommand ownedSequence(ByteSequenceSource source) {
        RedisReply reply = DbReplies.sequence(source);
        return PreparedCommands.owned(CommandResult.reply(reply), source);
    }

    private static double parseScore(byte[] raw) {
        try {
            return String2d.parse(raw);
        } catch (NumberFormatException failure) {
            throw new CommandParseException(SCORE_ERROR);
        }
    }

    private static ScoreBound parseScoreBound(byte[] raw) {
        if (raw == null || raw.length == 0) {
            throw new CommandParseException(SCORE_BOUND_ERROR);
        }

        int start = 0;
        boolean exclusive = false;
        // 只有 '(' 是合法前缀（开区间）；'[' 属于 lex range 语法，Redis 在 score range 上拒绝它，
        // 这里让它落入下方的 Double.parseDouble 失败路径，报 "min or max is not a float"。
        if (raw[0] == '(') {
            exclusive = true;
            start = 1;
        }
        if (start >= raw.length) {
            throw new CommandParseException(SCORE_BOUND_ERROR);
        }

        String value = new String(raw, start, raw.length - start, StandardCharsets.US_ASCII);
        // Redis 8.0.2 的 zslParseRange 不剥空白，也不接受十六进制。8.10.2 这两点不同，这里不跟。
        // inf 以 f 结尾，必须先于 Java 浮点后缀判断，否则 +inf 会被当成 后缀错误。
        if (hasBoundWhitespace(value) || isHexBound(value)) {
            throw new CommandParseException(SCORE_BOUND_ERROR);
        }
        if (isNegativeInfinitySpelling(value)) {
            return new ScoreBound(Double.NEGATIVE_INFINITY, exclusive);
        }
        if (isPositiveInfinitySpelling(value)) {
            return new ScoreBound(Double.POSITIVE_INFINITY, exclusive);
        }
        if (hasJavaFloatSuffix(value)) {
            throw new CommandParseException(SCORE_BOUND_ERROR);
        }
        double parsed;
        try {
            parsed = Double.parseDouble(value);
        } catch (NumberFormatException failure) {
            throw new CommandParseException(SCORE_BOUND_ERROR);
        }
        if (Double.isNaN(parsed)) {
            throw new CommandParseException(SCORE_BOUND_ERROR);
        }
        // 1e309 在边界上是 ±inf，不像 ZADD 分数那样因上溢拒绝。
        return new ScoreBound(parsed, exclusive);
    }

    private static boolean hasBoundWhitespace(String value) {
        if (value.isEmpty()) {
            return true;
        }
        return isBoundSpace(value.charAt(0)) || isBoundSpace(value.charAt(value.length() - 1));
    }

    private static boolean isBoundSpace(char value) {
        return value == ' ' || value == '\t' || value == '\n' || value == '\r' || value == '\f' || value == 0x0b;
    }

    private static boolean isHexBound(String value) {
        int index = (value.charAt(0) == '+' || value.charAt(0) == '-') ? 1 : 0;
        return value.length() >= index + 2
                && value.charAt(index) == '0'
                && (value.charAt(index + 1) == 'x' || value.charAt(index + 1) == 'X');
    }

    private static boolean hasJavaFloatSuffix(String value) {
        char last = value.charAt(value.length() - 1);
        return last == 'd' || last == 'D' || last == 'f' || last == 'F';
    }

    private static boolean isNegativeInfinitySpelling(String value) {
        String body = infinityBody(value);
        return body != null && value.charAt(0) == '-' && (body.equals("inf") || body.equals("infinity"));
    }

    private static boolean isPositiveInfinitySpelling(String value) {
        String body = infinityBody(value);
        return body != null && value.charAt(0) != '-' && (body.equals("inf") || body.equals("infinity"));
    }

    private static String infinityBody(String value) {
        String lowered = value.toLowerCase(java.util.Locale.ROOT);
        if (lowered.startsWith("+") || lowered.startsWith("-")) {
            lowered = lowered.substring(1);
        }
        if (lowered.equals("inf") || lowered.equals("infinity")) {
            return lowered;
        }
        return null;
    }

    private static CommandParseException syntaxFailure() {
        return new CommandParseException(SYNTAX_ERROR);
    }

    private enum RangeDirection {
        FORWARD,
        REVERSE
    }

    private record ZRangeArgs(
            byte[] key,
            long start,
            long stop,
            boolean withScores,
            RangeDirection direction
    ) {
    }

    private record ZRangeByScoreArgs(
            byte[] key,
            ScoreBound min,
            ScoreBound max,
            boolean withScores,
            long offset,
            long count,
            RangeDirection direction
    ) {
    }

    private record ScoreRemovalArgs(byte[] key, ScoreBound min, ScoreBound max) {
    }

    private record RankRemovalArgs(byte[] key, long start, long stop) {
    }

    private record ScoreBound(double value, boolean exclusive) {
    }
}
