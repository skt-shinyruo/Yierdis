package yier.bubu.redis.command.defaults.zset;

import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.defaults.BulkStringReplyAdapter;
import yier.bubu.redis.command.defaults.CommandSupport;

import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.util.Objects;

public final class ZSetCommands implements CommandModule {
    private final CommandSupport support;

    public ZSetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(
                "ZADD",
                CommandDescriptor.of(-4, 1, 1, 1),
                CommandParsers.pairTail(4, 2, "zadd"),
                this::zadd
        );
        registration.register("ZRANGE", CommandDescriptor.of(-4, 1, 1, 1), this::parseZRange, this::zrange);
        registration.register("ZREVRANGE", CommandDescriptor.of(-4, 1, 1, 1), CommandParsers.oneOfRequest("zrevrange", 4, 5), this::zrevrange);
        registration.register(
                "ZRANGEBYSCORE",
                CommandDescriptor.of(-4, 1, 1, 1),
                args -> parseZRangeByScore(args, false),
                this::zrangebyscore
        );
        registration.register(
                "ZREVRANGEBYSCORE",
                CommandDescriptor.of(-4, 1, 1, 1),
                args -> parseZRangeByScore(args, true),
                this::zrevrangebyscore
        );
        registration.register(
                "ZREMRANGEBYSCORE",
                CommandDescriptor.of(4, 1, 1, 1),
                CommandParsers.exactRequest(4, "zremrangebyscore"),
                this::zremrangebyscore
        );
        registration.register(
                "ZREMRANGEBYRANK",
                CommandDescriptor.of(4, 1, 1, 1),
                CommandParsers.exactRequest(4, "zremrangebyrank"),
                this::zremrangebyrank
        );
        registration.register("ZREM", CommandDescriptor.of(-3, 1, 1, 1), CommandParsers.minRequest(3, "zrem"), this::zrem);
    }

    private void zadd(ArgReader args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        int pairsLen = args.argc() - 2;
        ExecutionRequest request = args.request();
        support.sliceResetFromRequest(request, 2, pairsLen);
        try {
            long added = support.recordWriteValue(
                    ctx,
                    support.commandDb(ctx).writes().zsets().zadd(request.readOnlyByteArray(1), support.slice())
            );
            out.integer(added);
        } finally {
            support.clearScratch(pairsLen);
        }
    }

    private record ZRangeArgs(byte[] key, long start, long stop, boolean withScores, boolean rev) {
    }

    private CommandParseResult<ZRangeArgs> parseZRange(ArgReader args) {
        CommandParseError arity = CommandArity.range(4, 6, "zrange").validate(args);
        if (arity != null) {
            return CommandParseResult.error(arity);
        }
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

    private void zrange(ZRangeArgs args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        BulkStringSequence seq = args.rev()
                ? support.commandDb(ctx).reads().zsets().zrevrange(args.key(), args.start(), args.stop(), args.withScores())
                : support.commandDb(ctx).reads().zsets().zrange(args.key(), args.start(), args.stop(), args.withScores());
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void zrevrange(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 4 && request.argc() != 5) {
            CommandSupport.wrongArity(out, "zrevrange");
            return;
        }
        long start = CommandSupport.parseLong(request, 2, "start");
        long stop = CommandSupport.parseLong(request, 3, "stop");

        boolean withScores = false;
        if (request.argc() == 5) {
            if (!CommandSupport.asciiEqualsIgnoreCase(request, 4, "WITHSCORES")) {
                out.error("ERR syntax error");
                return;
            }
            withScores = true;
        }

        byte[] key = request.readOnlyByteArray(1);
        BulkStringSequence seq = support.commandDb(ctx).reads().zsets().zrevrange(key, start, stop, withScores);
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
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
        String commandLower = reverse ? "zrevrangebyscore" : "zrangebyscore";
        CommandParseError arity = CommandArity.min(4, commandLower).validate(args);
        if (arity != null) {
            return CommandParseResult.error(arity);
        }
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
        long offset = 0;
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
                    count = args.nonNegativeLongAt(i + 2);
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

    private void zrangebyscore(ZRangeByScoreArgs args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        BulkStringSequence seq = support.commandDb(ctx).reads().zsets().zrangeByScore(
                args.key(),
                args.min().value,
                args.min().exclusive,
                args.max().value,
                args.max().exclusive,
                args.withScores(),
                args.offset(),
                args.count()
        );
        int replyCount = seq.count();
        out.arrayHeader(replyCount);
        if (replyCount == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void zremrangebyscore(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 4) {
            CommandSupport.wrongArity(out, "zremrangebyscore");
            return;
        }

        CommandSupport.ScoreBound min = CommandSupport.parseScoreBound(request.readOnlyByteArray(2));
        CommandSupport.ScoreBound max = CommandSupport.parseScoreBound(request.readOnlyByteArray(3));
        var result = support.commandDb(ctx).writes().zsets().zremrangeByScore(
                request.readOnlyByteArray(1),
                min.value,
                min.exclusive,
                max.value,
                max.exclusive
        );
        out.integer(support.recordWriteValue(ctx, result));
    }

    private void zrevrangebyscore(ZRangeByScoreArgs args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        BulkStringSequence seq = support.commandDb(ctx).reads().zsets().zrevrangeByScore(
                args.key(),
                args.min().value,
                args.min().exclusive,
                args.max().value,
                args.max().exclusive,
                args.withScores(),
                args.offset(),
                args.count()
        );
        int replyCount = seq.count();
        out.arrayHeader(replyCount);
        if (replyCount == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void zremrangebyrank(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 4) {
            CommandSupport.wrongArity(out, "zremrangebyrank");
            return;
        }
        long start = CommandSupport.parseLong(request, 2, "start");
        long stop = CommandSupport.parseLong(request, 3, "stop");
        long removed = support.recordWriteValue(
                ctx,
                support.commandDb(ctx).writes().zsets().zremrangeByRank(request.readOnlyByteArray(1), start, stop)
        );
        out.integer(removed);
    }

    private void zrem(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "zrem");
            return;
        }
        int membersLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, membersLen);
        try {
            long removed = support.recordWriteValue(
                    ctx,
                    support.commandDb(ctx).writes().zsets().zrem(request.readOnlyByteArray(1), support.slice())
            );
            out.integer(removed);
        } finally {
            support.clearScratch(membersLen);
        }
    }
}
