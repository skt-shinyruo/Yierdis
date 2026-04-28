package yier.bubu.redis.command;

import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;

import java.util.Objects;

final class ZSetCommands implements CommandModule {
    private final CommandSupport support;

    ZSetCommands(CommandSupport support) {
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
        registration.register("ZRANGE", this::zrange, CommandDescriptor.of(-4, 1, 1, 1));
        registration.register("ZREVRANGE", this::zrevrange, CommandDescriptor.of(-4, 1, 1, 1));
        registration.register("ZRANGEBYSCORE", this::zrangebyscore, CommandDescriptor.of(-4, 1, 1, 1));
        registration.register("ZREVRANGEBYSCORE", this::zrevrangebyscore, CommandDescriptor.of(-4, 1, 1, 1));
        registration.register("ZREMRANGEBYSCORE", this::zremrangebyscore, CommandDescriptor.of(4, 1, 1, 1));
        registration.register("ZREMRANGEBYRANK", this::zremrangebyrank, CommandDescriptor.of(4, 1, 1, 1));
        registration.register("ZREM", this::zrem, CommandDescriptor.of(-3, 1, 1, 1));
    }

    private void zadd(ArgReader args, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        int pairsLen = args.argc() - 2;
        ExecutionRequest request = args.request();
        support.sliceResetFromRequest(request, 2, pairsLen);
        try {
            long added = support.dbWrites(ctx).zsets().zadd(request.readOnlyByteArray(1), support.slice());
            out.integer(added);
        } finally {
            support.clearScratch(pairsLen);
        }
    }

    private void zrange(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 4 || request.argc() > 6) {
            CommandSupport.wrongArity(out, "zrange");
            return;
        }
        long start = CommandSupport.parseLong(request, 2, "start");
        long stop = CommandSupport.parseLong(request, 3, "stop");

        boolean withScores = false;
        boolean rev = false;
        for (int i = 4; i < request.argc(); i++) {
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "WITHSCORES")) {
                withScores = true;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "REV")) {
                rev = true;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        byte[] key = request.readOnlyByteArray(1);
        BulkStringSequence seq = rev
                ? support.dbReads(ctx).zsets().zrevrange(key, start, stop, withScores)
                : support.dbReads(ctx).zsets().zrange(key, start, stop, withScores);
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void zrevrange(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
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
        BulkStringSequence seq = support.dbReads(ctx).zsets().zrevrange(key, start, stop, withScores);
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void zrangebyscore(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 4) {
            CommandSupport.wrongArity(out, "zrangebyscore");
            return;
        }

        CommandSupport.ScoreBound min = CommandSupport.parseScoreBound(request.readOnlyByteArray(2));
        CommandSupport.ScoreBound max = CommandSupport.parseScoreBound(request.readOnlyByteArray(3));

        boolean withScores = false;
        long offset = 0;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < request.argc()) {
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "WITHSCORES")) {
                withScores = true;
                i++;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "LIMIT")) {
                if (i + 2 >= request.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                offset = CommandSupport.parseNonNegativeLong(request, i + 1, "offset");
                count = CommandSupport.parseNonNegativeLong(request, i + 2, "count");
                i += 3;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        byte[] key = request.readOnlyByteArray(1);
        BulkStringSequence seq = support.dbReads(ctx).zsets().zrangeByScore(
                key,
                min.value,
                min.exclusive,
                max.value,
                max.exclusive,
                withScores,
                offset,
                count
        );
        int replyCount = seq.count();
        out.arrayHeader(replyCount);
        if (replyCount == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void zremrangebyscore(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 4) {
            CommandSupport.wrongArity(out, "zremrangebyscore");
            return;
        }

        CommandSupport.ScoreBound min = CommandSupport.parseScoreBound(request.readOnlyByteArray(2));
        CommandSupport.ScoreBound max = CommandSupport.parseScoreBound(request.readOnlyByteArray(3));
        out.integer(support.dbWrites(ctx).zsets().zremrangeByScore(request.readOnlyByteArray(1), min.value, min.exclusive, max.value, max.exclusive));
    }

    private void zrevrangebyscore(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 4) {
            CommandSupport.wrongArity(out, "zrevrangebyscore");
            return;
        }

        CommandSupport.ScoreBound max = CommandSupport.parseScoreBound(request.readOnlyByteArray(2));
        CommandSupport.ScoreBound min = CommandSupport.parseScoreBound(request.readOnlyByteArray(3));

        boolean withScores = false;
        long offset = 0;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < request.argc()) {
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "WITHSCORES")) {
                withScores = true;
                i++;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "LIMIT")) {
                if (i + 2 >= request.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                offset = CommandSupport.parseNonNegativeLong(request, i + 1, "offset");
                count = CommandSupport.parseNonNegativeLong(request, i + 2, "count");
                i += 3;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        byte[] key = request.readOnlyByteArray(1);
        BulkStringSequence seq = support.dbReads(ctx).zsets().zrevrangeByScore(
                key,
                min.value,
                min.exclusive,
                max.value,
                max.exclusive,
                withScores,
                offset,
                count
        );
        int replyCount = seq.count();
        out.arrayHeader(replyCount);
        if (replyCount == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void zremrangebyrank(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 4) {
            CommandSupport.wrongArity(out, "zremrangebyrank");
            return;
        }
        long start = CommandSupport.parseLong(request, 2, "start");
        long stop = CommandSupport.parseLong(request, 3, "stop");
        out.integer(support.dbWrites(ctx).zsets().zremrangeByRank(request.readOnlyByteArray(1), start, stop));
    }

    private void zrem(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "zrem");
            return;
        }
        int membersLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, membersLen);
        try {
            out.integer(support.dbWrites(ctx).zsets().zrem(request.readOnlyByteArray(1), support.slice()));
        } finally {
            support.clearScratch(membersLen);
        }
    }
}
