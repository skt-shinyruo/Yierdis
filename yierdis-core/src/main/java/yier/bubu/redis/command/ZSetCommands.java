package yier.bubu.redis.command;

import yier.bubu.redis.db.DbMemoryConstants;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.ZSetOps;
import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.ReplyWriter;

import java.util.Objects;

final class ZSetCommands {
    private final CommandSupport support;

    ZSetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("ZADD", this::zadd);
        registry.register("ZRANGE", this::zrange);
        registry.register("ZREVRANGE", this::zrevrange);
        registry.register("ZRANGEBYSCORE", this::zrangebyscore);
        registry.register("ZREVRANGEBYSCORE", this::zrevrangebyscore);
        registry.register("ZREMRANGEBYSCORE", this::zremrangebyscore);
        registry.register("ZREMRANGEBYRANK", this::zremrangebyrank);
        registry.register("ZREM", this::zrem);
    }

    private void zadd(Command cmd, ReplyWriter out) {
        if (cmd.argc() < 4) {
            CommandSupport.wrongArity(out, "zadd");
            return;
        }
        DbEngine engine = support.db(out);
        long extra = (long) Math.max(0, cmd.len(1)) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        for (int i = 2; i < cmd.argc(); i++) {
            extra += Math.max(0, cmd.len(i));
        }
        engine.eviction().prepareWrite(extra);
        int pairsLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, pairsLen);
        try {
            long added = engine.values().zsets().zadd(cmd.toByteArray(1), support.slice());
            out.integer(added);
        } finally {
            support.clearScratch(pairsLen);
        }
    }

    private void zrange(Command cmd, ReplyWriter out) {
        if (cmd.argc() < 4 || cmd.argc() > 6) {
            CommandSupport.wrongArity(out, "zrange");
            return;
        }
        long start = CommandSupport.parseLong(cmd, 2, "start");
        long stop = CommandSupport.parseLong(cmd, 3, "stop");

        boolean withScores = false;
        boolean rev = false;
        for (int i = 4; i < cmd.argc(); i++) {
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "WITHSCORES")) {
                withScores = true;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "REV")) {
                rev = true;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        ZSetOps zsets = support.db(out).values().zsets();
        BulkStringSequence seq = rev
                ? zsets.zrevrange(key, start, stop, withScores)
                : zsets.zrange(key, start, stop, withScores);
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void zrevrange(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 4 && cmd.argc() != 5) {
            CommandSupport.wrongArity(out, "zrevrange");
            return;
        }
        long start = CommandSupport.parseLong(cmd, 2, "start");
        long stop = CommandSupport.parseLong(cmd, 3, "stop");

        boolean withScores = false;
        if (cmd.argc() == 5) {
            if (!CommandSupport.asciiEqualsIgnoreCase(cmd, 4, "WITHSCORES")) {
                out.error("ERR syntax error");
                return;
            }
            withScores = true;
        }

        byte[] key = cmd.toByteArray(1);
        ZSetOps zsets = support.db(out).values().zsets();
        BulkStringSequence seq = zsets.zrevrange(key, start, stop, withScores);
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void zrangebyscore(Command cmd, ReplyWriter out) {
        if (cmd.argc() < 4) {
            CommandSupport.wrongArity(out, "zrangebyscore");
            return;
        }

        CommandSupport.ScoreBound min = CommandSupport.parseScoreBound(cmd.toByteArray(2));
        CommandSupport.ScoreBound max = CommandSupport.parseScoreBound(cmd.toByteArray(3));

        boolean withScores = false;
        long offset = 0;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < cmd.argc()) {
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "WITHSCORES")) {
                withScores = true;
                i++;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "LIMIT")) {
                if (i + 2 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                offset = CommandSupport.parseNonNegativeLong(cmd, i + 1, "offset");
                count = CommandSupport.parseNonNegativeLong(cmd, i + 2, "count");
                i += 3;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        ZSetOps zsets = support.db(out).values().zsets();
        BulkStringSequence seq = zsets.zrangeByScore(
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

    private void zremrangebyscore(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "zremrangebyscore");
            return;
        }

        CommandSupport.ScoreBound min = CommandSupport.parseScoreBound(cmd.toByteArray(2));
        CommandSupport.ScoreBound max = CommandSupport.parseScoreBound(cmd.toByteArray(3));
        out.integer(support.db(out).values().zsets().zremrangeByScore(cmd.toByteArray(1), min.value, min.exclusive, max.value, max.exclusive));
    }

    private void zrevrangebyscore(Command cmd, ReplyWriter out) {
        if (cmd.argc() < 4) {
            CommandSupport.wrongArity(out, "zrevrangebyscore");
            return;
        }

        CommandSupport.ScoreBound max = CommandSupport.parseScoreBound(cmd.toByteArray(2));
        CommandSupport.ScoreBound min = CommandSupport.parseScoreBound(cmd.toByteArray(3));

        boolean withScores = false;
        long offset = 0;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < cmd.argc()) {
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "WITHSCORES")) {
                withScores = true;
                i++;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "LIMIT")) {
                if (i + 2 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                offset = CommandSupport.parseNonNegativeLong(cmd, i + 1, "offset");
                count = CommandSupport.parseNonNegativeLong(cmd, i + 2, "count");
                i += 3;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        ZSetOps zsets = support.db(out).values().zsets();
        BulkStringSequence seq = zsets.zrevrangeByScore(
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

    private void zremrangebyrank(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "zremrangebyrank");
            return;
        }
        long start = CommandSupport.parseLong(cmd, 2, "start");
        long stop = CommandSupport.parseLong(cmd, 3, "stop");
        out.integer(support.db(out).values().zsets().zremrangeByRank(cmd.toByteArray(1), start, stop));
    }

    private void zrem(Command cmd, ReplyWriter out) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "zrem");
            return;
        }
        int membersLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, membersLen);
        try {
            out.integer(support.db(out).values().zsets().zrem(cmd.toByteArray(1), support.slice()));
        } finally {
            support.clearScratch(membersLen);
        }
    }
}
