package yier.bubu.redis.command;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

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

    private void zadd(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4) {
            CommandSupport.wrongArity(out, "zadd");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + CommandSupport.ENTRY_OVERHEAD_ESTIMATE_BYTES;
        for (int i = 2; i < cmd.argc(); i++) {
            extra += Math.max(0, cmd.len(i));
        }
        support.db().prepareWrite(extra);
        int pairsLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, pairsLen);
        try {
            long added = support.db().zadd(cmd.toByteArray(1), support.slice());
            support.db().enforceMaxmemory();
            out.integer(added);
        } finally {
            support.clearScratch(pairsLen);
        }
    }

    private void zrange(RespCommand cmd, RespWriter out) {
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
        int count = rev
                ? support.db().zrevrangeReplyCount(key, start, stop, withScores)
                : support.db().zrangeReplyCount(key, start, stop, withScores);
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        if (rev) {
            support.db().zrevrangeReplyInto(key, start, stop, withScores, support.bulkOut(out));
        } else {
            support.db().zrangeReplyInto(key, start, stop, withScores, support.bulkOut(out));
        }
    }

    private void zrevrange(RespCommand cmd, RespWriter out) {
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
        int count = support.db().zrevrangeReplyCount(key, start, stop, withScores);
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        support.db().zrevrangeReplyInto(key, start, stop, withScores, support.bulkOut(out));
    }

    private void zrangebyscore(RespCommand cmd, RespWriter out) {
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
        int replyCount = support.db().zrangeByScoreReplyCount(
                key,
                min.value,
                min.exclusive,
                max.value,
                max.exclusive,
                withScores,
                offset,
                count
        );
        out.arrayHeader(replyCount);
        if (replyCount == 0) {
            return;
        }
        support.db().zrangeByScoreReplyInto(
                key,
                min.value,
                min.exclusive,
                max.value,
                max.exclusive,
                withScores,
                offset,
                count,
                support.bulkOut(out)
        );
    }

    private void zremrangebyscore(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "zremrangebyscore");
            return;
        }

        CommandSupport.ScoreBound min = CommandSupport.parseScoreBound(cmd.toByteArray(2));
        CommandSupport.ScoreBound max = CommandSupport.parseScoreBound(cmd.toByteArray(3));
        out.integer(support.db().zremrangeByScore(cmd.toByteArray(1), min.value, min.exclusive, max.value, max.exclusive));
    }

    private void zrevrangebyscore(RespCommand cmd, RespWriter out) {
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
        int replyCount = support.db().zrevrangeByScoreReplyCount(
                key,
                min.value,
                min.exclusive,
                max.value,
                max.exclusive,
                withScores,
                offset,
                count
        );
        out.arrayHeader(replyCount);
        if (replyCount == 0) {
            return;
        }
        support.db().zrevrangeByScoreReplyInto(
                key,
                min.value,
                min.exclusive,
                max.value,
                max.exclusive,
                withScores,
                offset,
                count,
                support.bulkOut(out)
        );
    }

    private void zremrangebyrank(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "zremrangebyrank");
            return;
        }
        long start = CommandSupport.parseLong(cmd, 2, "start");
        long stop = CommandSupport.parseLong(cmd, 3, "stop");
        out.integer(support.db().zremrangeByRank(cmd.toByteArray(1), start, stop));
    }

    private void zrem(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "zrem");
            return;
        }
        int membersLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, membersLen);
        try {
            out.integer(support.db().zrem(cmd.toByteArray(1), support.slice()));
        } finally {
            support.clearScratch(membersLen);
        }
    }
}
