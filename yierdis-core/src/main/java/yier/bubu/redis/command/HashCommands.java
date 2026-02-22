package yier.bubu.redis.command;

import yier.bubu.redis.db.DbMemoryConstants;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.result.BulkStringMapPairs;
import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.ReplyWriter;

import java.util.Objects;

final class HashCommands {
    private final CommandSupport support;

    HashCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("HSET", this::hset);
        registry.register("HGET", this::hget);
        registry.register("HGETALL", this::hgetall);
        registry.register("HLEN", this::hlen);
        registry.register("HDEL", this::hdel);
    }

    private void hset(Command cmd, ReplyWriter out) {
        if (cmd.argc() < 4) {
            CommandSupport.wrongArity(out, "hset");
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
            long added = engine.values().hashes().hset(cmd.toByteArray(1), support.slice());
            out.integer(added);
        } finally {
            support.clearScratch(pairsLen);
        }
    }

    private void hget(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "hget");
            return;
        }
        DbEngine engine = support.db(out);
        out.bulkString(engine.values().hashes().hget(cmd.toByteArray(1), cmd.toByteArray(2)));
    }

    private void hgetall(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "hgetall");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        DbEngine engine = support.db(out);
        BulkStringMapPairs pairsResult = engine.values().hashes().hgetall(key);
        int pairs = pairsResult.pairCount();
        out.mapHeader(pairs);
        if (pairs == 0) {
            return;
        }
        pairsResult.emitPairsTo(new BulkStringReplyAdapter(out));
    }

    private void hlen(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "hlen");
            return;
        }
        DbEngine engine = support.db(out);
        out.integer(engine.values().hashes().hlen(cmd.toByteArray(1)));
    }

    private void hdel(Command cmd, ReplyWriter out) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "hdel");
            return;
        }
        int fieldsLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, fieldsLen);
        try {
            DbEngine engine = support.db(out);
            out.integer(engine.values().hashes().hdel(cmd.toByteArray(1), support.slice()));
        } finally {
            support.clearScratch(fieldsLen);
        }
    }
}
