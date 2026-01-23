package yier.bubu.redis.command;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespWriter;

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

    private void hset(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4) {
            CommandSupport.wrongArity(out, "hset");
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
            long added = support.db().hset(cmd.toByteArray(1), support.slice());
            support.db().enforceMaxmemory();
            out.integer(added);
        } finally {
            support.clearScratch(pairsLen);
        }
    }

    private void hget(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "hget");
            return;
        }
        out.bulkString(support.db().hget(cmd.toByteArray(1), cmd.toByteArray(2)));
    }

    private void hgetall(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "hgetall");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        int count = support.db().hgetallReplyCount(key);
        if (out.protocol() == RespProtocol.RESP3) {
            out.mapHeader(count / 2);
        } else {
            out.arrayHeader(count);
        }
        if (count == 0) {
            return;
        }
        support.db().hgetallReplyInto(key, support.bulkOut(out));
    }

    private void hlen(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "hlen");
            return;
        }
        out.integer(support.db().hlen(cmd.toByteArray(1)));
    }

    private void hdel(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "hdel");
            return;
        }
        int fieldsLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, fieldsLen);
        try {
            out.integer(support.db().hdel(cmd.toByteArray(1), support.slice()));
        } finally {
            support.clearScratch(fieldsLen);
        }
    }
}
