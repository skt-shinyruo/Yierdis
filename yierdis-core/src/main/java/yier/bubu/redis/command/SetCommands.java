package yier.bubu.redis.command;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespWriter;

import java.util.Objects;

final class SetCommands {
    private final CommandSupport support;

    SetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("SADD", this::sadd);
        registry.register("SREM", this::srem);
        registry.register("SMEMBERS", this::smembers);
        registry.register("SISMEMBER", this::sismember);
        registry.register("SCARD", this::scard);
    }

    private void sadd(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "sadd");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + CommandSupport.ENTRY_OVERHEAD_ESTIMATE_BYTES;
        for (int i = 2; i < cmd.argc(); i++) {
            extra += Math.max(0, cmd.len(i));
        }
        support.db().prepareWrite(extra);
        int membersLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, membersLen);
        try {
            long added = support.db().sadd(cmd.toByteArray(1), support.slice());
            support.db().enforceMaxmemory();
            out.integer(added);
        } finally {
            support.clearScratch(membersLen);
        }
    }

    private void srem(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "srem");
            return;
        }
        int membersLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, membersLen);
        try {
            out.integer(support.db().srem(cmd.toByteArray(1), support.slice()));
        } finally {
            support.clearScratch(membersLen);
        }
    }

    private void smembers(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "smembers");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        int count = support.db().smembersReplyCount(key);
        if (out.protocol() == RespProtocol.RESP3) {
            out.setHeader(count);
        } else {
            out.arrayHeader(count);
        }
        if (count == 0) {
            return;
        }
        support.db().smembersReplyInto(key, support.bulkOut(out));
    }

    private void sismember(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "sismember");
            return;
        }
        out.integer(support.db().sismember(cmd.toByteArray(1), cmd.toByteArray(2)) ? 1 : 0);
    }

    private void scard(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "scard");
            return;
        }
        out.integer(support.db().scard(cmd.toByteArray(1)));
    }
}
