package yier.bubu.redis.command;

import yier.bubu.redis.ops.result.BulkStringMapPairs;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ReplyWriter;

import java.util.Objects;

final class HashCommands implements CommandModule {
    private final CommandSupport support;

    HashCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("HSET", this::hset);
        registration.register("HGET", this::hget);
        registration.register("HGETALL", this::hgetall);
        registration.register("HLEN", this::hlen);
        registration.register("HDEL", this::hdel);
    }

    private void hset(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 4) {
            CommandSupport.wrongArity(out, "hset");
            return;
        }
        int pairsLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, pairsLen);
        try {
            long added = support.dbWrites(ctx).hashes().hset(cmd.toByteArray(1), support.slice());
            out.integer(added);
        } finally {
            support.clearScratch(pairsLen);
        }
    }

    private void hget(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "hget");
            return;
        }
        out.bulkString(support.dbReads(ctx).hashes().hget(cmd.toByteArray(1), cmd.toByteArray(2)));
    }

    private void hgetall(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "hgetall");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        BulkStringMapPairs pairsResult = support.dbReads(ctx).hashes().hgetall(key);
        int pairs = pairsResult.pairCount();
        out.mapHeader(pairs);
        if (pairs == 0) {
            return;
        }
        pairsResult.emitPairsTo(new BulkStringReplyAdapter(out));
    }

    private void hlen(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "hlen");
            return;
        }
        out.integer(support.dbReads(ctx).hashes().hlen(cmd.toByteArray(1)));
    }

    private void hdel(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "hdel");
            return;
        }
        int fieldsLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, fieldsLen);
        try {
            out.integer(support.dbWrites(ctx).hashes().hdel(cmd.toByteArray(1), support.slice()));
        } finally {
            support.clearScratch(fieldsLen);
        }
    }
}
