package yier.bubu.redis.command;

import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ReplyWriter;

import java.util.Objects;

final class SetCommands implements CommandModule {
    private final CommandSupport support;

    SetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("SADD", this::sadd, CommandDescriptor.of(-3, 1, 1, 1));
        registration.register("SREM", this::srem, CommandDescriptor.of(-3, 1, 1, 1));
        registration.register("SMEMBERS", this::smembers, CommandDescriptor.of(2, 1, 1, 1));
        registration.register("SISMEMBER", this::sismember, CommandDescriptor.of(3, 1, 1, 1));
        registration.register("SCARD", this::scard, CommandDescriptor.of(2, 1, 1, 1));
    }

    private void sadd(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "sadd");
            return;
        }
        int membersLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, membersLen);
        try {
            long added = support.dbWrites(ctx).sets().sadd(cmd.toByteArray(1), support.slice());
            out.integer(added);
        } finally {
            support.clearScratch(membersLen);
        }
    }

    private void srem(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "srem");
            return;
        }
        int membersLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, membersLen);
        try {
            out.integer(support.dbWrites(ctx).sets().srem(cmd.toByteArray(1), support.slice()));
        } finally {
            support.clearScratch(membersLen);
        }
    }

    private void smembers(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "smembers");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        BulkStringSequence seq = support.dbReads(ctx).sets().smembers(key);
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void sismember(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "sismember");
            return;
        }
        out.integer(support.dbReads(ctx).sets().sismember(cmd.toByteArray(1), cmd.toByteArray(2)) ? 1 : 0);
    }

    private void scard(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "scard");
            return;
        }
        out.integer(support.dbReads(ctx).sets().scard(cmd.toByteArray(1)));
    }
}
