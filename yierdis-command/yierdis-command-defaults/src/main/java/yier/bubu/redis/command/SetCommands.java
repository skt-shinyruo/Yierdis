package yier.bubu.redis.command;

import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
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
        registration.register("SADD", CommandDescriptor.of(-3, 1, 1, 1), CommandParsers.minRequest(3, "sadd"), this::sadd);
        registration.register("SREM", CommandDescriptor.of(-3, 1, 1, 1), CommandParsers.minRequest(3, "srem"), this::srem);
        registration.register("SMEMBERS", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exactRequest(2, "smembers"), this::smembers);
        registration.register("SISMEMBER", CommandDescriptor.of(3, 1, 1, 1), CommandParsers.exactRequest(3, "sismember"), this::sismember);
        registration.register("SCARD", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exactRequest(2, "scard"), this::scard);
    }

    private void sadd(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "sadd");
            return;
        }
        int membersLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, membersLen);
        try {
            long added = support.dbWrites(ctx).sets().sadd(request.readOnlyByteArray(1), support.slice());
            out.integer(added);
        } finally {
            support.clearScratch(membersLen);
        }
    }

    private void srem(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "srem");
            return;
        }
        int membersLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, membersLen);
        try {
            out.integer(support.dbWrites(ctx).sets().srem(request.readOnlyByteArray(1), support.slice()));
        } finally {
            support.clearScratch(membersLen);
        }
    }

    private void smembers(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "smembers");
            return;
        }

        byte[] key = request.readOnlyByteArray(1);
        BulkStringSequence seq = support.dbReads(ctx).sets().smembers(key);
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void sismember(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 3) {
            CommandSupport.wrongArity(out, "sismember");
            return;
        }
        out.integer(support.dbReads(ctx).sets().sismember(request.readOnlyByteArray(1), request.readOnlyByteArray(2)) ? 1 : 0);
    }

    private void scard(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "scard");
            return;
        }
        out.integer(support.dbReads(ctx).sets().scard(request.readOnlyByteArray(1)));
    }
}
