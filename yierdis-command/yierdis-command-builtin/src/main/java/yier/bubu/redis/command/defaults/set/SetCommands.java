package yier.bubu.redis.command.defaults.set;

import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.defaults.CommandSupport;

import yier.bubu.redis.storage.api.result.MeasuredBulkStringSequence;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.util.Objects;

public final class SetCommands implements CommandModule {
    private final CommandSupport support;

    public SetCommands(CommandSupport support) {
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
        RedisReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "sadd");
            return;
        }
        int membersLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, membersLen);
        try {
            long added = support.commandDb(ctx).writes().sets()
                    .sadd(request.readOnlyByteArray(1), support.slice())
                    .value();
            out.integer(added);
        } finally {
            support.clearScratch(membersLen);
        }
    }

    private void srem(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "srem");
            return;
        }
        int membersLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, membersLen);
        try {
            long removed = support.commandDb(ctx).writes().sets()
                    .srem(request.readOnlyByteArray(1), support.slice())
                    .value();
            out.integer(removed);
        } finally {
            support.clearScratch(membersLen);
        }
    }

    private void smembers(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "smembers");
            return;
        }

        byte[] key = request.readOnlyByteArray(1);
        MeasuredBulkStringSequence seq = support.commandDb(ctx).reads().sets().smembers(key);
        CommandSupport.writeMeasuredBulkStringArray(out, seq);
    }

    private void sismember(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 3) {
            CommandSupport.wrongArity(out, "sismember");
            return;
        }
        out.integer(support.commandDb(ctx).reads().sets().sismember(request.readOnlyByteArray(1), request.readOnlyByteArray(2)) ? 1 : 0);
    }

    private void scard(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "scard");
            return;
        }
        out.integer(support.commandDb(ctx).reads().sets().scard(request.readOnlyByteArray(1)));
    }
}
