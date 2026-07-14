package yier.bubu.redis.command.defaults.hash;

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

import yier.bubu.redis.storage.api.result.BulkStringMapMetrics;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.util.Objects;

public final class HashCommands implements CommandModule {
    private final CommandSupport support;

    public HashCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(
                "HSET",
                CommandDescriptor.of(-4, 1, 1, 1),
                CommandParsers.pairTail(4, 2, "hset"),
                this::hset
        );
        registration.register("HGET", CommandDescriptor.of(3, 1, 1, 1), CommandParsers.exactRequest(3, "hget"), this::hget);
        registration.register("HGETALL", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exactRequest(2, "hgetall"), this::hgetall);
        registration.register("HLEN", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exactRequest(2, "hlen"), this::hlen);
        registration.register("HDEL", CommandDescriptor.of(-3, 1, 1, 1), CommandParsers.minRequest(3, "hdel"), this::hdel);
    }

    private void hset(ArgReader args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        int pairsLen = args.argc() - 2;
        ExecutionRequest request = args.request();
        support.sliceResetFromRequest(request, 2, pairsLen);
        try {
            long added = support.commandDb(ctx).writes().hashes()
                    .hset(request.readOnlyByteArray(1), support.slice())
                    .value();
            out.integer(added);
        } finally {
            support.clearScratch(pairsLen);
        }
    }

    private void hget(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 3) {
            CommandSupport.wrongArity(out, "hget");
            return;
        }
        BulkStringValue value = support.commandDb(ctx).reads().hashes()
                .hget(request.readOnlyByteArray(1), request.readOnlyByteArray(2));
        CommandSupport.writeOwnedBulkString(out, value);
    }

    private void hgetall(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "hgetall");
            return;
        }

        byte[] key = request.readOnlyByteArray(1);
        BulkStringMapMetrics pairsResult = support.commandDb(ctx).reads().hashes().hgetall(key);
        CommandSupport.writeMeasuredBulkStringMap(out, pairsResult);
    }

    private void hlen(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "hlen");
            return;
        }
        out.integer(support.commandDb(ctx).reads().hashes().hlen(request.readOnlyByteArray(1)));
    }

    private void hdel(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "hdel");
            return;
        }
        int fieldsLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, fieldsLen);
        try {
            long deleted = support.commandDb(ctx).writes().hashes()
                    .hdel(request.readOnlyByteArray(1), support.slice())
                    .value();
            out.integer(deleted);
        } finally {
            support.clearScratch(fieldsLen);
        }
    }
}
