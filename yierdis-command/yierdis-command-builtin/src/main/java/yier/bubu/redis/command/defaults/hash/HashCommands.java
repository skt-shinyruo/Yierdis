package yier.bubu.redis.command.defaults.hash;

import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CollectionScanCommandSupport;
import yier.bubu.redis.command.defaults.CommandSupport;

import yier.bubu.redis.storage.api.result.BulkStringMapMetrics;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.util.Objects;

public final class HashCommands implements CommandModule {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public HashCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(CommandSpec.of(
                syntax("HSET", CommandArity.pairTail(4, 2)),
                CommandParsers.args(),
                this::hset
        ));
        registration.register(CommandSpec.of(syntax("HGET", CommandArity.exact(3)), CommandParsers.request(), this::hget));
        registration.register(CommandSpec.of(syntax("HGETALL", CommandArity.exact(2)), CommandParsers.request(), this::hgetall));
        registration.register(CommandSpec.of(syntax("HLEN", CommandArity.exact(2)), CommandParsers.request(), this::hlen));
        registration.register(CommandSpec.of(syntax("HDEL", CommandArity.min(3)), CommandParsers.request(), this::hdel));
        registration.register(CommandSpec.of(
                syntax("HSCAN", CommandArity.min(3)),
                args -> CollectionScanCommandSupport.parse(args, true),
                this::hscan
        ));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
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

    private void hscan(CollectionScanCommandSupport.Arguments args, CommandContext ctx) {
        CollectionScanCommandSupport.writeReply(
                ctx.out(),
                support.commandDb(ctx).reads().hashes().hscan(
                        args.key(),
                        args.cursor(),
                        args.match(),
                        args.count(),
                        args.noValues()
                )
        );
    }
}
