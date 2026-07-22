package yier.bubu.redis.command.defaults.set;

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

import yier.bubu.redis.storage.api.result.MeasuredBulkStringSequence;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.util.Objects;

public final class SetCommands implements CommandModule {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public SetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(CommandSpec.of(syntax("SADD", CommandArity.min(3)), CommandParsers.request(), this::sadd));
        registration.register(CommandSpec.of(syntax("SREM", CommandArity.min(3)), CommandParsers.request(), this::srem));
        registration.register(CommandSpec.of(syntax("SMEMBERS", CommandArity.exact(2)), CommandParsers.request(), this::smembers));
        registration.register(CommandSpec.of(syntax("SISMEMBER", CommandArity.exact(3)), CommandParsers.request(), this::sismember));
        registration.register(CommandSpec.of(syntax("SCARD", CommandArity.exact(2)), CommandParsers.request(), this::scard));
        registration.register(CommandSpec.of(
                syntax("SSCAN", CommandArity.min(3)),
                args -> CollectionScanCommandSupport.parse(args, false),
                this::sscan
        ));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private void sadd(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
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

        byte[] key = request.readOnlyByteArray(1);
        MeasuredBulkStringSequence seq = support.commandDb(ctx).reads().sets().smembers(key);
        CommandSupport.writeMeasuredBulkStringArray(out, seq);
    }

    private void sismember(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        out.integer(support.commandDb(ctx).reads().sets().sismember(request.readOnlyByteArray(1), request.readOnlyByteArray(2)) ? 1 : 0);
    }

    private void scard(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        out.integer(support.commandDb(ctx).reads().sets().scard(request.readOnlyByteArray(1)));
    }

    private void sscan(CollectionScanCommandSupport.Arguments args, CommandContext ctx) {
        CollectionScanCommandSupport.writeReply(
                ctx.out(),
                support.commandDb(ctx).reads().sets().sscan(
                        args.key(),
                        args.cursor(),
                        args.match(),
                        args.count()
                )
        );
    }
}
