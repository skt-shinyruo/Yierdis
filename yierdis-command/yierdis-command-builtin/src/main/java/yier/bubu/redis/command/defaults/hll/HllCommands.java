package yier.bubu.redis.command.defaults.hll;

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
import yier.bubu.redis.command.defaults.BulkStringReplyAdapter;
import yier.bubu.redis.command.defaults.CommandSupport;

import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.util.Objects;

public final class HllCommands implements CommandModule {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);
    private static final CommandKeySpec MULTI_KEYS = new CommandKeySpec(1, -1, 1);

    private final CommandSupport support;

    public HllCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(CommandSpec.of(syntax("PFADD", CommandArity.min(3), KEY), CommandParsers.request(), this::pfadd));
        registration.register(CommandSpec.of(syntax("PFCOUNT", CommandArity.min(2), MULTI_KEYS), CommandParsers.request(), this::pfcount));
        registration.register(CommandSpec.of(syntax("PFMERGE", CommandArity.min(3), MULTI_KEYS), CommandParsers.request(), this::pfmerge));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity, CommandKeySpec keys) {
        return new CommandSyntax(nameUpper, arity, keys, TransactionPolicy.QUEUEABLE);
    }

    private void pfadd(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "pfadd");
            return;
        }
        int elementsLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, elementsLen);
        try {
            long changed = support.commandDb(ctx).writes().hll()
                    .pfadd(request.readOnlyByteArray(1), support.slice())
                    .value();
            out.integer(changed);
        } finally {
            support.clearScratch(elementsLen);
        }
    }

    private void pfcount(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() < 2) {
            CommandSupport.wrongArity(out, "pfcount");
            return;
        }
        int len = request.argc() - 1;
        support.sliceResetFromRequest(request, 1, len);
        try {
            out.integer(support.commandDb(ctx).reads().hll().pfcount(support.slice()));
        } finally {
            support.clearScratch(len);
        }
    }

    private void pfmerge(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "pfmerge");
            return;
        }
        int sourcesLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, sourcesLen);
        try {
            support.commandDb(ctx).writes().hll()
                    .pfmerge(request.readOnlyByteArray(1), support.slice())
                    .value();
        } finally {
            support.clearScratch(sourcesLen);
        }
        out.simpleString("OK");
    }
}
