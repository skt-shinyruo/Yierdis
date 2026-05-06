package yier.bubu.redis.command.defaults.hll;

import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.defaults.BulkStringReplyAdapter;
import yier.bubu.redis.command.defaults.CommandSupport;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;

import java.util.Objects;

public final class HllCommands implements CommandModule {
    private final CommandSupport support;

    public HllCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("PFADD", CommandDescriptor.of(-3, 1, 1, 1), CommandParsers.minRequest(3, "pfadd"), this::pfadd);
        registration.register("PFCOUNT", CommandDescriptor.of(-2, 1, -1, 1), CommandParsers.minRequest(2, "pfcount"), this::pfcount);
        registration.register("PFMERGE", CommandDescriptor.of(-3, 1, -1, 1), CommandParsers.minRequest(3, "pfmerge"), this::pfmerge);
    }

    private void pfadd(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "pfadd");
            return;
        }
        int elementsLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, elementsLen);
        try {
            var result = support.dbWrites(ctx).hll().pfadd(request.readOnlyByteArray(1), support.slice());
            support.recordMutation(ctx, result.mutationOutcome());
            out.integer(result.value());
        } finally {
            support.clearScratch(elementsLen);
        }
    }

    private void pfcount(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 2) {
            CommandSupport.wrongArity(out, "pfcount");
            return;
        }
        int len = request.argc() - 1;
        support.sliceResetFromRequest(request, 1, len);
        try {
            out.integer(support.dbReads(ctx).hll().pfcount(support.slice()));
        } finally {
            support.clearScratch(len);
        }
    }

    private void pfmerge(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "pfmerge");
            return;
        }
        int sourcesLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, sourcesLen);
        try {
            var result = support.dbWrites(ctx).hll().pfmerge(request.readOnlyByteArray(1), support.slice());
            support.recordMutation(ctx, result.mutationOutcome());
        } finally {
            support.clearScratch(sourcesLen);
        }
        out.simpleString("OK");
    }
}
