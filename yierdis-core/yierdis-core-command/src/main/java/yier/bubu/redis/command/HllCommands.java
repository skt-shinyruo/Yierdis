package yier.bubu.redis.command;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;

import java.util.Objects;

final class HllCommands implements CommandModule {
    private final CommandSupport support;

    HllCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("PFADD", this::pfadd, CommandDescriptor.of(-3, 1, 1, 1));
        registration.register("PFCOUNT", this::pfcount, CommandDescriptor.of(-2, 1, -1, 1));
        registration.register("PFMERGE", this::pfmerge, CommandDescriptor.of(-3, 1, -1, 1));
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
            out.integer(support.dbWrites(ctx).hll().pfadd(request.readOnlyByteArray(1), support.slice()));
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
            support.dbWrites(ctx).hll().pfmerge(request.readOnlyByteArray(1), support.slice());
        } finally {
            support.clearScratch(sourcesLen);
        }
        out.simpleString("OK");
    }
}
