package yier.bubu.redis.command;

import yier.bubu.redis.db.DbMemoryConstants;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.CommandContext;
import yier.bubu.redis.protocol.ReplyWriter;

import java.util.Objects;

final class HllCommands {
    private static final int HLL_DENSE_BYTES = 8 + 12288;

    private final CommandSupport support;

    HllCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("PFADD", this::pfadd);
        registry.register("PFCOUNT", this::pfcount);
        registry.register("PFMERGE", this::pfmerge);
    }

    private void pfadd(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "pfadd");
            return;
        }
        DbEngine engine = support.db(ctx);
        long extra = (long) Math.max(0, cmd.len(1)) + HLL_DENSE_BYTES + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        engine.eviction().prepareWrite(extra);
        int elementsLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, elementsLen);
        try {
            out.integer(engine.values().hll().pfadd(cmd.toByteArray(1), support.slice()));
        } finally {
            support.clearScratch(elementsLen);
        }
    }

    private void pfcount(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 2) {
            CommandSupport.wrongArity(out, "pfcount");
            return;
        }
        int len = cmd.argc() - 1;
        support.sliceResetFromCommand(cmd, 1, len);
        try {
            out.integer(support.db(ctx).values().hll().pfcount(support.slice()));
        } finally {
            support.clearScratch(len);
        }
    }

    private void pfmerge(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "pfmerge");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + HLL_DENSE_BYTES + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        DbEngine engine = support.db(ctx);
        engine.eviction().prepareWrite(extra);

        int sourcesLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, sourcesLen);
        try {
            engine.values().hll().pfmerge(cmd.toByteArray(1), support.slice());
        } finally {
            support.clearScratch(sourcesLen);
        }
        out.simpleString("OK");
    }
}
