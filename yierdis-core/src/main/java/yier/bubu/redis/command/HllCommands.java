package yier.bubu.redis.command;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

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

    private void pfadd(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "pfadd");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + HLL_DENSE_BYTES + CommandSupport.ENTRY_OVERHEAD_ESTIMATE_BYTES;
        support.db().prepareWrite(extra);
        long changed = support.db().pfadd(cmd.toByteArray(1), cmd, 2);
        support.db().enforceMaxmemory();
        out.integer(changed);
    }

    private void pfcount(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            CommandSupport.wrongArity(out, "pfcount");
            return;
        }
        int len = cmd.argc() - 1;
        support.sliceResetFromCommand(cmd, 1, len);
        try {
            out.integer(support.db().pfcount(support.slice()));
        } finally {
            support.clearScratch(len);
        }
    }

    private void pfmerge(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "pfmerge");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + HLL_DENSE_BYTES + CommandSupport.ENTRY_OVERHEAD_ESTIMATE_BYTES;
        support.db().prepareWrite(extra);

        int sourcesLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, sourcesLen);
        try {
            support.db().pfmerge(cmd.toByteArray(1), support.slice());
        } finally {
            support.clearScratch(sourcesLen);
        }
        support.db().enforceMaxmemory();
        out.simpleString("OK");
    }
}
