package yier.bubu.redis.command;

import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.CommandContext;
import yier.bubu.redis.protocol.ReplyWriter;
import yier.bubu.redis.protocol.ServerSession;
import yier.bubu.redis.protocol.TransactionState;

import java.util.List;
import java.util.Objects;

/**
 * Redis 事务命令（最小实现）：MULTI/EXEC/DISCARD。
 * <p>
 * 设计约束：
 * - 连接级状态通过 {@link ServerSession} 暴露，避免 core 依赖 server/Netty
 * - MULTI 态下，普通命令由 {@link YierdisFastCommandProcessor} 负责入队并返回 QUEUED
 */
final class TransactionCommands {
    private final CommandSupport support;
    private final YierdisFastCommandProcessor processor;

    TransactionCommands(CommandSupport support, YierdisFastCommandProcessor processor) {
        this.support = Objects.requireNonNull(support, "support");
        this.processor = Objects.requireNonNull(processor, "processor");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("MULTI", this::multi);
        registry.register("DISCARD", this::discard);
        registry.register("EXEC", this::exec);
    }

    private void multi(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 1) {
            CommandSupport.wrongArity(out, "multi");
            return;
        }
        TransactionState tx = txOrNull(ctx);
        if (tx == null) {
            out.error("ERR MULTI is only supported on server connections");
            return;
        }
        if (tx.active()) {
            out.error("ERR MULTI calls can not be nested");
            return;
        }
        tx.begin();
        out.simpleString("OK");
    }

    private void discard(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 1) {
            CommandSupport.wrongArity(out, "discard");
            return;
        }
        TransactionState tx = txOrNull(ctx);
        if (tx == null || !tx.active()) {
            out.error("ERR DISCARD without MULTI");
            return;
        }
        tx.discard();
        out.simpleString("OK");
    }

    private void exec(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 1) {
            CommandSupport.wrongArity(out, "exec");
            return;
        }
        TransactionState tx = txOrNull(ctx);
        if (tx == null || !tx.active()) {
            out.error("ERR EXEC without MULTI");
            return;
        }
        if (tx.aborted()) {
            tx.discard();
            out.error("EXECABORT Transaction discarded because of previous errors.");
            return;
        }

        List<byte[][]> queued = tx.drain();
        out.arrayHeader(queued.size());
        for (int i = 0; i < queued.size(); i++) {
            byte[][] argv = queued.get(i);
            try (Command q = new QueuedCommand(argv)) {
                processor.execute(q, ctx);
            }
        }
    }

    private TransactionState txOrNull(CommandContext ctx) {
        ServerSession s = ctx.serverSessionOrNull();
        if (s != null) {
            return s.transaction();
        }
        return null;
    }

    /**
     * Transaction-queued command: wraps argv bytes with stable lifetime and command API.
     */
    private static final class QueuedCommand implements Command {
        private final byte[][] argv;
        private final int retainedBytes;

        private QueuedCommand(byte[][] argv) {
            this.argv = Objects.requireNonNull(argv, "argv");
            int total = 0;
            for (int i = 0; i < argv.length; i++) {
                byte[] a = argv[i];
                if (a != null) {
                    total += a.length;
                }
            }
            this.retainedBytes = total;
        }

        @Override
        public int argc() {
            return argv.length;
        }

        @Override
        public boolean isNull(int index) {
            if (index < 0 || index >= argv.length) {
                throw new IndexOutOfBoundsException();
            }
            return argv[index] == null;
        }

        @Override
        public int len(int index) {
            if (index < 0 || index >= argv.length) {
                throw new IndexOutOfBoundsException();
            }
            byte[] a = argv[index];
            return a == null ? -1 : a.length;
        }

        @Override
        public byte byteAt(int index, int offset) {
            byte[] a = argv[index];
            if (a == null) {
                throw new IllegalStateException("arg is null");
            }
            return a[offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            byte[] a = argv[index];
            if (a == null) {
                throw new IllegalStateException("arg is null");
            }
            System.arraycopy(a, 0, dst, dstOff, a.length);
        }

        @Override
        public byte[] toByteArray(int index) {
            byte[] a = argv[index];
            if (a == null) {
                return null;
            }
            if (a.length == 0) {
                return new byte[0];
            }
            byte[] out = new byte[a.length];
            System.arraycopy(a, 0, out, 0, a.length);
            return out;
        }

        @Override
        public int retainedBytes() {
            return retainedBytes;
        }

        @Override
        public void close() {
            // no-op: argv is owned by the transaction queue and will be released on discard/drain
        }
    }
}
