package yier.bubu.redis.command;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespCommandBuilder;
import yier.bubu.redis.protocol.RespFrame;
import yier.bubu.redis.protocol.RespServerSession;
import yier.bubu.redis.protocol.RespTransactionState;
import yier.bubu.redis.protocol.RespWriter;

import java.util.List;
import java.util.Objects;

/**
 * Redis 事务命令（最小实现）：MULTI/EXEC/DISCARD。
 * <p>
 * 设计约束：
 * - 连接级状态通过 {@link RespServerSession} 暴露，避免 core 依赖 server/Netty
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

    private void multi(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 1) {
            CommandSupport.wrongArity(out, "multi");
            return;
        }
        RespTransactionState tx = txOrNull(out);
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

    private void discard(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 1) {
            CommandSupport.wrongArity(out, "discard");
            return;
        }
        RespTransactionState tx = txOrNull(out);
        if (tx == null || !tx.active()) {
            out.error("ERR DISCARD without MULTI");
            return;
        }
        tx.discard();
        out.simpleString("OK");
    }

    private void exec(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 1) {
            CommandSupport.wrongArity(out, "exec");
            return;
        }
        RespTransactionState tx = txOrNull(out);
        if (tx == null || !tx.active()) {
            out.error("ERR EXEC without MULTI");
            return;
        }

        List<byte[][]> queued = tx.drain();
        out.arrayHeader(queued.size());
        for (int i = 0; i < queued.size(); i++) {
            byte[][] argv = queued.get(i);
            try (RespCommand q = buildCommand(argv)) {
                processor.execute(q, out);
            }
        }
    }

    private RespTransactionState txOrNull(RespWriter out) {
        if (out == null) {
            return null;
        }
        if (out.session() instanceof RespServerSession s) {
            return s.transaction();
        }
        return null;
    }

    private static RespCommand buildCommand(byte[][] argv) {
        if (argv == null) {
            throw new IllegalArgumentException("argv must not be null");
        }

        int argc = argv.length;
        RespCommand cmd = RespCommandBuilder.acquire(argc);

        int total = 0;
        for (int i = 0; i < argc; i++) {
            byte[] arg = argv[i];
            if (arg != null) {
                total += arg.length;
            }
        }

        byte[] data = new byte[total];
        RespCommandBuilder.setFrame(cmd, new HeapFrame(data));

        int off = 0;
        for (int i = 0; i < argc; i++) {
            byte[] arg = argv[i];
            if (arg == null) {
                RespCommandBuilder.setArgNull(cmd, i);
                continue;
            }
            int len = arg.length;
            if (len > 0) {
                System.arraycopy(arg, 0, data, off, len);
            }
            RespCommandBuilder.setArgSlice(cmd, i, off, len);
            off += len;
        }
        return cmd;
    }

    /**
     * 事务队列里的命令帧：将 argv 拼成一个连续 heap byte[]，避免依赖 Netty ByteBuf 生命周期。
     */
    private static final class HeapFrame implements RespFrame {
        private final byte[] data;

        private HeapFrame(byte[] data) {
            this.data = Objects.requireNonNull(data, "data");
        }

        @Override
        public int length() {
            return data.length;
        }

        @Override
        public byte getByte(int index) {
            return data[index];
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            System.arraycopy(data, index, dst, dstOff, len);
        }

        @Override
        public void close() {
            // no-op (heap byte[] will be GC'ed)
        }
    }
}
