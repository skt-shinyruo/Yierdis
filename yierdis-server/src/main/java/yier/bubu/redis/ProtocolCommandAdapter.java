package yier.bubu.redis;

// 协议请求到执行命令的适配器：保持 Custom Protocol v1 的 UTF-8/null argv 语义，同时把协议层与 core-contract 解耦。

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.protocol.v1.CustomProtocolV1Request;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

final class ProtocolCommandAdapter extends SimpleChannelInboundHandler<CustomProtocolV1Request> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CustomProtocolV1Request msg) {
        if (ctx == null || msg == null) {
            return;
        }
        ctx.fireChannelRead(new AdaptedCommand(msg.cmd(), msg.args()));
    }

    private static final class AdaptedCommand implements Command {
        private final byte[][] argv;
        private final int retainedBytes;

        private AdaptedCommand(String cmd, List<String> args) {
            String name = Objects.requireNonNull(cmd, "cmd").trim();
            if (name.isEmpty()) {
                throw new IllegalArgumentException("cmd must not be blank");
            }

            int argc = 1 + (args == null ? 0 : args.size());
            byte[][] argv = new byte[argc][];
            argv[0] = name.getBytes(StandardCharsets.UTF_8);

            int total = argv[0].length;
            if (args != null && !args.isEmpty()) {
                for (int i = 0; i < args.size(); i++) {
                    String arg = args.get(i);
                    if (arg == null) {
                        argv[i + 1] = null;
                        continue;
                    }
                    byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
                    argv[i + 1] = bytes;
                    total += bytes.length;
                }
            }

            this.argv = argv;
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
            byte[] arg = argv[index];
            return arg == null ? -1 : arg.length;
        }

        @Override
        public byte byteAt(int index, int offset) {
            byte[] arg = argv[index];
            if (arg == null) {
                throw new IllegalStateException("arg is null");
            }
            if (offset < 0 || offset >= arg.length) {
                throw new IndexOutOfBoundsException();
            }
            return arg[offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            if (dst == null) {
                throw new IllegalArgumentException("dst must not be null");
            }
            byte[] arg = argv[index];
            if (arg == null) {
                throw new IllegalStateException("arg is null");
            }
            if (dstOff < 0 || dstOff + arg.length > dst.length) {
                throw new IndexOutOfBoundsException();
            }
            if (arg.length == 0) {
                return;
            }
            System.arraycopy(arg, 0, dst, dstOff, arg.length);
        }

        @Override
        public byte[] toByteArray(int index) {
            if (index < 0 || index >= argv.length) {
                throw new IndexOutOfBoundsException();
            }
            return argv[index];
        }

        @Override
        public int retainedBytes() {
            return retainedBytes;
        }

        @Override
        public void close() {
            // no-op (heap arrays will be GC'ed)
        }
    }
}
