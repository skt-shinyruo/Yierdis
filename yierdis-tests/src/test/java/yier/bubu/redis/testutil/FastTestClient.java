package yier.bubu.redis.testutil;

import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyRenderer;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.execution.engine.EngineSession;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/**
 * 测试辅助：直接执行 {@link PreparedCommand}，再通过语义 renderer 捕获 {@link CommandResult} 供断言使用。
 * <p>
 * 该路径不经过执行器的容量预留和网络协议编码，因此只用于验证命令语义。
 */
public final class FastTestClient {
    private final CommandDispatcher dispatcher;
    private final CommandSession session;

    public FastTestClient(CommandDispatcher dispatcher) {
        this(dispatcher, new EngineSession(0, 0));
    }

    public FastTestClient(CommandDispatcher dispatcher, CommandSession session) {
        Objects.requireNonNull(dispatcher, "dispatcher");
        this.dispatcher = dispatcher;
        this.session = Objects.requireNonNull(session, "session");
    }

    public ReplyObject execute(List<byte[]> args) {
        Objects.requireNonNull(args, "args");
        return execute(ByteArrayExecutionRequest.copyOf(args));
    }

    public ReplyObject execute(ExecutionRequest request) {
        Objects.requireNonNull(request, "request");
        CapturingReplyWriter writer = new CapturingReplyWriter();
        try {
            for (;;) {
                try (PreparedCommand prepared = dispatcher.prepare(session, request)) {
                    if (prepared.validateBeforeExecute() == ValidationResult.STALE) {
                        continue;
                    }
                    CommandResult result = prepared.execute(session);
                    RedisReplyRenderer.render(result.reply(), writer);
                    return writer.root();
                }
            }
        } finally {
            request.close();
        }
    }

    private static final class CapturingReplyWriter implements RedisReplyWriter {
        private ReplyObject root;
        private final Deque<Container> stack = new ArrayDeque<>(8);

        private enum ContainerType {
            ARRAY,
            MAP
        }

        private static final class Container {
            final ContainerType type;
            int remaining;
            final ArrayList<ReplyObject> arrayValues;
            final ArrayList<ReplyMap.Entry> mapEntries;
            ReplyObject pendingKey;

            private Container(ContainerType type, int remaining) {
                this.type = type;
                this.remaining = remaining;
                this.arrayValues = type == ContainerType.ARRAY ? new ArrayList<>(Math.min(remaining, 16)) : null;
                this.mapEntries = type == ContainerType.MAP ? new ArrayList<>(Math.min(remaining, 16)) : null;
            }
        }

        private CapturingReplyWriter() {}

        ReplyObject root() {
            if (root == null) {
                throw new AssertionError("expected a reply");
            }
            if (!stack.isEmpty()) {
                throw new AssertionError("reply container not finished");
            }
            return root;
        }

        @Override
        public void simpleString(String value) {
            addValue(new ReplySimpleString(value == null ? "" : value));
        }

        @Override
        public void error(String message) {
            addValue(new ReplyError(message == null ? "ERR error" : message));
        }

        @Override
        public void integer(long value) {
            addValue(new ReplyInteger(value));
        }

        @Override
        public void bulkString(byte[] data) {
            if (data == null) {
                nullValue();
                return;
            }
            addValue(new ReplyBulkString(data));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            if (data == null) {
                nullValue();
                return;
            }
            byte[] slice = new byte[Math.max(0, len)];
            if (len > 0) {
                System.arraycopy(data, off, slice, 0, len);
            }
            addValue(new ReplyBulkString(slice));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                nullValue();
                return;
            }
            int len = Math.max(0, slice.length());
            byte[] data = new byte[len];
            if (len > 0) {
                slice.getBytes(0, data, 0, len);
            }
            addValue(new ReplyBulkString(data));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            bulkString(Long.toString(value).getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }

        @Override
        public void nullValue() {
            addValue(ReplyNull.INSTANCE);
        }

        @Override
        public void nullArray() {
            addValue(ReplyNullArray.INSTANCE);
        }

        @Override
        public void arrayHeader(int count) {
            int n = Math.max(0, count);
            if (n == 0) {
                addValue(new ReplyArray(List.of()));
                return;
            }
            stack.push(new Container(ContainerType.ARRAY, n));
        }

        @Override
        public void mapHeader(int pairs) {
            int n = Math.max(0, pairs);
            if (n == 0) {
                addValue(new ReplyMap(List.of()));
                return;
            }
            stack.push(new Container(ContainerType.MAP, n));
        }

        @Override
        public void setHeader(int count) {
            arrayHeader(count);
        }

        private void addValue(ReplyObject value) {
            Objects.requireNonNull(value, "value");
            if (stack.isEmpty()) {
                if (root != null) {
                    throw new IllegalStateException("reply already finished");
                }
                root = value;
                return;
            }

            for (; ; ) {
                Container c = stack.peek();
                if (c == null) {
                    // 防御异常的容器状态，避免后续回复悄悄覆盖已经捕获的根节点。
                    if (root != null) {
                        throw new IllegalStateException("reply already finished");
                    }
                    root = value;
                    return;
                }

                if (c.type == ContainerType.ARRAY) {
                    if (c.remaining <= 0) {
                        throw new IllegalStateException("too many array elements");
                    }
                    c.arrayValues.add(value);
                    c.remaining--;
                    if (c.remaining > 0) {
                        return;
                    }
                    stack.pop();
                    value = new ReplyArray(c.arrayValues);
                    if (stack.isEmpty()) {
                        root = value;
                        return;
                    }
                    continue;
                }

                if (c.type == ContainerType.MAP) {
                    if (c.remaining <= 0) {
                        throw new IllegalStateException("too many map entries");
                    }
                    if (c.pendingKey == null) {
                        c.pendingKey = value;
                        return;
                    }
                    c.mapEntries.add(new ReplyMap.Entry(c.pendingKey, value));
                    c.pendingKey = null;
                    c.remaining--;
                    if (c.remaining > 0) {
                        return;
                    }
                    stack.pop();
                    value = new ReplyMap(c.mapEntries);
                    if (stack.isEmpty()) {
                        root = value;
                        return;
                    }
                    continue;
                }

                throw new IllegalStateException("unknown container type");
            }
        }
    }

}
