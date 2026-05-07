package yier.bubu.redis.testutil;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ServerSession;
import yier.bubu.redis.execution.api.TransactionState;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/**
 * 测试辅助：以协议无关的 {@link Command}/{@link ReplyWriter} 语义执行命令，并捕获 reply 供断言使用。
 * <p>
 * 说明：对外协议已切换为 Custom Protocol v1，因此 core 单测不再依赖任何 wire codec/对象模型。
 */
public final class FastTestClient implements AutoCloseable {
    private final YierdisFastCommandProcessor processor;
    private final ServerSession session;

    public FastTestClient(YierdisFastCommandProcessor processor) {
        this(processor, null);
    }

    public FastTestClient(YierdisFastCommandProcessor processor, ServerSession session) {
        Objects.requireNonNull(processor, "processor");
        this.processor = processor;
        this.session = session != null ? session : new DefaultTestSession();
    }

    public ReplyObject execute(List<byte[]> args) {
        Objects.requireNonNull(args, "args");
        return execute(ByteArrayExecutionRequest.copyOf(args));
    }

    public ReplyObject execute(ExecutionRequest request) {
        Objects.requireNonNull(request, "request");
        CapturingReplyWriter writer = new CapturingReplyWriter();
        try {
            processor.execute(request, new CommandContext(session, writer));
            return writer.root();
        } finally {
            request.close();
        }
    }

    @Override
    public void close() {
        // no-op
    }

    private static final class CapturingReplyWriter implements ReplyWriter {
        private boolean closeAfterReplyRequested;

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
        public void requestCloseAfterReply() {
            closeAfterReplyRequested = true;
        }

        @Override
        public boolean closeAfterReplyRequested() {
            return closeAfterReplyRequested;
        }

        @Override
        public void protocolError(String message) {
            addValue(new ReplyError(ReplyError.Kind.PROTOCOL, message == null ? "ERR error" : message));
        }

        @Override
        public void simpleString(String value) {
            addValue(new ReplySimpleString(value == null ? "" : value));
        }

        @Override
        public void error(String message) {
            addValue(new ReplyError(ReplyError.Kind.COMMAND, message == null ? "ERR error" : message));
        }

        @Override
        public void integer(long value) {
            addValue(new ReplyInteger(value));
        }

        @Override
        public void booleanValue(boolean value) {
            addValue(new ReplyInteger(value ? 1L : 0L));
        }

        @Override
        public void doubleValue(double value) {
            addValue(new ReplySimpleString(Double.toString(value)));
        }

        @Override
        public void bigNumberAscii(String value) {
            addValue(new ReplySimpleString(value == null ? "" : value));
        }

        @Override
        public void verbatimString(String format, byte[] data) {
            String prefix = format == null ? "" : format.trim();
            String payload = data == null ? "" : new String(data, java.nio.charset.StandardCharsets.UTF_8);
            addValue(new ReplySimpleString(prefix.isEmpty() ? payload : (prefix + ":" + payload)));
        }

        @Override
        public void blobError(String message) {
            addValue(new ReplyError(ReplyError.Kind.COMMAND, message == null ? "ERR error" : message));
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
        public void bulkStringArray(List<byte[]> values) {
            if (values == null) {
                nullArray();
                return;
            }
            arrayHeader(values.size());
            for (int i = 0; i < values.size(); i++) {
                bulkString(values.get(i));
            }
        }

        @Override
        public void emptyArray() {
            arrayHeader(0);
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

        @Override
        public void pushHeader(int count) {
            arrayHeader(count);
        }

        @Override
        public void attributeHeader(int pairs) {
            mapHeader(pairs);
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
                    // Should not happen, but keep it robust.
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

    private static final class DefaultTestSession implements ServerSession {
        private int dbIndex;
        private String clientName;
        private boolean authenticated;
        private final TransactionState tx = new DefaultTransactionState();

        @Override
        public int dbIndex() {
            return dbIndex;
        }

        @Override
        public void setDbIndex(int dbIndex) {
            this.dbIndex = Math.max(0, dbIndex);
        }

        @Override
        public long clientId() {
            return 1L;
        }

        @Override
        public String clientName() {
            return clientName;
        }

        @Override
        public void setClientName(String clientName) {
            this.clientName = clientName;
        }

        @Override
        public boolean authenticated() {
            return authenticated;
        }

        @Override
        public void setAuthenticated(boolean authenticated) {
            this.authenticated = authenticated;
        }

        @Override
        public TransactionState transaction() {
            return tx;
        }

        @Override
        public yier.bubu.redis.execution.api.ConnectionStatsView connectionStats() {
            return null;
        }
    }

    private static final class DefaultTransactionState implements TransactionState {
        private boolean active;
        private boolean aborted;
        private final ArrayList<ExecutionRequest> queue = new ArrayList<>();

        @Override
        public synchronized boolean active() {
            return active;
        }

        @Override
        public synchronized void begin() {
            active = true;
            aborted = false;
            queue.clear();
        }

        @Override
        public synchronized void discard() {
            active = false;
            aborted = false;
            queue.clear();
        }

        @Override
        public synchronized void enqueue(ExecutionRequest request) {
            if (request == null) {
                return;
            }
            queue.add(ByteArrayExecutionRequest.copyOf(request));
        }

        @Override
        public synchronized boolean aborted() {
            return aborted;
        }

        @Override
        public synchronized void markAborted() {
            aborted = true;
        }

        @Override
        public synchronized int size() {
            return queue.size();
        }

        @Override
        public synchronized List<ExecutionRequest> drain() {
            ArrayList<ExecutionRequest> out = new ArrayList<>(queue);
            queue.clear();
            active = false;
            aborted = false;
            return out;
        }
    }
}
