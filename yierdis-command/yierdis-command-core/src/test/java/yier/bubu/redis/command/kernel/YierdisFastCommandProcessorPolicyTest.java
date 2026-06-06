package yier.bubu.redis.command.kernel;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.CommandSessionCapabilities;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.TransactionState;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class YierdisFastCommandProcessorPolicyTest {
    @Test
    public void unknownCommandInMultiMarksTransactionAbortedAndExecDiscardsQueue() {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                registration -> registration.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("LOCAL")
                )
        );
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        CommandContext ctx = context(session, out);

        processor.execute(request("MULTI"), ctx);
        Assert.assertEquals("OK", out.simpleString());

        out.clear();
        processor.execute(request("NO_SUCH_COMMAND"), ctx);
        Assert.assertEquals("ERR unknown command 'NO_SUCH_COMMAND'", out.error());
        Assert.assertTrue(session.transactionState().aborted());
        Assert.assertEquals(0, session.transactionState().size());

        out.clear();
        processor.execute(request("EXEC"), ctx);
        Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", out.error());
        Assert.assertFalse(session.transactionState().active());
    }

    @Test
    public void disallowedInMultiMarksTransactionAbortedWithoutCallingHandler() {
        boolean[] handlerCalled = {false};
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                registration -> registration.register(
                        "FORBIDDEN",
                        CommandSpec.disallowedInMulti(
                                CommandDescriptor.of(1, 0, 0, 0),
                                CommandParsers.exactRequest(1, "forbidden"),
                                (request, ctx) -> {
                                    handlerCalled[0] = true;
                                    ctx.out().simpleString("FORBIDDEN");
                                },
                                "ERR FORBIDDEN is not allowed in MULTI"
                        )
                )
        );
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        CommandContext ctx = context(session, out);

        processor.execute(request("MULTI"), ctx);
        out.clear();
        processor.execute(request("FORBIDDEN"), ctx);

        Assert.assertEquals("ERR FORBIDDEN is not allowed in MULTI", out.error());
        Assert.assertFalse(handlerCalled[0]);
        Assert.assertTrue(session.transactionState().aborted());

        out.clear();
        processor.execute(request("EXEC"), ctx);
        Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", out.error());
    }

    @Test
    public void changeObserverReceivesUserCommandEventOnlyAfterMutationOutcome() {
        ArrayList<String> events = new ArrayList<>();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                YierdisCommandProcessorOptions.builder()
                        .changeObserver((dbIndex, request) -> events.add(dbIndex + ":" + arg(request, 0)))
                        .build(),
                registration -> {
                    registration.register(
                            "READONLY",
                            CommandDescriptor.of(1, 0, 0, 0),
                            CommandParsers.exactRequest(1, "readonly"),
                            (request, ctx) -> ctx.out().simpleString("READONLY")
                    );
                    registration.register(
                            "MUTATE",
                            CommandDescriptor.of(1, 0, 0, 0),
                            CommandParsers.exactRequest(1, "mutate"),
                            (request, ctx) -> {
                                ctx.recordMutation(true, false);
                                ctx.out().simpleString("MUTATE");
                            }
                    );
                }
        );
        TestSession session = new TestSession();
        session.setDbIndex(2);
        CapturingReplyWriter out = new CapturingReplyWriter();
        CommandContext ctx = context(session, out);

        processor.execute(request("READONLY"), ctx);
        Assert.assertEquals(0, events.size());

        out.clear();
        processor.execute(request("MUTATE"), ctx);
        Assert.assertEquals("MUTATE", out.simpleString());
        Assert.assertEquals(1, events.size());
        Assert.assertEquals("2:MUTATE", events.get(0));

        out.clear();
        processor.execute(request("READONLY"), ctx);
        Assert.assertEquals("READONLY", out.simpleString());
        Assert.assertEquals(1, events.size());
    }

    @Test
    public void execReplayEmitsQueuedMutationWithoutEmittingExec() {
        ArrayList<String> events = new ArrayList<>();
        YierdisFastCommandProcessor processor = mutationAwareProcessor(events);
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        CommandContext ctx = context(session, out);

        processor.execute(request("MULTI"), ctx);

        out.clear();
        processor.execute(request("READONLY"), ctx);
        Assert.assertEquals("QUEUED", out.simpleString());

        out.clear();
        processor.execute(request("MUTATE"), ctx);
        Assert.assertEquals("QUEUED", out.simpleString());

        out.clear();
        processor.execute(request("EXEC"), ctx);

        Assert.assertEquals(Integer.valueOf(2), out.arrayHeader());
        Assert.assertEquals("MUTATE", out.simpleString());
        Assert.assertEquals(1, events.size());
        Assert.assertEquals("0:MUTATE", events.get(0));
    }

    @Test
    public void execReplayDoesNotCarryMutationOutcomeIntoFollowingReadOnlyCommand() {
        ArrayList<String> events = new ArrayList<>();
        YierdisFastCommandProcessor processor = mutationAwareProcessor(events);
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        CommandContext ctx = context(session, out);

        processor.execute(request("MULTI"), ctx);

        out.clear();
        processor.execute(request("MUTATE"), ctx);
        Assert.assertEquals("QUEUED", out.simpleString());

        out.clear();
        processor.execute(request("READONLY"), ctx);
        Assert.assertEquals("QUEUED", out.simpleString());

        out.clear();
        processor.execute(request("EXEC"), ctx);

        Assert.assertEquals(Integer.valueOf(2), out.arrayHeader());
        Assert.assertEquals("READONLY", out.simpleString());
        Assert.assertEquals(1, events.size());
        Assert.assertEquals("0:MUTATE", events.get(0));
    }

    private static YierdisFastCommandProcessor mutationAwareProcessor(ArrayList<String> events) {
        return new YierdisFastCommandProcessor(
                YierdisCommandProcessorOptions.builder()
                        .changeObserver((dbIndex, request) -> events.add(dbIndex + ":" + arg(request, 0)))
                        .build(),
                registration -> {
                    registration.register(
                            "READONLY",
                            CommandDescriptor.of(1, 0, 0, 0),
                            CommandParsers.exactRequest(1, "readonly"),
                            (request, ctx) -> ctx.out().simpleString("READONLY")
                    );
                    registration.register(
                            "MUTATE",
                            CommandDescriptor.of(1, 0, 0, 0),
                            CommandParsers.exactRequest(1, "mutate"),
                            (request, ctx) -> {
                                ctx.recordMutation(true, false);
                                ctx.out().simpleString("MUTATE");
                            }
                    );
                }
        );
    }

    private static ExecutionRequest request(String command, String... args) {
        return ByteArrayExecutionRequest.fromUtf8(command, List.of(args));
    }

    private static String arg(ExecutionRequest request, int index) {
        byte[] bytes = request.toByteArray(index);
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    private static CommandContext context(TestSession session, CapturingReplyWriter out) {
        return new CommandContext(CommandSessionCapabilities.of(session, session, session, session, session), out);
    }

    private static final class TestSession implements
            yier.bubu.redis.execution.api.DbIndexSession,
            yier.bubu.redis.execution.api.ClientMetadataSession,
            yier.bubu.redis.execution.api.TransactionSession,
            yier.bubu.redis.execution.api.ConnectionStatsSession,
            yier.bubu.redis.execution.api.ProtocolNegotiationSession {
        private final TestTransactionState tx = new TestTransactionState();
        private int dbIndex;

        @Override
        public int dbIndex() {
            return dbIndex;
        }

        @Override
        public void setDbIndex(int dbIndex) {
            this.dbIndex = dbIndex;
        }

        @Override
        public long clientId() {
            return 1L;
        }

        @Override
        public String clientName() {
            return null;
        }

        @Override
        public void setClientName(String clientName) {
        }

        @Override
        public boolean authenticated() {
            return false;
        }

        @Override
        public void setAuthenticated(boolean authenticated) {
        }

        @Override
        public TransactionState transaction() {
            return tx;
        }

        @Override
        public ConnectionStatsView connectionStats() {
            return null;
        }

        private TestTransactionState transactionState() {
            return tx;
        }
    }

    private static final class TestTransactionState implements TransactionState {
        private boolean active;
        private boolean aborted;
        private final ArrayList<ExecutionRequest> queue = new ArrayList<>();

        @Override
        public boolean active() {
            return active;
        }

        @Override
        public void begin() {
            active = true;
            aborted = false;
            queue.clear();
        }

        @Override
        public void discard() {
            active = false;
            aborted = false;
            queue.clear();
        }

        @Override
        public void enqueue(ExecutionRequest request) {
            if (request != null) {
                queue.add(ByteArrayExecutionRequest.copyOf(request));
            }
        }

        @Override
        public boolean aborted() {
            return aborted;
        }

        @Override
        public void markAborted() {
            aborted = true;
        }

        @Override
        public int size() {
            return queue.size();
        }

        @Override
        public List<ExecutionRequest> drain() {
            ArrayList<ExecutionRequest> out = new ArrayList<>(queue);
            queue.clear();
            active = false;
            aborted = false;
            return out;
        }
    }

    private static final class CapturingReplyWriter implements RedisReplyWriter {
        private String simpleString;
        private String error;
        private Integer arrayHeader;

        private void clear() {
            simpleString = null;
            error = null;
            arrayHeader = null;
        }

        private String simpleString() {
            return simpleString;
        }

        private String error() {
            return error;
        }

        private Integer arrayHeader() {
            return arrayHeader;
        }

        @Override
        public void requestCloseAfterReply() {
        }

        @Override
        public boolean closeAfterReplyRequested() {
            return false;
        }

        @Override
        public void simpleString(String value) {
            simpleString = value;
        }

        @Override
        public void error(String message) {
            error = message;
        }

        @Override
        public void integer(long value) {
            throw unsupported();
        }

        @Override
        public void booleanValue(boolean value) {
            throw unsupported();
        }

        @Override
        public void doubleValue(double value) {
            throw unsupported();
        }

        @Override
        public void bigNumberAscii(String value) {
            throw unsupported();
        }

        @Override
        public void verbatimString(String format, byte[] data) {
            throw unsupported();
        }

        @Override
        public void blobError(String message) {
            throw unsupported();
        }

        @Override
        public void nullValue() {
            throw unsupported();
        }

        @Override
        public void nullArray() {
            throw unsupported();
        }

        @Override
        public void arrayHeader(int count) {
            arrayHeader = count;
        }

        @Override
        public void bulkStringArray(List<byte[]> values) {
            throw unsupported();
        }

        @Override
        public void emptyArray() {
            arrayHeader = 0;
        }

        @Override
        public void mapHeader(int pairs) {
            throw unsupported();
        }

        @Override
        public void setHeader(int count) {
            throw unsupported();
        }

        @Override
        public void pushHeader(int count) {
            throw unsupported();
        }

        @Override
        public void attributeHeader(int pairs) {
            throw unsupported();
        }

        @Override
        public void bulkString(byte[] data) {
            throw unsupported();
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            throw unsupported();
        }

        @Override
        public void bulkString(BytesSlice slice) {
            throw unsupported();
        }

        @Override
        public void bulkStringLongAscii(long value) {
            throw unsupported();
        }

        private UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("reply shape not used by this test");
        }
    }
}
