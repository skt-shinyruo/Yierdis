package yier.bubu.redis.command.kernel;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.CommandSessionCapabilities;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyPlans;
import yier.bubu.redis.execution.api.TransactionState;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class YierdisFastCommandProcessorPolicyTest {
    @Test
    public void unknownCommandInMultiMarksTransactionAbortedAndExecDiscardsQueue() {
        YierdisFastCommandProcessor processor = processorWithTransactions(
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
        YierdisFastCommandProcessor processor = processorWithTransactions(
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
    public void transactionControlParseErrorsAbortActiveTransaction() {
        for (String control : List.of("MULTI", "EXEC", "DISCARD")) {
            YierdisFastCommandProcessor processor = processorWithTransactions(registration -> { });
            TestSession session = new TestSession();
            CapturingReplyWriter out = new CapturingReplyWriter();
            CommandContext ctx = context(session, out);

            processor.execute(request("MULTI"), ctx);
            out.clear();
            processor.execute(request(control, "extra"), ctx);

            Assert.assertEquals(
                    "ERR wrong number of arguments for '" + control.toLowerCase(java.util.Locale.ROOT) + "' command",
                    out.error()
            );
            Assert.assertTrue(control, session.transactionState().aborted());

            out.clear();
            processor.execute(request("EXEC"), ctx);
            Assert.assertEquals(
                    control,
                    "EXECABORT Transaction discarded because of previous errors.",
                    out.error()
            );
        }
    }

    @Test
    public void nestedMultiHandlerErrorDoesNotAbortActiveTransaction() {
        YierdisFastCommandProcessor processor = processorWithTransactions(registration -> { });
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        CommandContext ctx = context(session, out);

        processor.execute(request("MULTI"), ctx);
        out.clear();
        processor.execute(request("MULTI"), ctx);

        Assert.assertEquals("ERR MULTI calls can not be nested", out.error());
        Assert.assertFalse(session.transactionState().aborted());

        out.clear();
        processor.execute(request("EXEC"), ctx);
        Assert.assertEquals(Integer.valueOf(0), out.arrayHeader());
    }

    @Test
    public void transactionCommandsReplayQueuedRequestsThroughNarrowReplayer() {
        ArrayList<String> replayed = new ArrayList<>();
        CommandRegistry registry = CommandRegistries.from(
                new TransactionCommands((request, ctx) -> {
                    replayed.add(arg(request, 0));
                    ctx.out().simpleString("REPLAY_" + arg(request, 0));
                }),
                registration -> registration.register(
                        "WRITE",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "write"),
                        (request, ctx) -> ctx.out().simpleString("WRITE")
                )
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        CommandContext ctx = context(session, out);

        processor.execute(request("MULTI"), ctx);
        out.clear();
        processor.execute(request("WRITE"), ctx);
        Assert.assertEquals("QUEUED", out.simpleString());

        out.clear();
        processor.execute(request("EXEC"), ctx);

        Assert.assertEquals(Integer.valueOf(1), out.arrayHeader());
        Assert.assertEquals(List.of("WRITE"), replayed);
        Assert.assertEquals("REPLAY_WRITE", out.simpleString());
    }

    @Test
    public void execClosesCurrentAndQueuedTailExactlyOnceWhenReplayFails() {
        AtomicReference<MutationContext> replayContext = new AtomicReference<>();
        CommandRegistry registry = CommandRegistries.from(
                new TransactionCommands((request, ctx) -> {
                    replayContext.set(ctx.mutationContext());
                    throw new IllegalStateException("injected replay failure");
                })
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        TrackingExecutionRequest first = new TrackingExecutionRequest(request("FIRST"));
        TrackingExecutionRequest second = new TrackingExecutionRequest(request("SECOND"));
        session.transactionState().begin();
        session.transactionState().enqueueOwned(first);
        session.transactionState().enqueueOwned(second);

        IllegalStateException failure = Assert.assertThrows(
                IllegalStateException.class,
                () -> processor.execute(request("EXEC"), context(session, out))
        );

        Assert.assertEquals("injected replay failure", failure.getMessage());
        Assert.assertEquals(1, first.closeCalls());
        Assert.assertEquals(1, second.closeCalls());
        Assert.assertNotNull(replayContext.get());
        Assert.assertFalse(replayContext.get().hasCommandRecord());
    }

    @Test
    public void execAggregatesRequestOnlyReplyPlansBeforeReplayingCommands() {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor::execute);
        registry.register(
                "PLANNED",
                CommandSpec.of(
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "planned"),
                        (request, ctx) -> ctx.out().simpleString("OK")
                ).withReplyPlanner(request -> ReplyPlan.exact(5L, 3L))
        );
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        CommandContext ctx = context(session, out);

        processor.execute(request("MULTI"), ctx);
        processor.execute(request("PLANNED"), ctx);
        out.clear();
        processor.execute(request("EXEC"), ctx);

        Assert.assertEquals(ReplyPlan.exact(9L, 3L), out.envelopePlan());
        Assert.assertEquals(Integer.valueOf(1), out.arrayHeader());
        Assert.assertEquals("OK", out.simpleString());
    }

    @Test
    public void execFallsBackToMaximumWhenAnyQueuedCommandHasNoPlanner() {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor::execute);
        registry.register(
                "UNPLANNED",
                CommandDescriptor.of(1, 0, 0, 0),
                CommandParsers.exactRequest(1, "unplanned"),
                (request, ctx) -> ctx.out().simpleString("OK")
        );
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        CommandContext ctx = context(session, out);

        processor.execute(request("MULTI"), ctx);
        processor.execute(request("UNPLANNED"), ctx);
        out.clear();
        processor.execute(request("EXEC"), ctx);

        Assert.assertEquals(ReplyPlan.maximum(), out.envelopePlan());
    }

    private static YierdisFastCommandProcessor processorWithTransactions(CommandModule module) {
        YierdisFastCommandProcessor[] self = new YierdisFastCommandProcessor[1];
        CommandRegistry registry = CommandRegistries.from(
                new TransactionCommands((request, ctx) -> self[0].execute(request, ctx)),
                module
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        self[0] = processor;
        return processor;
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
        public ReplyPlan planExecReply(Function<? super ExecutionRequest, ReplyPlan> planner) {
            long encodedElementBytes = 0L;
            long retainedSourceBytes = 0L;
            for (ExecutionRequest request : queue) {
                ReplyPlan child = planner.apply(request);
                if (child == null || child.reserveMaximum()) {
                    return ReplyPlan.maximum();
                }
                encodedElementBytes = addSaturating(encodedElementBytes, child.encodedUpperBoundBytes());
                retainedSourceBytes = addSaturating(retainedSourceBytes, child.retainedSourceBytes());
            }
            return ReplyPlans.array(queue.size(), encodedElementBytes, retainedSourceBytes);
        }

        @Override
        public List<ExecutionRequest> drain() {
            ArrayList<ExecutionRequest> out = new ArrayList<>(queue);
            queue.clear();
            active = false;
            aborted = false;
            return out;
        }

        private void enqueueOwned(ExecutionRequest request) {
            queue.add(request);
        }

        private static long addSaturating(long left, long right) {
            return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
        }
    }

    private static final class TrackingExecutionRequest implements ExecutionRequest {
        private final ExecutionRequest delegate;
        private int closeCalls;

        private TrackingExecutionRequest(ExecutionRequest delegate) {
            this.delegate = delegate;
        }

        @Override
        public int argc() {
            return delegate.argc();
        }

        @Override
        public boolean isNull(int index) {
            return delegate.isNull(index);
        }

        @Override
        public int len(int index) {
            return delegate.len(index);
        }

        @Override
        public byte byteAt(int index, int offset) {
            return delegate.byteAt(index, offset);
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            delegate.copyToByteArray(index, dst, dstOff);
        }

        @Override
        public byte[] toByteArray(int index) {
            return delegate.toByteArray(index);
        }

        @Override
        public void close() {
            closeCalls++;
            delegate.close();
        }

        private int closeCalls() {
            return closeCalls;
        }
    }

    private static final class CapturingReplyWriter implements RedisReplyWriter {
        private String simpleString;
        private String error;
        private Integer arrayHeader;
        private ReplyPlan envelopePlan;

        private void clear() {
            simpleString = null;
            error = null;
            arrayHeader = null;
            envelopePlan = null;
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

        private ReplyPlan envelopePlan() {
            return envelopePlan;
        }

        @Override
        public void requireReplyEnvelope(ReplyPlan plan) {
            envelopePlan = plan;
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
