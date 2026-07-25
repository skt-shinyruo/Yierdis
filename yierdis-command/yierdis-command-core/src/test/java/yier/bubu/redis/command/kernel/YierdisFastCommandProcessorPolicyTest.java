package yier.bubu.redis.command.kernel;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.ValidationResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class YierdisFastCommandProcessorPolicyTest {
    @Test
    public void unknownCommandInMultiMarksTransactionAbortedAndExecDiscardsQueue() {
        YierdisFastCommandProcessor processor = processorWithTransactions(
                registration -> registration.register(new CommandDefinition<>(
                        syntax("LOCAL", CommandArity.exact(1)),
                        CommandParsers.request(),
                        (request, preparation) -> fixedReply("LOCAL")
                ))
        );
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();

        execute(processor, session, out, "MULTI");
        Assert.assertEquals("OK", out.simpleString());

        out.clear();
        execute(processor, session, out, "NO_SUCH_COMMAND");
        Assert.assertEquals("ERR unknown command 'NO_SUCH_COMMAND'", out.error());
        Assert.assertTrue(session.transactionState().aborted());
        Assert.assertEquals(0, session.transactionState().size());

        out.clear();
        execute(processor, session, out, "EXEC");
        Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", out.error());
        Assert.assertFalse(session.transactionState().active());
    }

    @Test
    public void disallowedInMultiMarksTransactionAbortedWithoutCallingHandler() {
        boolean[] handlerCalled = {false};
        YierdisFastCommandProcessor processor = processorWithTransactions(
                registration -> registration.register(new CommandDefinition<>(
                        syntax("FORBIDDEN", CommandArity.exact(1), TransactionPolicy.DISALLOWED_IN_MULTI),
                        CommandParsers.request(),
                        (request, preparation) -> {
                            handlerCalled[0] = true;
                            return fixedReply("FORBIDDEN");
                        }
                ))
        );
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();

        execute(processor, session, out, "MULTI");
        out.clear();
        execute(processor, session, out, "FORBIDDEN");

        Assert.assertEquals("ERR FORBIDDEN is not allowed in MULTI", out.error());
        Assert.assertFalse(handlerCalled[0]);
        Assert.assertTrue(session.transactionState().aborted());

        out.clear();
        execute(processor, session, out, "EXEC");
        Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", out.error());
    }

    @Test
    public void transactionControlParseErrorsAbortActiveTransaction() {
        for (String control : List.of("MULTI", "EXEC", "DISCARD")) {
            YierdisFastCommandProcessor processor = processorWithTransactions(registration -> { });
            TestSession session = new TestSession();
            CapturingReplyWriter out = new CapturingReplyWriter();

            execute(processor, session, out, "MULTI");
            out.clear();
            execute(processor, session, out, control, "extra");

            Assert.assertEquals(
                    "ERR wrong number of arguments for '" + control.toLowerCase(java.util.Locale.ROOT) + "' command",
                    out.error()
            );
            Assert.assertTrue(control, session.transactionState().aborted());

            out.clear();
            execute(processor, session, out, "EXEC");
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

        execute(processor, session, out, "MULTI");
        out.clear();
        execute(processor, session, out, "MULTI");

        Assert.assertEquals("ERR MULTI calls can not be nested", out.error());
        Assert.assertFalse(session.transactionState().aborted());

        out.clear();
        execute(processor, session, out, "EXEC");
        Assert.assertEquals(Integer.valueOf(0), out.arrayHeader());
    }

    @Test
    public void preparedExecReplaysQueuedRequestsInOrder() {
        ArrayList<String> replayed = new ArrayList<>();
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor);
        registry.register(new CommandDefinition<>(
                syntax("WRITE", CommandArity.exact(1)),
                CommandParsers.request(),
                (request, preparation) -> {
                    String command = arg(request, 0);
                    return PreparedCommands.fixed(
                            ReplyShapes.simpleString("REPLAY_" + command),
                            execution -> {
                                replayed.add(command);
                                execution.reply().simpleString("REPLAY_" + command);
                            }
                    );
                }
        ));
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();

        execute(processor, session, out, "MULTI");
        out.clear();
        execute(processor, session, out, "WRITE");
        Assert.assertEquals("QUEUED", out.simpleString());

        out.clear();
        execute(processor, session, out, "EXEC");

        Assert.assertEquals(Integer.valueOf(1), out.arrayHeader());
        Assert.assertEquals(List.of("WRITE"), replayed);
        Assert.assertEquals("REPLAY_WRITE", out.simpleString());
    }

    @Test
    public void execClosesCurrentAndQueuedTailExactlyOnceWhenAChildFails() {
        AtomicInteger executions = new AtomicInteger();
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor);
        registry.register(new CommandDefinition<>(
                syntax("FIRST", CommandArity.exact(1)),
                CommandParsers.request(),
                (request, preparation) -> PreparedCommands.fixed(
                        ReplyShapes.simpleString("OK"),
                        execution -> {
                            executions.incrementAndGet();
                            throw new IllegalStateException("injected replay failure");
                        }
                )
        ));
        registry.register(new CommandDefinition<>(
                syntax("SECOND", CommandArity.exact(1)),
                CommandParsers.request(),
                (request, preparation) -> fixedReply("SECOND")
        ));
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        TrackingExecutionRequest first = new TrackingExecutionRequest(request("FIRST"));
        TrackingExecutionRequest second = new TrackingExecutionRequest(request("SECOND"));
        session.transactionState().begin();
        session.transactionState().enqueueOwned(first);
        session.transactionState().enqueueOwned(second);

        ExecutionRequest execRequest = request("EXEC");
        try (execRequest;
             PreparedCommand prepared = processor.prepare(execRequest, new CommandPreparationContext(session));
             CommandExecutionContext execution = CommandExecutionContext.forRequest(session, out, execRequest)) {
            IllegalStateException failure = Assert.assertThrows(
                    IllegalStateException.class,
                    () -> prepared.execute(execution)
            );
            Assert.assertEquals("injected replay failure", failure.getMessage());
        }

        Assert.assertEquals(1, first.closeCalls());
        Assert.assertEquals(1, second.closeCalls());
        Assert.assertEquals(1, executions.get());
    }

    @Test
    public void execClosesEveryQueuedRequestBeforeReplayPreparationWhenArrayHeaderFails() {
        AtomicInteger firstChildCloses = new AtomicInteger();
        AtomicInteger secondChildCloses = new AtomicInteger();
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor);
        registry.register(new CommandDefinition<>(
                syntax("FIRST", CommandArity.exact(1)),
                CommandParsers.request(),
                (request, preparation) -> countingPrepared(firstChildCloses)
        ));
        registry.register(new CommandDefinition<>(
                syntax("SECOND", CommandArity.exact(1)),
                CommandParsers.request(),
                (request, preparation) -> countingPrepared(secondChildCloses)
        ));
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        TrackingExecutionRequest first = new TrackingExecutionRequest(request("FIRST"));
        TrackingExecutionRequest second = new TrackingExecutionRequest(request("SECOND"));
        session.transactionState().begin();
        session.transactionState().enqueueOwned(first);
        session.transactionState().enqueueOwned(second);
        out.failArrayHeader(new IllegalStateException("injected array header failure"));

        ExecutionRequest execRequest = request("EXEC");
        try (execRequest;
             PreparedCommand prepared = processor.prepare(execRequest, new CommandPreparationContext(session));
             CommandExecutionContext execution = CommandExecutionContext.forRequest(session, out, execRequest)) {
            IllegalStateException failure = Assert.assertThrows(
                    IllegalStateException.class,
                    () -> prepared.execute(execution)
            );
            Assert.assertEquals("injected array header failure", failure.getMessage());
            Assert.assertEquals(1, first.closeCalls());
            Assert.assertEquals(1, second.closeCalls());
            Assert.assertEquals(0, firstChildCloses.get());
            Assert.assertEquals(0, secondChildCloses.get());
        }
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return syntax(nameUpper, arity, TransactionPolicy.QUEUEABLE);
    }

    private static CommandSyntax syntax(
            String nameUpper,
            CommandArity arity,
            TransactionPolicy transactionPolicy
    ) {
        return new CommandSyntax(nameUpper, arity, CommandKeySpec.NONE, transactionPolicy);
    }

    private static PreparedCommand fixedReply(String value) {
        return PreparedCommands.fixed(
                ReplyShapes.simpleString(value),
                execution -> execution.reply().simpleString(value)
        );
    }

    private static PreparedCommand countingPrepared(AtomicInteger closes) {
        return new PreparedCommand() {
            @Override
            public ReplyShape replyShape() {
                return ReplyShapes.simpleString("OK");
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return ValidationResult.VALID;
            }

            @Override
            public void execute(CommandExecutionContext context) {
                context.reply().simpleString("OK");
            }

            @Override
            public void close() {
                closes.incrementAndGet();
            }
        };
    }

    private static YierdisFastCommandProcessor processorWithTransactions(CommandModule module) {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor);
        module.register(registry);
        return processor;
    }

    private static ExecutionRequest request(String command, String... args) {
        return ByteArrayExecutionRequest.fromUtf8(command, List.of(args));
    }

    private static String arg(ExecutionRequest request, int index) {
        byte[] bytes = request.toByteArray(index);
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    private static void execute(
            YierdisFastCommandProcessor processor,
            TestSession session,
            CapturingReplyWriter out,
            String command,
            String... args
    ) {
        PreparedCommandTestSupport.execute(processor, session, request(command, args), out);
    }

    private static final class TestSession implements CommandSession {
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

        @Override
        public int respVersion() {
            return 2;
        }

        @Override
        public void setRespVersion(int respVersion) {
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
            closeQueued();
            active = true;
            aborted = false;
        }

        @Override
        public void discard() {
            closeQueued();
            active = false;
            aborted = false;
        }

        @Override
        public String tryEnqueue(ExecutionRequest request) {
            if (request != null) {
                queue.add(ByteArrayExecutionRequest.copyOf(request));
            }
            return null;
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
        public void forEachQueued(Consumer<? super ExecutionRequest> visitor) {
            for (ExecutionRequest request : queue) {
                visitor.accept(request);
            }
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

        @Override
        public void close() {
            discard();
        }

        private void closeQueued() {
            for (ExecutionRequest request : queue) {
                request.close();
            }
            queue.clear();
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
        private RuntimeException arrayHeaderFailure;

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

        private void failArrayHeader(RuntimeException failure) {
            arrayHeaderFailure = failure;
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
            if (arrayHeaderFailure != null) {
                throw arrayHeaderFailure;
            }
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
