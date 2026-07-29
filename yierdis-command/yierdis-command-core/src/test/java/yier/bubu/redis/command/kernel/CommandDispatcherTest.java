package yier.bubu.redis.command.kernel;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandHandler;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class CommandDispatcherTest {
    @Test
    public void validationErrorsSkipHandlersAndAbortActiveTransactionAfterReservation() {
        List<ValidationCase> cases = List.of(
                new ValidationCase(request(), "ERR empty command"),
                new ValidationCase(request((String) null), "ERR empty command"),
                new ValidationCase(request(""), "ERR empty command"),
                new ValidationCase(request("PING", "value", null), "ERR Protocol error: null bulk string"),
                new ValidationCase(request("MISSING"), "ERR unknown command 'MISSING'"),
                new ValidationCase(request("PING", "extra"),
                        "ERR wrong number of arguments for 'ping' command")
        );

        for (ValidationCase testCase : cases) {
            AtomicInteger parses = new AtomicInteger();
            AtomicInteger prepares = new AtomicInteger();
            CommandDispatcher dispatcher = dispatcher(spec(
                    "PING", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    countingHandler(parses, prepares, "PONG")
            ));
            RecordingSession session = new RecordingSession(true);
            try (ExecutionRequest request = testCase.request();
                 PreparedCommand prepared = dispatcher.prepare(session, request)) {
                Assert.assertFalse(testCase.expectedReply(), session.tx.aborted());
                CapturingReplyWriter reply = execute(prepared, session, request);
                Assert.assertEquals(testCase.expectedReply(), reply.error());
                Assert.assertTrue(testCase.expectedReply(), session.tx.aborted());
                Assert.assertEquals(testCase.expectedReply(), 0, parses.get());
                Assert.assertEquals(testCase.expectedReply(), 0, prepares.get());
            }
        }
    }

    @Test
    public void parseErrorsSkipPreparationAndAbortActiveTransactionAfterReservation() {
        AtomicInteger parses = new AtomicInteger();
        AtomicInteger prepares = new AtomicInteger();
        CommandDispatcher dispatcher = dispatcher(spec(
                "STRICT", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> {
                    parses.incrementAndGet();
                    throw new CommandParseException("ERR injected parse failure");
                }
        ));
        RecordingSession session = new RecordingSession(true);

        try (ExecutionRequest request = request("STRICT");
             PreparedCommand prepared = dispatcher.prepare(session, request)) {
            Assert.assertFalse(session.tx.aborted());
            CapturingReplyWriter reply = execute(prepared, session, request);
            Assert.assertEquals("ERR injected parse failure", reply.error());
            Assert.assertTrue(session.tx.aborted());
            Assert.assertEquals(1, parses.get());
            Assert.assertEquals(0, prepares.get());
        }
    }

    @Test
    public void queueableCommandParsesBeforeReservationAndEnqueuesOriginalRequestAfterReservation() {
        AtomicInteger parses = new AtomicInteger();
        AtomicInteger prepares = new AtomicInteger();
        CommandDispatcher dispatcher = dispatcher(spec(
                "WRITE", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                countingHandler(parses, prepares, "WRITTEN")
        ));
        RecordingSession session = new RecordingSession(true);

        try (ExecutionRequest request = request("WRITE");
             PreparedCommand prepared = dispatcher.prepare(session, request)) {
            Assert.assertEquals(1, parses.get());
            Assert.assertEquals(0, prepares.get());
            Assert.assertEquals(0, session.tx.enqueueCalls);

            CapturingReplyWriter reply = execute(prepared, session, request);

            Assert.assertEquals("QUEUED", reply.simpleString());
            Assert.assertEquals(1, session.tx.enqueueCalls);
            Assert.assertSame(request, session.tx.lastEnqueued);
            Assert.assertEquals(0, prepares.get());
        }
    }

    @Test
    public void queueableNullInvocationFailsPreflightWithoutEnqueueing() {
        CommandDispatcher dispatcher = dispatcher(spec(
                "WRITE", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> null
        ));
        RecordingSession session = new RecordingSession(true);

        try (ExecutionRequest request = request("WRITE")) {
            NullPointerException failure = Assert.assertThrows(
                    NullPointerException.class,
                    () -> {
                        try (PreparedCommand ignored = dispatcher.prepare(session, request)) {
                        }
                    }
            );

            Assert.assertEquals("command handler returned null", failure.getMessage());
            Assert.assertEquals(0, session.tx.enqueueCalls);
            Assert.assertNull(session.tx.lastEnqueued);
        }
    }

    @Test
    public void disallowedCommandInMultiSkipsHandlerAndAbortsAfterReservation() {
        AtomicInteger parses = new AtomicInteger();
        AtomicInteger prepares = new AtomicInteger();
        CommandDispatcher dispatcher = dispatcher(spec(
                "FORBIDDEN", CommandArity.exact(1), TransactionPolicy.DISALLOWED_IN_MULTI,
                countingHandler(parses, prepares, "NO")
        ));
        RecordingSession session = new RecordingSession(true);

        try (ExecutionRequest request = request("FORBIDDEN");
             PreparedCommand prepared = dispatcher.prepare(session, request)) {
            Assert.assertFalse(session.tx.aborted());
            CapturingReplyWriter reply = execute(prepared, session, request);
            Assert.assertEquals("ERR FORBIDDEN is not allowed in MULTI", reply.error());
            Assert.assertTrue(session.tx.aborted());
            Assert.assertEquals(0, parses.get());
            Assert.assertEquals(0, prepares.get());
        }
    }

    @Test
    public void transactionControlParsesAndPreparesImmediately() {
        AtomicInteger parses = new AtomicInteger();
        AtomicInteger prepares = new AtomicInteger();
        CommandDispatcher dispatcher = dispatcher(spec(
                "MULTI", CommandArity.exact(1), TransactionPolicy.TRANSACTION_CONTROL,
                countingHandler(parses, prepares, "OK")
        ));
        RecordingSession session = new RecordingSession(true);

        try (ExecutionRequest request = request("MULTI");
             PreparedCommand prepared = dispatcher.prepare(session, request)) {
            Assert.assertEquals(1, parses.get());
            Assert.assertEquals(1, prepares.get());
            Assert.assertEquals(0, session.tx.enqueueCalls);
            Assert.assertEquals("OK", execute(prepared, session, request).simpleString());
        }
    }

    @Test
    public void replayParsesAndPreparesQueueableCommandWithoutQueueingAgain() {
        AtomicInteger parses = new AtomicInteger();
        AtomicInteger prepares = new AtomicInteger();
        CommandDispatcher dispatcher = dispatcher(spec(
                "WRITE", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                countingHandler(parses, prepares, "WRITTEN")
        ));
        RecordingSession session = new RecordingSession(true);

        try (ExecutionRequest request = request("WRITE");
             PreparedCommand prepared = dispatcher.prepareReplay(session, request)) {
            Assert.assertEquals(1, parses.get());
            Assert.assertEquals(1, prepares.get());
            Assert.assertEquals(0, session.tx.enqueueCalls);
            Assert.assertEquals("WRITTEN", execute(prepared, session, request).simpleString());
        }
    }

    @Test
    public void requestCommandNameUsesExactBytesWithoutMetadataTrimming() {
        CommandRegistry registry = registry(spec(
                "PING", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> session -> ready("PONG")
        ));
        Assert.assertNotNull(registry.specByUpperName(" PiNg "));
        CommandDispatcher dispatcher = new CommandDispatcher(registry);
        RecordingSession session = new RecordingSession(false);

        try (ExecutionRequest request = request(" PING ");
             PreparedCommand prepared = dispatcher.prepare(session, request)) {
            Assert.assertEquals("ERR unknown command ' PING '", execute(prepared, session, request).error());
        }
    }

    @Test
    public void nonAsciiCommandNameUsesSafeUnknownCommandReply() {
        CommandDispatcher dispatcher = dispatcher(spec("PING"));
        RecordingSession session = new RecordingSession(false);
        ExecutionRequest request = ByteArrayExecutionRequest.copyOf(List.of(new byte[]{(byte) 0xff}));

        try (request; PreparedCommand prepared = dispatcher.prepare(session, request)) {
            Assert.assertEquals("ERR unknown command", execute(prepared, session, request).error());
        }
    }

    @Test
    public void pingAndEchoKeepTheirSingleNullMessageCompatibility() {
        AtomicInteger parses = new AtomicInteger();
        CommandDispatcher dispatcher = dispatcher(spec(
                "PING", CommandArity.exact(2), TransactionPolicy.QUEUEABLE,
                args -> {
                    parses.incrementAndGet();
                    Assert.assertTrue(args.isNull(1));
                    return session -> ready("PONG");
                }
        ));
        RecordingSession session = new RecordingSession(false);

        try (ExecutionRequest request = request("PING", null);
             PreparedCommand prepared = dispatcher.prepare(session, request)) {
            Assert.assertEquals("PONG", execute(prepared, session, request).simpleString());
            Assert.assertEquals(1, parses.get());
        }
    }

    @Test
    public void semanticStorageExceptionsBecomeErrorReplies() {
        List<RuntimeException> failures = List.of(
                new WrongTypeException(),
                new YierdisCommandException("ERR injected semantic failure")
        );
        List<String> expected = List.of(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
                "ERR injected semantic failure"
        );

        for (int index = 0; index < failures.size(); index++) {
            RuntimeException failure = failures.get(index);
            CommandDispatcher dispatcher = dispatcher(spec(
                    "READ", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> {
                        throw failure;
                    }
            ));
            RecordingSession session = new RecordingSession(false);
            try (ExecutionRequest request = request("READ");
                 PreparedCommand prepared = dispatcher.prepare(session, request)) {
                Assert.assertEquals(expected.get(index), execute(prepared, session, request).error());
            }
        }
    }

    @Test
    public void arbitraryIllegalArgumentsEscapeFromParseAndPrepare() {
        IllegalArgumentException parseFailure = new IllegalArgumentException("parse defect");
        CommandDispatcher parseDispatcher = dispatcher(spec(
                "PARSE", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> {
                    throw parseFailure;
                }
        ));
        RecordingSession session = new RecordingSession(false);
        try (ExecutionRequest request = request("PARSE")) {
            Assert.assertSame(parseFailure, Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> parseDispatcher.prepare(session, request)
            ));
        }

        IllegalArgumentException prepareFailure = new IllegalArgumentException("prepare defect");
        CommandDispatcher prepareDispatcher = dispatcher(spec(
                "PREPARE", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> ignored -> {
                    throw prepareFailure;
                }
        ));
        try (ExecutionRequest request = request("PREPARE")) {
            Assert.assertSame(prepareFailure, Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> prepareDispatcher.prepare(session, request)
            ));
        }
    }

    @Test
    public void enqueueAndPreparedCloseFaultsEscapeUnchanged() {
        IllegalStateException enqueueFailure = new IllegalStateException("enqueue defect");
        CommandDispatcher queueDispatcher = dispatcher(spec("WRITE"));
        RecordingSession activeSession = new RecordingSession(true);
        activeSession.tx.enqueueFailure = enqueueFailure;
        try (ExecutionRequest request = request("WRITE");
             PreparedCommand prepared = queueDispatcher.prepare(activeSession, request);
             CommandExecutionContext context = CommandExecutionContext.forRequest(
                     activeSession, new CapturingReplyWriter(), request)) {
            Assert.assertSame(enqueueFailure, Assert.assertThrows(
                    IllegalStateException.class,
                    () -> prepared.execute(context)
            ));
        }

        IllegalStateException closeFailure = new IllegalStateException("close defect");
        CommandDispatcher closeDispatcher = dispatcher(spec(
                "CLOSE", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> session -> throwingClose(closeFailure)
        ));
        RecordingSession inactiveSession = new RecordingSession(false);
        try (ExecutionRequest request = request("CLOSE")) {
            PreparedCommand prepared = closeDispatcher.prepare(inactiveSession, request);
            Assert.assertSame(closeFailure, Assert.assertThrows(
                    IllegalStateException.class,
                    prepared::close
            ));
        }
    }

    private static CommandDispatcher dispatcher(CommandSpec... specs) {
        return new CommandDispatcher(registry(specs));
    }

    private static CommandRegistry registry(CommandSpec... specs) {
        CommandRegistry registry = new CommandRegistry();
        for (CommandSpec spec : specs) {
            registry.register(spec);
        }
        registry.seal();
        return registry;
    }

    private static CommandSpec spec(String name) {
        return spec(name, CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> session -> ready("OK"));
    }

    private static CommandSpec spec(
            String name,
            CommandArity arity,
            TransactionPolicy transactionPolicy,
            CommandHandler handler
    ) {
        return new CommandSpec(
                new CommandSyntax(name, arity, CommandKeySpec.NONE, transactionPolicy),
                handler
        );
    }

    private static CommandHandler countingHandler(
            AtomicInteger parses,
            AtomicInteger prepares,
            String reply
    ) {
        return args -> {
            parses.incrementAndGet();
            return session -> {
                prepares.incrementAndGet();
                return ready(reply);
            };
        };
    }

    private static PreparedCommand ready(String reply) {
        return yier.bubu.redis.execution.api.PreparedCommands.ready(
                RedisReplies.simpleString(reply)
        );
    }

    private static PreparedCommand throwingClose(RuntimeException failure) {
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
                throw failure;
            }
        };
    }

    private static CapturingReplyWriter execute(
            PreparedCommand prepared,
            CommandSession session,
            ExecutionRequest request
    ) {
        Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
        CapturingReplyWriter reply = new CapturingReplyWriter();
        try (CommandExecutionContext context = CommandExecutionContext.forRequest(session, reply, request)) {
            prepared.execute(context);
        }
        return reply;
    }

    private static ExecutionRequest request(String... argv) {
        List<byte[]> bytes = new ArrayList<>(argv.length);
        for (String arg : argv) {
            bytes.add(arg == null ? null : arg.getBytes(StandardCharsets.UTF_8));
        }
        return ByteArrayExecutionRequest.copyOf(bytes);
    }

    private record ValidationCase(ExecutionRequest request, String expectedReply) {
    }

    private static final class RecordingSession implements CommandSession {
        private final RecordingTransactionState tx;

        private RecordingSession(boolean active) {
            tx = new RecordingTransactionState(active);
        }

        @Override
        public int dbIndex() {
            return 0;
        }

        @Override
        public void setDbIndex(int dbIndex) {
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
    }

    private static final class RecordingTransactionState implements TransactionState {
        private boolean active;
        private boolean aborted;
        private int enqueueCalls;
        private ExecutionRequest lastEnqueued;
        private RuntimeException enqueueFailure;

        private RecordingTransactionState(boolean active) {
            this.active = active;
        }

        @Override
        public boolean active() {
            return active;
        }

        @Override
        public boolean aborted() {
            return aborted;
        }

        @Override
        public void begin() {
            active = true;
            aborted = false;
        }

        @Override
        public void markAborted() {
            aborted = true;
        }

        @Override
        public String tryEnqueue(ExecutionRequest request) {
            enqueueCalls++;
            lastEnqueued = request;
            if (enqueueFailure != null) {
                throw enqueueFailure;
            }
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void forEachQueued(Consumer<? super ExecutionRequest> visitor) {
        }

        @Override
        public List<ExecutionRequest> drain() {
            active = false;
            return List.of();
        }

        @Override
        public void discard() {
            active = false;
            aborted = false;
        }

        @Override
        public void close() {
            discard();
        }
    }

    private static final class CapturingReplyWriter implements RedisReplyWriter {
        private String simpleString;
        private String error;

        private String simpleString() {
            return simpleString;
        }

        private String error() {
            return error;
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
            throw unsupported();
        }

        @Override
        public void emptyArray() {
            throw unsupported();
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
