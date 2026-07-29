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

    @Test
    public void transactionQueuePreflightParsesOnlyAndRetainsTheOriginalRequestAfterQueuedReply() {
        AtomicInteger parses = new AtomicInteger();
        AtomicInteger prepares = new AtomicInteger();
        CommandDispatcher dispatcher = transactionDispatcher(registration -> registration.register(spec(
                "WRITE", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> {
                    parses.incrementAndGet();
                    return session -> {
                        prepares.incrementAndGet();
                        return ready("WRITTEN");
                    };
                }
        )));
        TrackingTransactionState tx = new TrackingTransactionState();
        TrackingSession session = new TrackingSession(tx);
        tx.begin();
        TrackingRequest request = trackingRequest("WRITE");

        try (PreparedCommand queued = dispatcher.prepare(session, request)) {
            Assert.assertEquals(1, parses.get());
            Assert.assertEquals(0, prepares.get());
            Assert.assertEquals(0, tx.size());

            Assert.assertEquals("QUEUED", execute(queued, session, request).simpleString());
            Assert.assertEquals(1, tx.size());
            Assert.assertSame(request, tx.request(0));
            Assert.assertEquals(0, prepares.get());
            Assert.assertEquals(0, request.closeCount());
        }
        tx.discard();
        Assert.assertEquals(1, request.closeCount());
    }

    @Test
    public void multiChildExecUsesTheWriterBridgeAndClosesEveryOwnerOnce() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        TrackingPrepared first = writingChild(
                ReplyShapes.bulkString(3, 0), out -> out.bulkString(bytes("one")));
        TrackingPrepared second = writingChild(
                ReplyShapes.integer(2), out -> out.integer(2));
        PreparedCommand exec = prepareExec(tx, first, second);

        Assert.assertEquals(ReplyShapes.maximum(), exec.replyShape());
        EventReplyWriter out = executeWithWriter(exec, tx);
        Assert.assertEquals(List.of("array:2", "bulk:one", "integer:2"), out.events());
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(1, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());

        exec.close();
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(1, second.closeCount());
    }

    @Test
    public void singleChildExecUsesExactShapeAndClosesStaleChildBeforeRepreparing() {
        TrackingTransactionState tx = transactionWith("FIRST");
        List<String> lifecycle = new ArrayList<>();
        ReplyShape staleShape = ReplyShapes.simpleString("OLD");
        TrackingPrepared stale = writingChild(staleShape, out -> out.simpleString("OLD"));
        stale.stale = true;
        stale.onClose = () -> lifecycle.add("close:stale");
        ReplyShape currentShape = ReplyShapes.simpleString("NEW");
        TrackingPrepared current = writingChild(currentShape, out -> out.simpleString("NEW"));
        current.onClose = () -> lifecycle.add("close:current");
        AtomicInteger preparations = new AtomicInteger();
        CommandDispatcher dispatcher = transactionDispatcher(registration -> registration.register(spec(
                "FIRST", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> session -> {
                    int preparation = preparations.incrementAndGet();
                    lifecycle.add("prepare:" + preparation);
                    return preparation == 1 ? stale : current;
                }
        )));
        TrackingSession session = new TrackingSession(tx);

        PreparedCommand staleExec = prepare(dispatcher, session, "EXEC");
        Assert.assertEquals(ReplyShapes.array(List.of(staleShape)), staleExec.replyShape());
        Assert.assertEquals(ValidationResult.STALE, staleExec.validateBeforeExecute());
        staleExec.close();

        PreparedCommand currentExec = prepare(dispatcher, session, "EXEC");
        Assert.assertEquals(ReplyShapes.array(List.of(currentShape)), currentExec.replyShape());
        EventReplyWriter out = executeWithWriter(currentExec, session, trackingRequest("EXEC"));

        Assert.assertEquals(List.of("array:1", "simple:NEW"), out.events());
        Assert.assertEquals(List.of("prepare:1", "close:stale", "prepare:2", "close:current"), lifecycle);
        Assert.assertEquals(1, stale.closeCount());
        Assert.assertEquals(1, current.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
    }

    @Test
    public void dynamicExecClosesStaleChildBeforeRepreparingTheSameRequest() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        List<String> lifecycle = new ArrayList<>();
        TrackingPrepared stale = writingChild(ReplyShapes.simpleString("OLD"), out -> out.simpleString("OLD"));
        stale.stale = true;
        stale.onClose = () -> lifecycle.add("close:stale");
        TrackingPrepared current = writingChild(ReplyShapes.simpleString("NEW"), out -> out.simpleString("NEW"));
        TrackingPrepared second = writingChild(ReplyShapes.integer(2), out -> out.integer(2));
        AtomicInteger firstPreparations = new AtomicInteger();
        CommandDispatcher dispatcher = transactionDispatcher(registration -> {
            registration.register(spec("FIRST", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> {
                        int preparation = firstPreparations.incrementAndGet();
                        lifecycle.add("prepare:first:" + preparation);
                        return preparation == 1 ? stale : current;
                    }));
            registration.register(spec("SECOND", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> {
                        lifecycle.add("prepare:second");
                        return second;
                    }));
        });
        TrackingSession session = new TrackingSession(tx);
        PreparedCommand exec = prepare(dispatcher, session, "EXEC");

        EventReplyWriter out = executeWithWriter(exec, session, trackingRequest("EXEC"));
        Assert.assertEquals(List.of("array:2", "simple:NEW", "integer:2"), out.events());
        Assert.assertEquals(1, stale.closeCount());
        Assert.assertEquals(1, current.closeCount());
        Assert.assertEquals(1, second.closeCount());
        Assert.assertEquals(2, firstPreparations.get());
        Assert.assertTrue(lifecycle.indexOf("close:stale") < lifecycle.indexOf("prepare:first:2"));
    }

    @Test
    public void dynamicExecClosesEveryRequestWhenChildParseFails() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        IllegalStateException parseFailure = new IllegalStateException("parse failure");
        AtomicInteger secondPreparations = new AtomicInteger();
        TrackingPrepared second = writingChild(ReplyShapes.simpleString("TWO"), out -> out.simpleString("TWO"));
        CommandDispatcher dispatcher = transactionDispatcher(registration -> {
            registration.register(spec("FIRST", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> {
                        throw parseFailure;
                    }));
            registration.register(spec("SECOND", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> {
                        secondPreparations.incrementAndGet();
                        return second;
                    }));
        });

        PreparedCommand exec = prepare(dispatcher, new TrackingSession(tx), "EXEC");
        IllegalStateException thrown = Assert.assertThrows(
                IllegalStateException.class,
                () -> executeWithWriter(exec, tx)
        );

        Assert.assertSame(parseFailure, thrown);
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
        Assert.assertEquals(0, secondPreparations.get());
        Assert.assertEquals(0, second.closeCount());
    }

    @Test
    public void dynamicExecClosesEveryRequestWhenChildPreparationFails() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        IllegalStateException prepareFailure = new IllegalStateException("prepare failure");
        AtomicInteger secondPreparations = new AtomicInteger();
        TrackingPrepared second = writingChild(ReplyShapes.simpleString("TWO"), out -> out.simpleString("TWO"));
        CommandDispatcher dispatcher = transactionDispatcher(registration -> {
            registration.register(spec("FIRST", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> {
                        throw prepareFailure;
                    }));
            registration.register(spec("SECOND", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> {
                        secondPreparations.incrementAndGet();
                        return second;
                    }));
        });

        PreparedCommand exec = prepare(dispatcher, new TrackingSession(tx), "EXEC");
        IllegalStateException thrown = Assert.assertThrows(
                IllegalStateException.class,
                () -> executeWithWriter(exec, tx)
        );

        Assert.assertSame(prepareFailure, thrown);
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
        Assert.assertEquals(0, secondPreparations.get());
        Assert.assertEquals(0, second.closeCount());
    }

    @Test
    public void dynamicExecClosesPreparedChildWhenValidationFails() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        IllegalStateException validationFailure = new IllegalStateException("validation failure");
        IllegalStateException closeFailure = new IllegalStateException("validation cleanup failure");
        TrackingPrepared first = writingChild(ReplyShapes.simpleString("ONE"), out -> out.simpleString("ONE"));
        first.validationFailure = validationFailure;
        first.closeFailure = closeFailure;
        TrackingPrepared second = writingChild(ReplyShapes.simpleString("TWO"), out -> out.simpleString("TWO"));
        PreparedCommand exec = prepareExec(tx, first, second);

        IllegalStateException thrown = Assert.assertThrows(
                IllegalStateException.class,
                () -> executeWithWriter(exec, tx)
        );

        Assert.assertSame(validationFailure, thrown);
        Assert.assertArrayEquals(new Throwable[]{closeFailure}, thrown.getSuppressed());
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(0, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
    }

    @Test
    public void execFailureClosesTheRemainingQueuedRequestsAndSuppressesCloseFailures() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        IllegalStateException executionFailure = new IllegalStateException("execute failure");
        TrackingPrepared first = writingChild(ReplyShapes.simpleString("ONE"), out -> {
            throw executionFailure;
        });
        TrackingPrepared second = writingChild(ReplyShapes.simpleString("TWO"), out -> out.simpleString("TWO"));
        IllegalStateException closeFailure = new IllegalStateException("tail close failure");
        tx.request(1).closeFailure = closeFailure;
        PreparedCommand exec = prepareExec(tx, first, second);

        IllegalStateException thrown = Assert.assertThrows(
                IllegalStateException.class,
                () -> executeWithWriter(exec, tx)
        );
        Assert.assertSame(executionFailure, thrown);
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(0, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
        Assert.assertArrayEquals(new Throwable[]{closeFailure}, thrown.getSuppressed());
    }

    @Test
    public void currentRequestCloseFailureDoesNotCloseThatRequestTwice() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        IllegalStateException closeFailure = new IllegalStateException("current request close failure");
        tx.request(0).closeFailure = closeFailure;
        TrackingPrepared first = writingChild(ReplyShapes.simpleString("ONE"), out -> out.simpleString("ONE"));
        TrackingPrepared second = writingChild(ReplyShapes.simpleString("TWO"), out -> out.simpleString("TWO"));
        PreparedCommand exec = prepareExec(tx, first, second);

        IllegalStateException thrown = Assert.assertThrows(
                IllegalStateException.class,
                () -> executeWithWriter(exec, tx)
        );

        Assert.assertSame(closeFailure, thrown);
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(0, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
    }

    @Test
    public void retainedChildCloseRethrowingPrimaryDoesNotStopRequestCleanup() {
        TrackingTransactionState tx = transactionWith("FIRST");
        IllegalStateException executionFailure = new IllegalStateException("execute and close failure");
        TrackingPrepared child = writingChild(ReplyShapes.simpleString("ONE"), out -> {
            throw executionFailure;
        });
        child.closeFailure = executionFailure;
        CommandDispatcher dispatcher = transactionDispatcher(registration -> registration.register(spec(
                "FIRST", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> session -> child
        )));
        PreparedCommand exec = prepare(dispatcher, new TrackingSession(tx), "EXEC");

        IllegalStateException thrown = Assert.assertThrows(
                IllegalStateException.class,
                () -> executeWithWriter(exec, tx)
        );

        Assert.assertSame(executionFailure, thrown);
        Assert.assertEquals(1, child.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
    }

    @Test
    public void exactPreparationFailureContinuesClosingRetainedChildrenWhenCloseRethrowsPrimary() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        tx.reportedSize = 1;
        IllegalStateException preparationFailure = new IllegalStateException("reply shape failure");
        TrackingPrepared first = writingChild(ReplyShapes.simpleString("ONE"), out -> out.simpleString("ONE"));
        TrackingPrepared second = writingChild(ReplyShapes.simpleString("TWO"), out -> out.simpleString("TWO"));
        second.replyShapeFailure = preparationFailure;
        second.closeFailure = preparationFailure;
        CommandDispatcher dispatcher = transactionDispatcher(registration -> {
            registration.register(spec("FIRST", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> first));
            registration.register(spec("SECOND", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> second));
        });

        IllegalStateException thrown = Assert.assertThrows(
                IllegalStateException.class,
                () -> prepare(dispatcher, new TrackingSession(tx), "EXEC")
        );

        Assert.assertSame(preparationFailure, thrown);
        Assert.assertEquals(1, second.closeCount());
        Assert.assertEquals(1, first.closeCount());
        tx.discard();
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
    }

    private static CommandDispatcher transactionDispatcher(CommandModuleAction module) {
        return CommandRegistries.dispatcher(registration -> module.register(registration));
    }

    private static PreparedCommand prepareExec(
            TrackingTransactionState tx,
            TrackingPrepared first,
            TrackingPrepared second
    ) {
        CommandDispatcher dispatcher = transactionDispatcher(registration -> {
            registration.register(spec("FIRST", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> first));
            registration.register(spec("SECOND", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> second));
        });
        return prepare(dispatcher, new TrackingSession(tx), "EXEC");
    }

    private static PreparedCommand prepare(CommandDispatcher dispatcher, CommandSession session, String command) {
        return dispatcher.prepare(session, trackingRequest(command));
    }

    private static TrackingTransactionState transactionWith(String... commands) {
        TrackingTransactionState tx = new TrackingTransactionState();
        tx.begin();
        for (String command : commands) {
            tx.tryEnqueue(trackingRequest(command));
        }
        return tx;
    }

    private static TrackingRequest trackingRequest(String command) {
        return new TrackingRequest(ByteArrayExecutionRequest.fromUtf8(command, List.of()));
    }

    private static TrackingPrepared writingChild(ReplyShape shape, Consumer<RedisReplyWriter> writing) {
        return new TrackingPrepared(shape, writing);
    }

    private static EventReplyWriter executeWithWriter(PreparedCommand command, TrackingTransactionState tx) {
        return executeWithWriter(command, new TrackingSession(tx), trackingRequest("EXEC"));
    }

    private static EventReplyWriter executeWithWriter(
            PreparedCommand command,
            CommandSession session,
            ExecutionRequest request
    ) {
        Assert.assertEquals(ValidationResult.VALID, command.validateBeforeExecute());
        EventReplyWriter out = new EventReplyWriter();
        try (request; CommandExecutionContext context = CommandExecutionContext.forRequest(session, out, request)) {
            command.execute(context);
        }
        return out;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
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

    @FunctionalInterface
    private interface CommandModuleAction {
        void register(yier.bubu.redis.command.api.CommandModule.Registration registration);
    }

    private static final class TrackingSession implements CommandSession {
        private final TrackingTransactionState tx;

        private TrackingSession(TrackingTransactionState tx) {
            this.tx = tx;
        }

        @Override public int dbIndex() { return 0; }
        @Override public void setDbIndex(int dbIndex) { }
        @Override public long clientId() { return 1L; }
        @Override public String clientName() { return null; }
        @Override public void setClientName(String clientName) { }
        @Override public boolean authenticated() { return false; }
        @Override public void setAuthenticated(boolean authenticated) { }
        @Override public TransactionState transaction() { return tx; }
        @Override public ConnectionStatsView connectionStats() { return null; }
        @Override public int respVersion() { return 2; }
        @Override public void setRespVersion(int respVersion) { }
    }

    private static final class TrackingTransactionState implements TransactionState {
        private boolean active;
        private boolean aborted;
        private Integer reportedSize;
        private final ArrayList<TrackingRequest> queue = new ArrayList<>();
        private final ArrayList<TrackingRequest> requests = new ArrayList<>();

        @Override public boolean active() { return active; }
        @Override public boolean aborted() { return aborted; }
        @Override public void begin() { active = true; aborted = false; }
        @Override public void markAborted() { aborted = true; }
        @Override public int size() { return reportedSize == null ? queue.size() : reportedSize; }
        @Override public void forEachQueued(Consumer<? super ExecutionRequest> visitor) { queue.forEach(visitor); }
        @Override public String tryEnqueue(ExecutionRequest request) {
            TrackingRequest tracked = (TrackingRequest) request;
            queue.add(tracked);
            requests.add(tracked);
            return null;
        }
        @Override public List<ExecutionRequest> drain() {
            List<ExecutionRequest> drained = new ArrayList<>(queue);
            queue.clear();
            active = false;
            aborted = false;
            return drained;
        }
        @Override public void discard() {
            for (TrackingRequest request : queue) {
                request.close();
            }
            queue.clear();
            active = false;
            aborted = false;
        }
        @Override public void close() { discard(); }

        private TrackingRequest request(int index) { return requests.get(index); }
    }

    private static final class TrackingRequest implements ExecutionRequest {
        private final ExecutionRequest delegate;
        private int closeCount;
        private RuntimeException closeFailure;

        private TrackingRequest(ExecutionRequest delegate) {
            this.delegate = delegate;
        }

        @Override public int argc() { return delegate.argc(); }
        @Override public boolean isNull(int index) { return delegate.isNull(index); }
        @Override public int len(int index) { return delegate.len(index); }
        @Override public byte byteAt(int index, int offset) { return delegate.byteAt(index, offset); }
        @Override public void copyToByteArray(int index, byte[] dst, int dstOff) {
            delegate.copyToByteArray(index, dst, dstOff);
        }
        @Override public byte[] toByteArray(int index) { return delegate.toByteArray(index); }
        @Override public byte[] readOnlyByteArray(int index) { return delegate.readOnlyByteArray(index); }
        @Override public int retainedBytes() { return delegate.retainedBytes(); }
        @Override public long admittedMemoryBytes() { return delegate.admittedMemoryBytes(); }
        @Override public TrackingRequest retain() { return new TrackingRequest(delegate.retain()); }
        @Override public void close() {
            closeCount++;
            delegate.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        private int closeCount() { return closeCount; }
    }

    private static final class TrackingPrepared implements PreparedCommand {
        private final ReplyShape shape;
        private final Consumer<RedisReplyWriter> writing;
        private boolean stale;
        private RuntimeException replyShapeFailure;
        private RuntimeException validationFailure;
        private RuntimeException closeFailure;
        private Runnable onClose = () -> { };
        private int closeCount;

        private TrackingPrepared(ReplyShape shape, Consumer<RedisReplyWriter> writing) {
            this.shape = shape;
            this.writing = writing;
        }

        @Override public ReplyShape replyShape() {
            if (replyShapeFailure != null) {
                throw replyShapeFailure;
            }
            return shape;
        }
        @Override public ValidationResult validateBeforeExecute() {
            if (validationFailure != null) {
                throw validationFailure;
            }
            return stale ? ValidationResult.STALE : ValidationResult.VALID;
        }
        @Override public void execute(CommandExecutionContext context) { writing.accept(context.reply()); }
        @Override public void close() {
            closeCount++;
            onClose.run();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        private int closeCount() { return closeCount; }
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

    private static final class EventReplyWriter implements RedisReplyWriter {
        private final ArrayList<String> events = new ArrayList<>();

        private List<String> events() { return List.copyOf(events); }

        @Override public void requestCloseAfterReply() { }
        @Override public boolean closeAfterReplyRequested() { return false; }
        @Override public void simpleString(String value) { events.add("simple:" + value); }
        @Override public void error(String message) { events.add("error:" + message); }
        @Override public void integer(long value) { events.add("integer:" + value); }
        @Override public void booleanValue(boolean value) { throw unsupported(); }
        @Override public void doubleValue(double value) { throw unsupported(); }
        @Override public void bigNumberAscii(String value) { throw unsupported(); }
        @Override public void verbatimString(String format, byte[] data) { throw unsupported(); }
        @Override public void blobError(String message) { throw unsupported(); }
        @Override public void nullValue() { throw unsupported(); }
        @Override public void nullArray() { throw unsupported(); }
        @Override public void arrayHeader(int count) { events.add("array:" + count); }
        @Override public void emptyArray() { events.add("array:0"); }
        @Override public void mapHeader(int pairs) { throw unsupported(); }
        @Override public void setHeader(int count) { throw unsupported(); }
        @Override public void pushHeader(int count) { throw unsupported(); }
        @Override public void attributeHeader(int pairs) { throw unsupported(); }
        @Override public void bulkString(byte[] data) {
            events.add("bulk:" + new String(data, StandardCharsets.UTF_8));
        }
        @Override public void bulkString(byte[] data, int off, int len) {
            events.add("bulk:" + new String(data, off, len, StandardCharsets.UTF_8));
        }
        @Override public void bulkString(BytesSlice slice) { throw unsupported(); }
        @Override public void bulkStringLongAscii(long value) { throw unsupported(); }

        private UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("reply shape not used by this test");
        }
    }
}
