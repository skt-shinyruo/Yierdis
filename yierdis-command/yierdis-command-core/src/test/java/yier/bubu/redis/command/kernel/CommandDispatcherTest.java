package yier.bubu.redis.command.kernel;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandHandler;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.common.command.ResultUnknownException;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplySink;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public class CommandDispatcherTest {
    @Test
    public void dispatcherPreparesAndExecutesRegisteredCommand() {
        CommandDispatcher dispatcher = dispatcher(spec(
                "LOCAL",
                CommandArity.exact(1),
                TransactionPolicy.QUEUEABLE,
                args -> session -> ready("LOCAL_OK")
        ));
        RecordingSession session = new RecordingSession(false);

        try (ExecutionRequest request = request("LOCAL");
             PreparedCommand prepared = dispatcher.prepare(session, request)) {
            CapturedReply reply = execute(prepared, session, request);
            Assert.assertEquals("LOCAL_OK", reply.simpleString());
            Assert.assertNull(reply.error());
        }
    }

    @Test
    public void preparedExecutionScopesTheRequestAsItsExplicitMutationContext() {
        AtomicReference<MutationContext> successfulContext = new AtomicReference<>();
        AtomicReference<MutationContext> failedContext = new AtomicReference<>();
        CommandDispatcher dispatcher = CommandRegistries.dispatcher(registration -> {
            registration.register(new CommandSpec(
                    syntax("SCOPED"),
                    args -> session -> prepared(ReplyShapes.simpleString("OK"), context -> {
                        Assert.assertSame(args.request(), context.mutationContext().commandRecord());
                        successfulContext.set(context.mutationContext());
                        return CommandResult.reply(RedisReplies.simpleString("OK"));
                    })
            ));
            registration.register(new CommandSpec(
                    syntax("FAIL"),
                    args -> session -> prepared(ReplyShapes.simpleString("OK"), context -> {
                        Assert.assertSame(args.request(), context.mutationContext().commandRecord());
                        failedContext.set(context.mutationContext());
                        throw new IllegalStateException("injected");
                    })
            ));
        });
        RecordingSession session = new RecordingSession(false);

        executePrepared(dispatcher, session, request("SCOPED"));
        Assert.assertFalse(successfulContext.get().hasCommandRecord());

        Assert.assertThrows(
                IllegalStateException.class,
                () -> executePrepared(dispatcher, session, request("FAIL"))
        );
        Assert.assertFalse(failedContext.get().hasCommandRecord());
    }

    @Test
    public void transactionReplayUsesTheQueuedRequestAsItsCurrentMutationRecord() {
        AtomicReference<MutationContext> replayContext = new AtomicReference<>();
        CommandDispatcher dispatcher = CommandRegistries.dispatcher(registration -> registration.register(
                new CommandSpec(
                        syntax("SCOPED"),
                        args -> session -> prepared(ReplyShapes.simpleString("OK"), context -> {
                            Assert.assertSame(args.request(), context.mutationContext().commandRecord());
                            replayContext.set(context.mutationContext());
                            return CommandResult.reply(RedisReplies.simpleString("OK"));
                        })
                )
        ));
        TrackingTransactionState transaction = transactionWith("SCOPED");

        PreparedCommand exec = prepare(dispatcher, new TrackingSession(transaction), "EXEC");
        executeResult(exec, transaction);

        Assert.assertFalse(replayContext.get().hasCommandRecord());
        exec.close();
    }

    @Test
    public void dispatcherPreparationAcceptsCompleteCommandSession() {
        CommandDispatcher dispatcher = CommandRegistries.dispatcher(registration -> registration.register(
                new CommandSpec(
                        syntax("LOCAL"),
                        args -> session -> prepared(
                                ReplyShapes.simpleString("DB_" + session.dbIndex()),
                                context -> CommandResult.reply(RedisReplies.simpleString(
                                        "DB_" + context.session().dbIndex()))
                        )
                )
        ));
        RecordingSession session = new RecordingSession(false);
        session.setDbIndex(7);

        try (ExecutionRequest request = request("LOCAL");
             PreparedCommand prepared = dispatcher.prepare(session, request)) {
            CapturedReply reply = execute(prepared, session, request);
            Assert.assertEquals("DB_7", reply.simpleString());
            Assert.assertNull(reply.error());
        }
    }

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
                CapturedReply reply = execute(prepared, session, request);
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
            CapturedReply reply = execute(prepared, session, request);
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

            CapturedReply reply = execute(prepared, session, request);

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
            CapturedReply reply = execute(prepared, session, request);
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
    public void everyTransactionControlHandlerParsesWithoutSessionAccess() throws Exception {
        CommandRegistry registry = new CommandRegistry();
        CommandDispatcher dispatcher = new CommandDispatcher(registry);
        new TransactionCommands(dispatcher).register(registry);
        registry.seal();

        Assert.assertEquals(Set.of("MULTI", "DISCARD", "EXEC"),
                Set.of(registry.upperNamesSorted()));
        for (String name : registry.upperNamesSorted()) {
            try (ExecutionRequest request = request(name)) {
                CommandSpec spec = registry.specByUpperName(name);
                Assert.assertNotNull(name, spec.handler().parse(CommandArgs.of(request)));
            }
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
                     activeSession, request)) {
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
    public void multiChildExecReturnsRepliesInOrderAndRetainsEveryOwnerUntilClose() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        TrackingPrepared first = resultChild(CommandResult.reply(RedisReplies.simpleString("one")));
        TrackingPrepared second = resultChild(CommandResult.reply(RedisReplies.integer(2)));
        PreparedCommand exec = prepareExec(tx, first, second);

        Assert.assertEquals(ReplyShapes.maximum(), exec.reservationShape());
        CommandResult result = executeResult(exec, tx);
        RedisReply.Aggregate aggregate = aggregate(result);
        Assert.assertEquals(ReplyShape.AggregateKind.ARRAY, aggregate.kind());
        Assert.assertEquals("one", ((RedisReply.SimpleString) aggregate.elements().get(0)).value());
        Assert.assertEquals(2L, ((RedisReply.IntegerValue) aggregate.elements().get(1)).value());
        Assert.assertFalse(result.closeAfterReply());
        Assert.assertEquals(0, first.closeCount());
        Assert.assertEquals(0, second.closeCount());
        Assert.assertEquals(0, tx.request(0).closeCount());
        Assert.assertEquals(0, tx.request(1).closeCount());
        Assert.assertThrows(IllegalStateException.class, () -> executeResult(exec, tx));

        exec.close();
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(1, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
        exec.close();
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(1, second.closeCount());
    }

    @Test
    public void execConvertsChildControlErrorsAndOrsCloseAfterReply() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        TrackingPrepared first = resultChild(CommandResult.controlError("WRONGTYPE injected"));
        TrackingPrepared second = resultChild(CommandResult.closeAfterReply(
                RedisReplies.simpleString("BYE")));
        PreparedCommand exec = prepareExec(tx, first, second);

        CommandResult result = executeResult(exec, tx);
        RedisReply.Aggregate aggregate = aggregate(result);
        Assert.assertTrue(result.closeAfterReply());
        Assert.assertTrue(aggregate.elements().get(0) instanceof RedisReply.Error);
        Assert.assertFalse(aggregate.elements().get(0) instanceof RedisReply.ControlError);
        Assert.assertEquals(
                "WRONGTYPE injected",
                ((RedisReply.Error) aggregate.elements().get(0)).message());
        Assert.assertEquals("BYE", ((RedisReply.SimpleString) aggregate.elements().get(1)).value());

        exec.close();
    }

    @Test
    public void streamedChildSourceRemainsOpenThroughEmissionAndClosesWithExec() {
        TrackingSource source = new TrackingSource(bytes("one"));
        RedisReply streamed = RedisReplies.bulkString(3, 3, source::emit);
        TrackingPrepared child = resultChild(CommandResult.reply(streamed));
        child.onClose = source::close;
        TrackingTransactionState tx = transactionWith("FIRST");
        CommandDispatcher dispatcher = transactionDispatcher(registration -> registration.register(spec(
                "FIRST", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                args -> session -> child
        )));
        PreparedCommand exec = prepare(dispatcher, new TrackingSession(tx), "EXEC");

        CommandResult result = executeResult(exec, tx);
        RedisReply.BulkString bulk = (RedisReply.BulkString) aggregate(result).elements().get(0);
        CapturingBulkSink sink = new CapturingBulkSink();
        bulk.emitter().emit(sink);
        Assert.assertEquals("one", new String(sink.bytes(), StandardCharsets.UTF_8));
        Assert.assertEquals(0, source.closeCount());
        Assert.assertEquals(0, child.closeCount());
        Assert.assertEquals(0, tx.request(0).closeCount());

        exec.close();
        Assert.assertEquals(1, source.closeCount());
        Assert.assertEquals(1, child.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
    }

    @Test
    public void emptyExecKeepsItsExactEmptyArrayReservation() {
        TrackingTransactionState tx = transactionWith();
        CommandDispatcher dispatcher = transactionDispatcher(registration -> { });
        PreparedCommand exec = prepare(dispatcher, new TrackingSession(tx), "EXEC");

        Assert.assertEquals(ReplyShapes.array(List.of()), exec.reservationShape());
        Assert.assertTrue(aggregate(executeResult(exec, tx)).elements().isEmpty());
        exec.close();
    }

    @Test
    public void singleChildExecUsesMaximumReservationAndClosesStaleChildBeforeRepreparing() {
        TrackingTransactionState tx = transactionWith("FIRST");
        List<String> lifecycle = new ArrayList<>();
        TrackingPrepared stale = resultChild(CommandResult.reply(RedisReplies.simpleString("OLD")));
        stale.stale = true;
        stale.onClose = () -> lifecycle.add("close:stale");
        TrackingPrepared current = resultChild(CommandResult.reply(RedisReplies.simpleString("NEW")));
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
        Assert.assertEquals(ReplyShapes.maximum(), staleExec.reservationShape());
        Assert.assertEquals(ValidationResult.STALE, staleExec.validateBeforeExecute());
        staleExec.close();

        PreparedCommand currentExec = prepare(dispatcher, session, "EXEC");
        Assert.assertEquals(ReplyShapes.maximum(), currentExec.reservationShape());
        CommandResult result = executeResult(currentExec, session, trackingRequest("EXEC"));

        Assert.assertEquals(
                "NEW",
                ((RedisReply.SimpleString) aggregate(result).elements().get(0)).value());
        Assert.assertEquals(List.of("prepare:1", "close:stale", "prepare:2"), lifecycle);
        Assert.assertEquals(1, stale.closeCount());
        Assert.assertEquals(0, current.closeCount());
        Assert.assertEquals(0, tx.request(0).closeCount());

        currentExec.close();
        Assert.assertEquals(List.of(
                "prepare:1", "close:stale", "prepare:2", "close:current"), lifecycle);
        Assert.assertEquals(1, current.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
    }

    @Test
    public void dynamicExecClosesStaleChildBeforeRepreparingTheSameRequest() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        List<String> lifecycle = new ArrayList<>();
        TrackingPrepared stale = resultChild(CommandResult.reply(RedisReplies.simpleString("OLD")));
        stale.stale = true;
        stale.onClose = () -> lifecycle.add("close:stale");
        TrackingPrepared current = resultChild(CommandResult.reply(RedisReplies.simpleString("NEW")));
        TrackingPrepared second = resultChild(CommandResult.reply(RedisReplies.integer(2)));
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

        RedisReply.Aggregate result = aggregate(executeResult(
                exec, session, trackingRequest("EXEC")));
        Assert.assertEquals("NEW", ((RedisReply.SimpleString) result.elements().get(0)).value());
        Assert.assertEquals(2L, ((RedisReply.IntegerValue) result.elements().get(1)).value());
        Assert.assertEquals(1, stale.closeCount());
        Assert.assertEquals(0, current.closeCount());
        Assert.assertEquals(0, second.closeCount());
        Assert.assertEquals(2, firstPreparations.get());
        Assert.assertTrue(lifecycle.indexOf("close:stale") < lifecycle.indexOf("prepare:first:2"));

        exec.close();
        Assert.assertEquals(1, current.closeCount());
        Assert.assertEquals(1, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
    }

    @Test
    public void dynamicExecClosesEveryRequestWhenChildParseFails() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        IllegalStateException parseFailure = new IllegalStateException("parse failure");
        AtomicInteger secondPreparations = new AtomicInteger();
        TrackingPrepared second = resultChild(CommandResult.reply(RedisReplies.simpleString("TWO")));
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
                () -> executeResult(exec, tx)
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
        TrackingPrepared second = resultChild(CommandResult.reply(RedisReplies.simpleString("TWO")));
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
                () -> executeResult(exec, tx)
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
        TrackingPrepared first = resultChild(CommandResult.reply(RedisReplies.simpleString("ONE")));
        first.validationFailure = validationFailure;
        first.closeFailure = closeFailure;
        TrackingPrepared second = resultChild(CommandResult.reply(RedisReplies.simpleString("TWO")));
        PreparedCommand exec = prepareExec(tx, first, second);

        IllegalStateException thrown = Assert.assertThrows(
                IllegalStateException.class,
                () -> executeResult(exec, tx)
        );

        Assert.assertSame(validationFailure, thrown);
        Assert.assertArrayEquals(new Throwable[]{closeFailure}, thrown.getSuppressed());
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(0, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
    }

    @Test
    public void childExecutionFailureBecomesResultUnknownAndSuppressesCleanupFailures() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        IllegalStateException executionFailure = new IllegalStateException("execute failure");
        TrackingPrepared first = executingChild(ReplyShapes.simpleString("ONE"), context -> {
            throw executionFailure;
        });
        TrackingPrepared second = resultChild(CommandResult.reply(RedisReplies.simpleString("TWO")));
        IllegalStateException closeFailure = new IllegalStateException("tail close failure");
        tx.request(1).closeFailure = closeFailure;
        PreparedCommand exec = prepareExec(tx, first, second);

        ResultUnknownException thrown = Assert.assertThrows(
                ResultUnknownException.class,
                () -> executeResult(exec, tx)
        );
        Assert.assertSame(executionFailure, thrown.getCause());
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(0, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
        Assert.assertArrayEquals(new Throwable[]{closeFailure}, thrown.getSuppressed());
    }

    @Test
    public void childResultUnknownEscapesUnchangedAndCleansOwnedResources() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        List<String> lifecycle = new ArrayList<>();
        ResultUnknownException resultUnknown = new ResultUnknownException("injected result unknown");
        TrackingPrepared first = executingChild(ReplyShapes.simpleString("ONE"), context -> {
            throw resultUnknown;
        });
        first.onClose = () -> lifecycle.add("child:first");
        TrackingPrepared second = resultChild(CommandResult.reply(RedisReplies.simpleString("TWO")));
        tx.request(0).onClose = () -> lifecycle.add("request:first");
        tx.request(1).onClose = () -> lifecycle.add("request:second");
        PreparedCommand exec = prepareExec(tx, first, second);

        ResultUnknownException thrown = Assert.assertThrows(
                ResultUnknownException.class,
                () -> executeResult(exec, tx)
        );

        Assert.assertSame(resultUnknown, thrown);
        Assert.assertEquals(
                List.of("request:second", "child:first", "request:first"), lifecycle);
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(0, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
    }

    @Test
    public void failedExecCleanupIsReverseAndLaterCloseDoesNotCloseOwnersAgain() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        List<String> lifecycle = new ArrayList<>();
        IllegalStateException executionFailure = new IllegalStateException("second execute failure");
        TrackingPrepared first = resultChild(CommandResult.reply(RedisReplies.simpleString("ONE")));
        first.onClose = () -> lifecycle.add("child:first");
        TrackingPrepared second = executingChild(ReplyShapes.simpleString("TWO"), context -> {
            throw executionFailure;
        });
        second.onClose = () -> lifecycle.add("child:second");
        tx.request(0).onClose = () -> lifecycle.add("request:first");
        tx.request(1).onClose = () -> lifecycle.add("request:second");
        PreparedCommand exec = prepareExec(tx, first, second);

        ResultUnknownException thrown = Assert.assertThrows(
                ResultUnknownException.class,
                () -> executeResult(exec, tx)
        );

        Assert.assertSame(executionFailure, thrown.getCause());
        Assert.assertEquals(List.of(
                "child:second", "request:second", "child:first", "request:first"), lifecycle);
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(1, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());

        exec.close();
        exec.close();
        Assert.assertEquals(List.of(
                "child:second", "request:second", "child:first", "request:first"), lifecycle);
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(1, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
    }

    @Test
    public void completedChildAndUnconsumedTailCloseWhenLaterPreparationFails() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        IllegalStateException prepareFailure = new IllegalStateException("second prepare failure");
        TrackingPrepared first = resultChild(CommandResult.reply(RedisReplies.simpleString("ONE")));
        CommandDispatcher dispatcher = transactionDispatcher(registration -> {
            registration.register(spec("FIRST", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> first));
            registration.register(spec("SECOND", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> {
                        throw prepareFailure;
                    }));
        });
        PreparedCommand exec = prepare(dispatcher, new TrackingSession(tx), "EXEC");

        ResultUnknownException thrown = Assert.assertThrows(
                ResultUnknownException.class,
                () -> executeResult(exec, tx)
        );

        Assert.assertSame(prepareFailure, thrown.getCause());
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
    }

    @Test
    public void execCloseIsReverseExhaustiveAndSuppressesLaterFailures() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
        List<String> lifecycle = new ArrayList<>();
        IllegalStateException secondChildFailure = new IllegalStateException("second child close");
        IllegalStateException secondRequestFailure = new IllegalStateException("second request close");
        IllegalStateException firstChildFailure = new IllegalStateException("first child close");
        IllegalStateException firstRequestFailure = new IllegalStateException("first request close");
        TrackingPrepared first = resultChild(CommandResult.reply(RedisReplies.simpleString("ONE")));
        first.onClose = () -> lifecycle.add("child:first");
        first.closeFailure = firstChildFailure;
        TrackingPrepared second = resultChild(CommandResult.reply(RedisReplies.simpleString("TWO")));
        second.onClose = () -> lifecycle.add("child:second");
        second.closeFailure = secondChildFailure;
        tx.request(0).onClose = () -> lifecycle.add("request:first");
        tx.request(0).closeFailure = firstRequestFailure;
        tx.request(1).onClose = () -> lifecycle.add("request:second");
        tx.request(1).closeFailure = secondRequestFailure;
        PreparedCommand exec = prepareExec(tx, first, second);
        executeResult(exec, tx);

        IllegalStateException thrown = Assert.assertThrows(
                IllegalStateException.class,
                exec::close
        );

        Assert.assertSame(secondChildFailure, thrown);
        Assert.assertArrayEquals(
                new Throwable[]{secondRequestFailure, firstChildFailure, firstRequestFailure},
                thrown.getSuppressed());
        Assert.assertEquals(List.of(
                "child:second", "request:second", "child:first", "request:first"), lifecycle);
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(1, second.closeCount());
        Assert.assertEquals(1, tx.request(0).closeCount());
        Assert.assertEquals(1, tx.request(1).closeCount());
        exec.close();
        Assert.assertEquals(1, first.closeCount());
        Assert.assertEquals(1, second.closeCount());
    }

    @Test
    public void exactPreparationFailureContinuesClosingRetainedChildrenWhenCloseRethrowsPrimary() {
        TrackingTransactionState tx = transactionWith("FIRST", "SECOND", "THIRD");
        tx.reportedSize = 1;
        IllegalStateException preparationFailure = new IllegalStateException("prepare failure");
        TrackingPrepared first = resultChild(CommandResult.reply(RedisReplies.simpleString("ONE")));
        TrackingPrepared second = resultChild(CommandResult.reply(RedisReplies.simpleString("TWO")));
        second.closeFailure = preparationFailure;
        CommandDispatcher dispatcher = transactionDispatcher(registration -> {
            registration.register(spec("FIRST", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> first));
            registration.register(spec("SECOND", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> second));
            registration.register(spec("THIRD", CommandArity.exact(1), TransactionPolicy.QUEUEABLE,
                    args -> session -> {
                        throw preparationFailure;
                    }));
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
        Assert.assertEquals(1, tx.request(2).closeCount());
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

    private static TrackingPrepared resultChild(CommandResult result) {
        return executingChild(result.reply().shape(), context -> result);
    }

    private static TrackingPrepared executingChild(
            ReplyShape shape,
            Function<CommandExecutionContext, CommandResult> execution
    ) {
        return new TrackingPrepared(shape, execution);
    }

    private static CommandResult executeResult(
            PreparedCommand command,
            TrackingTransactionState tx
    ) {
        return executeResult(command, new TrackingSession(tx), trackingRequest("EXEC"));
    }

    private static CommandResult executeResult(
            PreparedCommand command,
            CommandSession session,
            ExecutionRequest request
    ) {
        Assert.assertEquals(ValidationResult.VALID, command.validateBeforeExecute());
        try (request; CommandExecutionContext context = CommandExecutionContext.forRequest(session, request)) {
            return command.execute(context);
        }
    }

    private static RedisReply.Aggregate aggregate(CommandResult result) {
        Assert.assertTrue(result.reply() instanceof RedisReply.Aggregate);
        return (RedisReply.Aggregate) result.reply();
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

    private static CommandSyntax syntax(String name) {
        return new CommandSyntax(
                name,
                CommandArity.exact(1),
                CommandKeySpec.NONE,
                TransactionPolicy.QUEUEABLE
        );
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
        return PreparedCommands.ready(
                RedisReplies.simpleString(reply)
        );
    }

    private static PreparedCommand prepared(
            ReplyShape shape,
            Function<CommandExecutionContext, CommandResult> execution
    ) {
        return new PreparedCommand() {
            @Override public ReplyShape reservationShape() { return shape; }
            @Override public ValidationResult validateBeforeExecute() { return ValidationResult.VALID; }
            @Override public CommandResult execute(CommandExecutionContext context) {
                return execution.apply(context);
            }
            @Override public void close() { }
        };
    }

    private static void executePrepared(
            CommandDispatcher dispatcher,
            CommandSession session,
            ExecutionRequest request
    ) {
        try (request;
             PreparedCommand prepared = dispatcher.prepare(session, request)) {
            Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
            try (CommandExecutionContext context = CommandExecutionContext.forRequest(session, request)) {
                prepared.execute(context);
            }
        }
    }

    private static PreparedCommand throwingClose(RuntimeException failure) {
        return new PreparedCommand() {
            @Override
            public ReplyShape reservationShape() {
                return ReplyShapes.simpleString("OK");
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return ValidationResult.VALID;
            }

            @Override
            public CommandResult execute(CommandExecutionContext context) {
                return CommandResult.reply(RedisReplies.simpleString("OK"));
            }

            @Override
            public void close() {
                throw failure;
            }
        };
    }

    private static CapturedReply execute(
            PreparedCommand prepared,
            CommandSession session,
            ExecutionRequest request
    ) {
        Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
        try (CommandExecutionContext context = CommandExecutionContext.forRequest(session, request)) {
            return new CapturedReply(prepared.execute(context));
        }
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

    private record CapturedReply(CommandResult result) {
        private String simpleString() {
            return result.reply() instanceof RedisReply.SimpleString reply
                    ? reply.value()
                    : null;
        }

        private String error() {
            if (result.reply() instanceof RedisReply.Error reply) {
                return reply.message();
            }
            if (result.reply() instanceof RedisReply.ControlError reply) {
                return reply.message();
            }
            return null;
        }
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
        private Runnable onClose = () -> { };

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
            onClose.run();
            delegate.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        private int closeCount() { return closeCount; }
    }

    private static final class TrackingPrepared implements PreparedCommand {
        private final ReplyShape shape;
        private final Function<CommandExecutionContext, CommandResult> execution;
        private boolean stale;
        private RuntimeException validationFailure;
        private RuntimeException closeFailure;
        private Runnable onClose = () -> { };
        private int closeCount;

        private TrackingPrepared(
                ReplyShape shape,
                Function<CommandExecutionContext, CommandResult> execution
        ) {
            this.shape = shape;
            this.execution = execution;
        }

        @Override public ReplyShape reservationShape() {
            return shape;
        }
        @Override public ValidationResult validateBeforeExecute() {
            if (validationFailure != null) {
                throw validationFailure;
            }
            return stale ? ValidationResult.STALE : ValidationResult.VALID;
        }
        @Override public CommandResult execute(CommandExecutionContext context) {
            return execution.apply(context);
        }
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
        private int dbIndex;

        private RecordingSession(boolean active) {
            tx = new RecordingTransactionState(active);
        }

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

    private static final class TrackingSource implements AutoCloseable {
        private final byte[] value;
        private int closeCount;

        private TrackingSource(byte[] value) {
            this.value = value;
        }

        private void emit(ReplySink sink) {
            Assert.assertEquals(0, closeCount);
            sink.bulkString(value);
        }

        @Override
        public void close() {
            closeCount++;
        }

        private int closeCount() {
            return closeCount;
        }
    }

    private static final class CapturingBulkSink implements ReplySink {
        private byte[] bytes;

        @Override
        public void bulkString(byte[] data) {
            bytes = data.clone();
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            bytes = java.util.Arrays.copyOfRange(data, off, off + len);
        }

        @Override
        public void bulkString(BytesSlice slice) {
            throw new UnsupportedOperationException("slice emission is not used by this test");
        }

        @Override
        public void bulkStringLongAscii(long value) {
            bytes = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
        }

        private byte[] bytes() {
            return bytes;
        }
    }
}
