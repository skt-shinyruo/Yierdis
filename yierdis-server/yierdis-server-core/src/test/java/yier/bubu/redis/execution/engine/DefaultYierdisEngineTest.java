package yier.bubu.redis.execution.engine;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.ValidationResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class DefaultYierdisEngineTest {
    @Test
    public void commandModulesDoNotReferenceTheDeletedLegacyExecutionContext() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to locate repository root", repoRoot);

        ArrayList<Path> offenders = new ArrayList<>();
        assertNoLegacyContextReference(
                repoRoot.resolve("yierdis-command/yierdis-command-core/src/main/java"),
                offenders
        );
        assertNoLegacyContextReference(
                repoRoot.resolve("yierdis-command/yierdis-command-builtin/src/main/java"),
                offenders
        );

        Assert.assertTrue("legacy execution-context references must be removed: " + offenders, offenders.isEmpty());
    }

    @Test
    public void prepareDelegatesThroughOwnedCommandProcessor() {
        CommandRegistry registry = CommandRegistries.from(
                registration -> registration.register(new CommandDefinition<>(
                        syntax("LOCAL"),
                        CommandParsers.args(),
                        (request, preparation) -> preparedSimpleString("LOCAL_OK")
                ))
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        YierdisEngine engine = new DefaultYierdisEngine(processor, () -> {
        });

        CapturingReplyWriter out = new CapturingReplyWriter();
        executePrepared(
                engine,
                new EngineSession(16, 1024),
                ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()),
                out
        );

        Assert.assertEquals("LOCAL_OK", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    @Test
    public void preparedExecutionPassesTheRequestThroughTheExplicitMutationContext() {
        AtomicReference<MutationContext> successfulContext = new AtomicReference<>();
        AtomicReference<MutationContext> failedContext = new AtomicReference<>();
        CommandRegistry registry = CommandRegistries.from(
                registration -> {
                    registration.register(new CommandDefinition<>(
                            syntax("SCOPED"),
                            CommandParsers.args(),
                            (request, preparation) -> prepared(ReplyShapes.simpleString("OK"), context -> {
                                Assert.assertSame(request.request(), context.mutationContext().commandRecord());
                                successfulContext.set(context.mutationContext());
                                context.reply().simpleString("OK");
                            })
                    ));
                    registration.register(new CommandDefinition<>(
                            syntax("FAIL"),
                            CommandParsers.args(),
                            (request, preparation) -> prepared(ReplyShapes.simpleString("OK"), context -> {
                                Assert.assertSame(request.request(), context.mutationContext().commandRecord());
                                failedContext.set(context.mutationContext());
                                throw new IllegalStateException("injected");
                            })
                    ));
                }
        );
        YierdisEngine engine = new DefaultYierdisEngine(new YierdisFastCommandProcessor(registry), () -> {
        });
        EngineSession session = new EngineSession(16, 1024);

        executePrepared(
                engine,
                session,
                ByteArrayExecutionRequest.fromUtf8("SCOPED", List.of()),
                new CapturingReplyWriter()
        );
        Assert.assertFalse(successfulContext.get().hasCommandRecord());

        Assert.assertThrows(
                IllegalStateException.class,
                () -> executePrepared(
                        engine,
                        session,
                        ByteArrayExecutionRequest.fromUtf8("FAIL", List.of()),
                        new CapturingReplyWriter()
                )
        );
        Assert.assertFalse(failedContext.get().hasCommandRecord());
    }

    @Test
    public void transactionReplayUsesTheQueuedRequestAsItsCurrentRecord() {
        AtomicReference<MutationContext> replayContext = new AtomicReference<>();
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor);
        registry.register(new CommandDefinition<>(
                syntax("SCOPED"),
                CommandParsers.args(),
                (request, preparation) -> prepared(ReplyShapes.simpleString("OK"), context -> {
                    Assert.assertSame(request.request(), context.mutationContext().commandRecord());
                    replayContext.set(context.mutationContext());
                    context.reply().simpleString("OK");
                })
        ));
        registry.seal();
        YierdisEngine engine = new DefaultYierdisEngine(processor, () -> {
        });
        EngineSession session = new EngineSession(16, 1024);
        CapturingReplyWriter out = new CapturingReplyWriter();

        executePrepared(engine, session, ByteArrayExecutionRequest.fromUtf8("MULTI", List.of()), out);
        executePrepared(engine, session, ByteArrayExecutionRequest.fromUtf8("SCOPED", List.of()), out);
        executePrepared(engine, session, ByteArrayExecutionRequest.fromUtf8("EXEC", List.of()), out);

        Assert.assertFalse(replayContext.get().hasCommandRecord());
    }

    @Test
    public void prepareAcceptsCompleteCommandSession() {
        CommandRegistry registry = CommandRegistries.from(
                registration -> registration.register(new CommandDefinition<>(
                        syntax("LOCAL"),
                        CommandParsers.args(),
                        (request, preparation) -> prepared(
                                ReplyShapes.simpleString("DB_" + preparation.session().dbIndex()),
                                context -> context.reply().simpleString("DB_" + context.session().dbIndex())
                        )
                ))
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        YierdisEngine engine = new DefaultYierdisEngine(processor, () -> {
        });

        NarrowEngineSession session = new NarrowEngineSession();
        session.setDbIndex(7);
        CapturingReplyWriter out = new CapturingReplyWriter();

        executePrepared(
                engine,
                session,
                ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()),
                out
        );

        Assert.assertEquals("DB_7", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    @Test
    public void maintenanceTickDelegatesToOwnerThreadRuntimeHook() {
        AtomicInteger ticks = new AtomicInteger();
        CommandRegistry registry = new CommandRegistry();
        registry.seal();
        YierdisEngine engine = new DefaultYierdisEngine(
                new YierdisFastCommandProcessor(registry),
                ticks::incrementAndGet
        );

        engine.maintenanceTick();
        engine.maintenanceTick();

        Assert.assertEquals(2, ticks.get());
    }

    private static CommandSyntax syntax(String nameUpper) {
        return new CommandSyntax(
                nameUpper,
                CommandArity.exact(1),
                CommandKeySpec.NONE,
                TransactionPolicy.QUEUEABLE
        );
    }

    private static PreparedCommand preparedSimpleString(String value) {
        return prepared(
                ReplyShapes.simpleString(value),
                context -> context.reply().simpleString(value)
        );
    }

    private static PreparedCommand prepared(
            ReplyShape shape,
            Consumer<CommandExecutionContext> execution
    ) {
        return new PreparedCommand() {
            @Override
            public ReplyShape replyShape() {
                return shape;
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return ValidationResult.VALID;
            }

            @Override
            public void execute(CommandExecutionContext context) {
                execution.accept(context);
            }

            @Override
            public void close() {
            }
        };
    }

    private static void executePrepared(
            YierdisEngine engine,
            CommandSession session,
            ExecutionRequest request,
            RedisReplyWriter reply
    ) {
        try (request;
             PreparedCommand prepared = engine.prepare(session, request)) {
            Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
            try (CommandExecutionContext context = CommandExecutionContext.forRequest(session, reply, request)) {
                prepared.execute(context);
            }
        }
    }

    private static final class CapturingReplyWriter implements RedisReplyWriter {
        private String simpleStringValue;
        private String errorValue;

        @Override
        public void requestCloseAfterReply() {
        }

        @Override
        public boolean closeAfterReplyRequested() {
            return false;
        }

        @Override
        public void simpleString(String value) {
            this.simpleStringValue = value;
        }

        @Override
        public void error(String message) {
            this.errorValue = message;
        }

        @Override
        public void integer(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void booleanValue(boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void doubleValue(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bigNumberAscii(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void verbatimString(String format, byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void blobError(String message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nullValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nullArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void arrayHeader(int count) {
        }

        @Override
        public void emptyArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mapHeader(int pairs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setHeader(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pushHeader(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void attributeHeader(int pairs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkString(byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkString(BytesSlice slice) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkStringLongAscii(long value) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class NarrowEngineSession implements CommandSession {
        private final TransactionState tx = new TransactionState() {
            @Override
            public boolean active() {
                return false;
            }

            @Override
            public boolean aborted() {
                return false;
            }

            @Override
            public void begin() {
            }

            @Override
            public void markAborted() {
            }

            @Override
            public void discard() {
            }

            @Override
            public String tryEnqueue(yier.bubu.redis.execution.api.ExecutionRequest request) {
                return null;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public List<yier.bubu.redis.execution.api.ExecutionRequest> drain() {
                return List.of();
            }

            @Override
            public void forEachQueued(
                    java.util.function.Consumer<? super yier.bubu.redis.execution.api.ExecutionRequest> visitor
            ) {
            }

            @Override
            public void close() {
            }
        };

        private int dbIndex;
        private String clientName;
        private boolean authenticated;
        private int respVersion = 2;

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
        public ConnectionStatsView connectionStats() {
            return null;
        }

        @Override
        public int respVersion() {
            return respVersion;
        }

        @Override
        public void setRespVersion(int respVersion) {
            this.respVersion = respVersion;
        }
    }

    private static void assertNoLegacyContextReference(Path sourceRoot, List<Path> offenders) throws IOException {
        if (!Files.isDirectory(sourceRoot)) {
            return;
        }
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .filter(DefaultYierdisEngineTest::containsLegacyContextReference)
                    .map(sourceRoot::relativize)
                    .forEach(offenders::add);
        }
    }

    private static boolean containsLegacyContextReference(Path path) {
        try {
            return Files.readString(path).contains("Command" + "Context");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Path resolveRepoRoot() {
        Path path = Path.of("").toAbsolutePath();
        while (path != null) {
            if (Files.isDirectory(path.resolve("yierdis-server"))
                    && Files.isDirectory(path.resolve("yierdis-command"))) {
                return path;
            }
            path = path.getParent();
        }
        return null;
    }
}
