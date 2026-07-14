package yier.bubu.redis.execution.engine;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.common.command.CommandRecordScope;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ClientMetadataSession;
import yier.bubu.redis.execution.api.ConnectionStatsSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.DbIndexSession;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ProtocolNegotiationSession;
import yier.bubu.redis.execution.api.Session;
import yier.bubu.redis.execution.api.TransactionSession;
import yier.bubu.redis.execution.api.TransactionState;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class DefaultYierdisEngineTest {
    @Test
    public void commandModulesDoNotReachThroughFullCommandContextSession() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to locate repository root", repoRoot);

        ArrayList<Path> offenders = new ArrayList<>();
        assertNoFullSessionAccess(
                repoRoot.resolve("yierdis-command/yierdis-command-core/src/main/java"),
                offenders
        );
        assertNoFullSessionAccess(
                repoRoot.resolve("yierdis-command/yierdis-command-builtin/src/main/java"),
                offenders
        );

        Assert.assertTrue("ctx.session() usages must move to narrow CommandContext getters: " + offenders, offenders.isEmpty());
    }

    @Test
    public void executeDelegatesThroughOwnedCommandProcessor() {
        CommandRegistry registry = CommandRegistries.from(
                registration -> registration.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("LOCAL_OK")
                )
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        YierdisEngine engine = new DefaultYierdisEngine(processor, () -> {
        });

        CapturingReplyWriter out = new CapturingReplyWriter();
        engine.execute(
                new EngineSession(16, 1024),
                ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()),
                out
        );

        Assert.assertEquals("LOCAL_OK", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    @Test
    public void executeOpensTheRequestRecordScopeAndRestoresItAfterFailures() {
        CommandRegistry registry = CommandRegistries.from(
                registration -> {
                    registration.register(
                            "SCOPED",
                            CommandDescriptor.of(1, 0, 0, 0),
                            CommandParsers.exactRequest(1, "scoped"),
                            (request, ctx) -> {
                                Assert.assertSame(request, CommandRecordScope.current());
                                ctx.out().simpleString("OK");
                            }
                    );
                    registration.register(
                            "FAIL",
                            CommandDescriptor.of(1, 0, 0, 0),
                            CommandParsers.exactRequest(1, "fail"),
                            (request, ctx) -> {
                                Assert.assertSame(request, CommandRecordScope.current());
                                throw new IllegalStateException("injected");
                            }
                    );
                }
        );
        YierdisEngine engine = new DefaultYierdisEngine(new YierdisFastCommandProcessor(registry), () -> {
        });
        EngineSession session = new EngineSession(16, 1024);

        engine.execute(session, ByteArrayExecutionRequest.fromUtf8("SCOPED", List.of()), new CapturingReplyWriter());
        Assert.assertNull(CommandRecordScope.current());

        Assert.assertThrows(
                IllegalStateException.class,
                () -> engine.execute(session, ByteArrayExecutionRequest.fromUtf8("FAIL", List.of()), new CapturingReplyWriter())
        );
        Assert.assertNull(CommandRecordScope.current());
    }

    @Test
    public void transactionReplayUsesTheQueuedRequestAsItsCurrentRecord() {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor::execute);
        registry.register(
                "SCOPED",
                CommandDescriptor.of(1, 0, 0, 0),
                CommandParsers.exactRequest(1, "scoped"),
                (request, ctx) -> {
                    Assert.assertSame(request, CommandRecordScope.current());
                    ctx.out().simpleString("OK");
                }
        );
        YierdisEngine engine = new DefaultYierdisEngine(processor, () -> {
        });
        EngineSession session = new EngineSession(16, 1024);
        CapturingReplyWriter out = new CapturingReplyWriter();

        engine.execute(session, ByteArrayExecutionRequest.fromUtf8("MULTI", List.of()), out);
        engine.execute(session, ByteArrayExecutionRequest.fromUtf8("SCOPED", List.of()), out);
        engine.execute(session, ByteArrayExecutionRequest.fromUtf8("EXEC", List.of()), out);

        Assert.assertNull(CommandRecordScope.current());
    }

    @Test
    public void executeAcceptsNarrowCommandSessionCapabilities() {
        CommandRegistry registry = CommandRegistries.from(
                registration -> registration.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("DB_" + ctx.dbIndexSession().dbIndex())
                )
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        YierdisEngine engine = new DefaultYierdisEngine(processor, () -> {
        });

        NarrowEngineSession session = new NarrowEngineSession();
        session.setDbIndex(7);
        CapturingReplyWriter out = new CapturingReplyWriter();

        engine.execute(
                session,
                ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()),
                out
        );

        Assert.assertEquals("DB_7", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    @Test
    public void executeRejectsSessionWithoutCommandCapabilitiesBeforeCommandModulesRun() {
        CommandRegistry registry = CommandRegistries.from(
                registration -> registration.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("LOCAL_OK")
                )
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        YierdisEngine engine = new DefaultYierdisEngine(processor, () -> {
        });

        CapturingReplyWriter out = new CapturingReplyWriter();
        try {
            engine.execute(
                    new Session() {
                    },
                    ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()),
                    out
            );
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("YierdisEngine requires command session capabilities", e.getMessage());
        }

        Assert.assertNull(out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    @Test
    public void maintenanceTickDelegatesToOwnerThreadRuntimeHook() {
        AtomicInteger ticks = new AtomicInteger();
        YierdisEngine engine = new DefaultYierdisEngine(
                new YierdisFastCommandProcessor(new CommandRegistry()),
                ticks::incrementAndGet
        );

        engine.maintenanceTick();
        engine.maintenanceTick();

        Assert.assertEquals(2, ticks.get());
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

    private static final class NarrowEngineSession implements DbIndexSession,
            ClientMetadataSession,
            TransactionSession,
            ConnectionStatsSession,
            ProtocolNegotiationSession {
        private final TransactionState tx = new TransactionState() {
            @Override
            public boolean active() {
                return false;
            }

            @Override
            public void begin() {
            }

            @Override
            public void discard() {
            }

            @Override
            public void enqueue(yier.bubu.redis.execution.api.ExecutionRequest request) {
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public List<yier.bubu.redis.execution.api.ExecutionRequest> drain() {
                return List.of();
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

    private static void assertNoFullSessionAccess(Path sourceRoot, List<Path> offenders) throws IOException {
        if (!Files.isDirectory(sourceRoot)) {
            return;
        }
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .filter(DefaultYierdisEngineTest::containsFullSessionAccess)
                    .map(sourceRoot::relativize)
                    .forEach(offenders::add);
        }
    }

    private static boolean containsFullSessionAccess(Path path) {
        try {
            return Files.readString(path).contains("ctx.session()");
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
