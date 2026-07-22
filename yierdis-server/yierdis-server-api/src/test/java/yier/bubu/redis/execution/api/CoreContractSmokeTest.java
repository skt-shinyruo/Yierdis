package yier.bubu.redis.execution.api;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;

public class CoreContractSmokeTest {
    @Test
    public void commandContextStoresCompleteCommandSession() throws Exception {
        Assert.assertEquals(
                "yier.bubu.redis.execution.api.CommandSession",
                CommandContext.class.getDeclaredField("session").getType().getName()
        );
    }

    @Test
    public void commandContextDoesNotExposeAggregateSessionCompatibilityAccessor() {
        for (Method method : CommandContext.class.getDeclaredMethods()) {
            Assert.assertFalse(
                    "CommandContext must expose narrow capability accessors instead of session(): " + method,
                    method.getName().equals("session") && method.getParameterCount() == 0
            );
        }
    }

    @Test
    public void commandContextCanBeBuiltFromNarrowCapabilitiesWithoutServerSession() {
        RedisReplyWriter writer = noopWriter();
        NarrowSession session = new NarrowSession();

        CommandContext ctx = new CommandContext(session, writer);

        ctx.dbIndexSession().setDbIndex(2);
        ctx.clientMetadataSession().setClientName(" client ");
        ctx.clientMetadataSession().setAuthenticated(true);
        ctx.protocolNegotiationSession().setRespVersion(3);

        Assert.assertSame(writer, ctx.out());
        Assert.assertEquals(2, ctx.dbIndexSession().dbIndex());
        Assert.assertEquals("client", ctx.clientMetadataSession().clientName());
        Assert.assertTrue(ctx.clientMetadataSession().authenticated());
        Assert.assertSame(session.transaction(), ctx.transactionSession().transaction());
        Assert.assertNull(ctx.connectionStatsSession().connectionStats());
        Assert.assertEquals(3, ctx.protocolNegotiationSession().respVersion());
    }

    @Test
    public void commandContextDoesNotKeepServerSessionCompatibilityEntrypoints() {
        for (var constructor : CommandContext.class.getDeclaredConstructors()) {
            for (Class<?> parameterType : constructor.getParameterTypes()) {
                Assert.assertNotEquals(
                        "CommandContext must not expose ServerSession compatibility constructors",
                        "yier.bubu.redis.execution.api.ServerSession",
                        parameterType.getName()
                );
            }
        }
        for (Method method : CommandContext.class.getDeclaredMethods()) {
            for (Class<?> parameterType : method.getParameterTypes()) {
                Assert.assertNotEquals(
                        "CommandContext must not expose ServerSession compatibility methods",
                        "yier.bubu.redis.execution.api.ServerSession",
                        parameterType.getName()
                );
            }
        }
    }

    @Test
    public void contractTypesComposeWithoutServerSessionAggregate() {
        RedisReplyWriter writer = noopWriter();
        NarrowSession session = new NarrowSession();

        CommandContext ctx = new CommandContext(session, writer);

        session.setDbIndex(4);
        session.setClientName("client");
        session.setAuthenticated(true);
        session.setRespVersion(3);

        Assert.assertSame(session, ctx.dbIndexSession());
        Assert.assertSame(session, ctx.clientMetadataSession());
        Assert.assertSame(session, ctx.transactionSession());
        Assert.assertSame(session, ctx.connectionStatsSession());
        Assert.assertSame(session, ctx.protocolNegotiationSession());
        Assert.assertSame(writer, ctx.out());
        Assert.assertEquals(4, ctx.dbIndexSession().dbIndex());
        Assert.assertEquals("client", ctx.clientMetadataSession().clientName());
        Assert.assertTrue(ctx.clientMetadataSession().authenticated());
        Assert.assertEquals(3, ctx.protocolNegotiationSession().respVersion());
        writer.requestCloseAfterReply();
        Assert.assertTrue(writer.closeAfterReplyRequested());
    }

    private static RedisReplyWriter noopWriter() {
        return new RedisReplyWriter() {
            private boolean closeAfter;

            @Override
            public void requestCloseAfterReply() {
                closeAfter = true;
            }

            @Override
            public boolean closeAfterReplyRequested() {
                return closeAfter;
            }

            @Override
            public void simpleString(String value) {
            }

            @Override
            public void error(String message) {
            }

            @Override
            public void integer(long value) {
            }

            @Override
            public void booleanValue(boolean value) {
            }

            @Override
            public void doubleValue(double value) {
            }

            @Override
            public void bigNumberAscii(String value) {
            }

            @Override
            public void verbatimString(String format, byte[] data) {
            }

            @Override
            public void blobError(String message) {
            }

            @Override
            public void nullValue() {
            }

            @Override
            public void nullArray() {
            }

            @Override
            public void arrayHeader(int count) {
            }

            @Override
            public void emptyArray() {
            }

            @Override
            public void mapHeader(int pairs) {
            }

            @Override
            public void setHeader(int count) {
            }

            @Override
            public void pushHeader(int count) {
            }

            @Override
            public void attributeHeader(int pairs) {
            }

            @Override
            public void bulkString(byte[] data) {
            }

            @Override
            public void bulkString(byte[] data, int off, int len) {
            }

            @Override
            public void bulkString(yier.bubu.redis.bytes.BytesSlice slice) {
            }

            @Override
            public void bulkStringLongAscii(long value) {
            }
        };
    }

    private static final class NarrowSession implements CommandSession {
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
            public void discard() {
            }

            @Override
            public void markAborted() {
            }

            @Override
            public String tryEnqueue(ExecutionRequest request) {
                return null;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public List<ExecutionRequest> drain() {
                return List.of();
            }

            @Override
            public void forEachQueued(java.util.function.Consumer<? super ExecutionRequest> visitor) {
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
            String name = clientName;
            if (name != null) {
                name = name.trim();
                if (name.isEmpty()) {
                    name = null;
                }
            }
            this.clientName = name;
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
}
