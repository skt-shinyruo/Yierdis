package yier.bubu.redis.execution.api;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;

public class CoreContractSmokeTest {
    @Test
    public void executionContextExposesItsSession() {
        NarrowSession session = new NarrowSession();
        CommandExecutionContext context = CommandExecutionContext.forSession(session);

        Assert.assertSame(session, context.session());
    }

    @Test
    public void preparedExecutionReturnsACommandResultWithoutAWriterContext() throws Exception {
        Assert.assertTrue(AutoCloseable.class.isAssignableFrom(PreparedCommand.class));
        Assert.assertArrayEquals(
                new String[]{"close", "execute", "reservationShape", "validateBeforeExecute"},
                java.util.Arrays.stream(PreparedCommand.class.getDeclaredMethods())
                        .map(Method::getName)
                        .sorted()
                        .toArray(String[]::new)
        );
        Assert.assertEquals(ReplyShape.class,
                PreparedCommand.class.getMethod("reservationShape").getReturnType());
        Assert.assertEquals(CommandResult.class,
                PreparedCommand.class.getMethod(
                        "execute", CommandExecutionContext.class).getReturnType());
        Assert.assertThrows(NoSuchMethodException.class,
                () -> PreparedCommand.class.getMethod("replyShape"));
        Assert.assertThrows(NoSuchMethodException.class,
                () -> CommandExecutionContext.class.getMethod("reply"));
        Assert.assertArrayEquals(
                new ValidationResult[]{ValidationResult.VALID, ValidationResult.STALE},
                ValidationResult.values()
        );
    }

    @Test
    public void replyWriterOnlyRendersSemanticReplies() {
        List<String> legacyMethods = List.of(
                "require" + "Reply",
                "require" + "ReplyEnvelope",
                "transfer" + "ReplyOwnership",
                "writeMeasured" + "BulkStringArray",
                "writeMeasured" + "BulkStringMap"
        );
        List<String> legacyNestedTypes = List.of("Measured" + "ReplyVisitor");

        Assert.assertFalse(
                java.util.Arrays.stream(RedisReplyWriter.class.getMethods())
                        .map(Method::getName)
                        .anyMatch(legacyMethods::contains)
        );
        Assert.assertFalse(
                java.util.Arrays.stream(RedisReplyWriter.class.getDeclaredClasses())
                        .map(Class::getSimpleName)
                        .anyMatch(legacyNestedTypes::contains)
        );
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
