package yier.bubu.redis.execution.api;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class PreparedCommandsTest {
    @Test
    public void readyReplyReturnsUsingItsOwnShape() {
        RedisReply reply = RedisReplies.simpleString("OK");
        PreparedCommand prepared = PreparedCommands.ready(reply);

        Assert.assertEquals(ReplyShapes.simpleString("OK"), prepared.reservationShape());
        Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
        CommandResult result = execute(prepared);
        Assert.assertSame(reply, result.reply());
        Assert.assertFalse(result.closeAfterReply());
    }

    @Test
    public void readyResultReturnsItsCloseAfterReplyFlag() {
        CommandResult readyResult = CommandResult.closeAfterReply(
                RedisReplies.simpleString("BYE"));
        PreparedCommand prepared = PreparedCommands.ready(readyResult);

        Assert.assertEquals(ReplyShapes.simpleString("BYE"), prepared.reservationShape());
        Assert.assertSame(readyResult, execute(prepared));
    }

    @Test
    public void actionUsesReservationShapeAndRunsOnce() {
        ReplyShape reservationShape = ReplyShapes.integerUpperBound();
        AtomicInteger executed = new AtomicInteger();
        PreparedCommand prepared = PreparedCommands.action(reservationShape, context -> {
            executed.incrementAndGet();
            return CommandResult.reply(RedisReplies.integer(7));
        });

        Assert.assertSame(reservationShape, prepared.reservationShape());
        Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
        Assert.assertEquals(RedisReplies.integer(7), execute(prepared).reply());
        Assert.assertEquals(1, executed.get());
    }

    @Test
    public void ownedReturnsAndClosesExactlyOnce() {
        AtomicInteger closed = new AtomicInteger();
        PreparedCommand prepared = PreparedCommands.owned(
                CommandResult.reply(RedisReplies.integer(2)),
                closed::incrementAndGet);

        Assert.assertEquals(RedisReplies.integer(2), execute(prepared).reply());
        prepared.close();
        prepared.close();

        Assert.assertEquals(1, closed.get());
    }

    @Test
    public void ownedActionValidatesExecutesAndClosesExactlyOnce() {
        AtomicInteger validated = new AtomicInteger();
        AtomicInteger executed = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        PreparedCommand prepared = PreparedCommands.ownedAction(
                ReplyShapes.integerUpperBound(),
                closed::incrementAndGet,
                () -> {
                    validated.incrementAndGet();
                    return ValidationResult.VALID;
                },
                context -> {
                    executed.incrementAndGet();
                    return CommandResult.reply(RedisReplies.integer(3));
                }
        );

        Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
        Assert.assertEquals(RedisReplies.integer(3), execute(prepared).reply());
        prepared.close();
        prepared.close();

        Assert.assertEquals(1, validated.get());
        Assert.assertEquals(1, executed.get());
        Assert.assertEquals(1, closed.get());
    }

    @Test
    public void staleValidationDoesNotRunAction() {
        AtomicInteger executed = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        PreparedCommand prepared = PreparedCommands.ownedAction(
                ReplyShapes.integerUpperBound(),
                closed::incrementAndGet,
                () -> ValidationResult.STALE,
                context -> {
                    executed.incrementAndGet();
                    return CommandResult.reply(RedisReplies.integer(1));
                }
        );

        Assert.assertEquals(ValidationResult.STALE, prepared.validateBeforeExecute());
        Assert.assertEquals(0, executed.get());
        prepared.close();
        Assert.assertEquals(1, closed.get());
    }

    @Test
    public void nullActionResultFails() {
        PreparedCommand prepared = PreparedCommands.action(
                ReplyShapes.integerUpperBound(),
                context -> null);
        Assert.assertThrows(NullPointerException.class,
                () -> execute(prepared));
    }

    @Test
    public void actionFailureLeavesOwnerOpenUntilPreparedCommandCloses() {
        AtomicInteger closed = new AtomicInteger();
        IllegalArgumentException failure = new IllegalArgumentException("action failed");
        PreparedCommand prepared = PreparedCommands.ownedAction(
                ReplyShapes.integerUpperBound(),
                closed::incrementAndGet,
                () -> ValidationResult.VALID,
                context -> {
                    throw failure;
                }
        );

        IllegalArgumentException actual = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> execute(prepared));
        Assert.assertSame(failure, actual);
        Assert.assertEquals(0, closed.get());

        prepared.close();
        Assert.assertEquals(1, closed.get());
    }

    @Test
    public void checkedOwnerCloseFailureBecomesIllegalStateException() {
        Exception failure = new Exception("checked close failed");
        PreparedCommand prepared = PreparedCommands.owned(
                CommandResult.reply(RedisReplies.integer(1)),
                () -> {
                    throw failure;
                });

        IllegalStateException actual = Assert.assertThrows(
                IllegalStateException.class,
                prepared::close);

        Assert.assertSame(failure, actual.getCause());
    }

    @Test
    public void closeFailureDoesNotRetryOwnerClose() {
        AtomicInteger closeAttempts = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("close failed");
        PreparedCommand prepared = PreparedCommands.owned(
                CommandResult.reply(RedisReplies.integer(1)),
                () -> {
                    closeAttempts.incrementAndGet();
                    throw failure;
                });

        IllegalStateException actual = Assert.assertThrows(
                IllegalStateException.class,
                prepared::close);
        Assert.assertSame(failure, actual);

        prepared.close();
        Assert.assertEquals(1, closeAttempts.get());
    }

    private static CommandResult execute(PreparedCommand prepared) {
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("TEST", List.of());
        try (request;
             CommandExecutionContext context = CommandExecutionContext.forRequest(
                     new TestSession(), request)) {
            return prepared.execute(context);
        }
    }

    private static final class TestSession implements CommandSession {
        private final TransactionState transaction = new TestTransactionState();

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
            return transaction;
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

    private static final class TestTransactionState implements TransactionState {
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
        public String tryEnqueue(ExecutionRequest request) {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void forEachQueued(
                java.util.function.Consumer<? super ExecutionRequest> visitor
        ) {
        }

        @Override
        public List<ExecutionRequest> drain() {
            return List.of();
        }

        @Override
        public void discard() {
        }

        @Override
        public void close() {
        }
    }
}
