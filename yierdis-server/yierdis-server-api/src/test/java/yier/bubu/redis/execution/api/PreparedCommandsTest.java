package yier.bubu.redis.execution.api;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;

public class PreparedCommandsTest {
    @Test
    public void readyReplyRendersUsingItsOwnShape() {
        PreparedCommand prepared = PreparedCommands.ready(RedisReplies.simpleString("OK"));

        Assert.assertEquals(ReplyShapes.simpleString("OK"), prepared.replyShape());
        Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
        Assert.assertEquals(List.of("simple:OK"), executeWithRecordingWriter(prepared).events());
    }

    @Test
    public void readyResultRequestsCloseAfterRendering() {
        PreparedCommand prepared = PreparedCommands.ready(
                CommandResult.closeAfterReply(RedisReplies.simpleString("BYE")));

        Assert.assertEquals(ReplyShapes.simpleString("BYE"), prepared.replyShape());
        Assert.assertEquals(List.of("simple:BYE", "close"),
                executeWithRecordingWriter(prepared).events());
    }

    @Test
    public void actionUsesReservationShapeAndRunsOnce() {
        ReplyShape reservationShape = ReplyShapes.integerUpperBound();
        AtomicInteger executed = new AtomicInteger();
        PreparedCommand prepared = PreparedCommands.action(reservationShape, context -> {
            executed.incrementAndGet();
            return CommandResult.reply(RedisReplies.integer(7));
        });

        Assert.assertSame(reservationShape, prepared.replyShape());
        Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
        Assert.assertEquals(List.of("integer:7"), executeWithRecordingWriter(prepared).events());
        Assert.assertEquals(1, executed.get());
    }

    @Test
    public void ownedRendersAndClosesExactlyOnce() {
        AtomicInteger closed = new AtomicInteger();
        PreparedCommand prepared = PreparedCommands.owned(
                CommandResult.reply(RedisReplies.integer(2)),
                closed::incrementAndGet);

        Assert.assertEquals(List.of("integer:2"), executeWithRecordingWriter(prepared).events());
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
        Assert.assertEquals(List.of("integer:3"), executeWithRecordingWriter(prepared).events());
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
    public void nullActionResultFailsBeforeRendering() {
        PreparedCommand prepared = PreparedCommands.action(
                ReplyShapes.integerUpperBound(),
                context -> null);
        RecordingWriter writer = new RecordingWriter();

        Assert.assertThrows(NullPointerException.class,
                () -> executeWithRecordingWriter(prepared, writer));
        Assert.assertEquals(List.of(), writer.events());
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
                () -> executeWithRecordingWriter(prepared));
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

    private static RecordingWriter executeWithRecordingWriter(PreparedCommand prepared) {
        RecordingWriter writer = new RecordingWriter();
        executeWithRecordingWriter(prepared, writer);
        return writer;
    }

    private static void executeWithRecordingWriter(
            PreparedCommand prepared,
            RecordingWriter writer
    ) {
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("TEST", List.of());
        try (request;
             CommandExecutionContext context = CommandExecutionContext.forRequest(
                     new TestSession(), writer, request)) {
            prepared.execute(context);
        }
    }

    private static final class RecordingWriter implements RedisReplyWriter {
        private final List<String> events = new ArrayList<>();

        List<String> events() {
            return List.copyOf(events);
        }

        @Override
        public void requestCloseAfterReply() {
            events.add("close");
        }

        @Override
        public boolean closeAfterReplyRequested() {
            return events.contains("close");
        }

        @Override
        public void simpleString(String value) {
            events.add("simple:" + value);
        }

        @Override
        public void error(String message) {
            events.add("error:" + message);
        }

        @Override
        public void integer(long value) {
            events.add("integer:" + value);
        }

        @Override
        public void booleanValue(boolean value) {
            events.add("boolean:" + value);
        }

        @Override
        public void doubleValue(double value) {
            events.add("double:" + value);
        }

        @Override
        public void bigNumberAscii(String value) {
            events.add("big-number:" + value);
        }

        @Override
        public void verbatimString(String format, byte[] data) {
            events.add("verbatim:" + format + ':'
                    + new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void blobError(String message) {
            events.add("blob-error:" + message);
        }

        @Override
        public void nullValue() {
            events.add("null");
        }

        @Override
        public void nullArray() {
            events.add("null-array");
        }

        @Override
        public void arrayHeader(int count) {
            events.add("array:" + count);
        }

        @Override
        public void emptyArray() {
            events.add("empty-array");
        }

        @Override
        public void mapHeader(int pairs) {
            events.add("map:" + pairs);
        }

        @Override
        public void setHeader(int count) {
            events.add("set:" + count);
        }

        @Override
        public void pushHeader(int count) {
            events.add("push:" + count);
        }

        @Override
        public void attributeHeader(int pairs) {
            events.add("attribute:" + pairs);
        }

        @Override
        public void bulkString(byte[] data) {
            events.add(data == null
                    ? "bulk:null"
                    : "bulk:" + new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            events.add("bulk:" + new String(data, off, len, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            slice.writeTo(bytes::write);
            events.add("bulk:" + bytes.toString(StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            events.add("bulk:" + value);
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
