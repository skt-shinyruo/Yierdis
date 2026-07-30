package yier.bubu.redis.command.defaults;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.RedisReplyRenderer;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.storage.api.result.ByteMapSource;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.api.result.ByteValueSink;

public class DbRepliesTest {
    @Test
    public void sequenceReplyStreamsThroughReplySinkAndClosesWithPreparedOwner() {
        TrackingSequence source = new TrackingSequence(bytes("a"), null, bytes("ccc"));
        RedisReply reply = DbReplies.sequence(source);
        PreparedCommand prepared = PreparedCommands.owned(CommandResult.reply(reply), source);

        Assert.assertEquals(0, source.emitCount());
        ReplyShape.ByteSequence shape = (ReplyShape.ByteSequence) reply.shape();
        Assert.assertEquals(3, shape.elementCount());
        shape.payloadLengths().visit(ignored -> { });
        Assert.assertEquals(3, source.lengthVisitCount());
        Assert.assertEquals(0, source.emitCount());
        Assert.assertEquals(List.of("array:3", "bulk:a", "bulk:null", "bulk:ccc"), render(prepared));
        Assert.assertEquals(1, source.emitCount());
        prepared.close();
        prepared.close();
        Assert.assertEquals(1, source.closeCount());
    }

    @Test
    public void valueRepliesPreserveNullHeapSliceLongAndOwnedValues() {
        assertValueReply(ByteValue.nullValue(), ReplyShapes.nullValue(), List.of("null"));
        assertValueReply(ByteValue.bytes(bytes("xheap"), 1, 4),
                ReplyShapes.bulkString(4, 0L), List.of("bulk:heap"));
        assertValueReply(ByteValue.slice(new ArraySlice(bytes("slice"))),
                ReplyShapes.bulkString(5, 0L), List.of("bulk-slice:slice"));
        assertValueReply(ByteValue.longAscii(-42L),
                ReplyShapes.bulkString(3, 0L), List.of("bulk-long:-42"));

        AtomicInteger ownerClosed = new AtomicInteger();
        ByteValue owned = ByteValue.owned(new ArraySlice(bytes("owned")), 5, 11L,
                ownerClosed::incrementAndGet);
        RedisReply reply = DbReplies.value(owned);
        PreparedCommand prepared = PreparedCommands.owned(CommandResult.reply(reply), owned);

        Assert.assertEquals(ReplyShapes.bulkString(5, 11L), reply.shape());
        Assert.assertEquals(List.of("bulk-slice:owned"), render(prepared));
        Assert.assertEquals(0, ownerClosed.get());
        prepared.close();
        prepared.close();
        Assert.assertEquals(1, ownerClosed.get());
    }

    @Test
    public void mapReplyUsesPairCountAndStreamsOnlyWhenRendered() {
        TrackingMap source = new TrackingMap(bytes("field"), bytes("value"), bytes("next"), null);
        RedisReply reply = DbReplies.map(source);
        PreparedCommand prepared = PreparedCommands.owned(CommandResult.reply(reply), source);

        Assert.assertEquals(0, source.emitCount());
        ReplyShape.ByteMap shape = (ReplyShape.ByteMap) reply.shape();
        Assert.assertEquals(2, shape.pairCount());
        shape.payloadLengths().visit(ignored -> { });
        Assert.assertEquals(4, source.lengthVisitCount());
        Assert.assertEquals(0, source.emitCount());
        Assert.assertEquals(List.of("map:2", "bulk:field", "bulk:value", "bulk:next", "bulk:null"),
                render(prepared));
        Assert.assertEquals(1, source.emitCount());
        prepared.close();
        Assert.assertEquals(1, source.closeCount());
    }

    @Test
    public void renderFailureLeavesSequenceSourceForPreparedOwnerToClose() {
        TrackingSequence source = new TrackingSequence(bytes("value"));
        source.failOnEmit(new IllegalStateException("render failed"));
        PreparedCommand prepared = PreparedCommands.owned(
                CommandResult.reply(DbReplies.sequence(source)), source);

        IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                () -> render(prepared));
        Assert.assertEquals("render failed", failure.getMessage());
        Assert.assertEquals(0, source.closeCount());
        prepared.close();
        Assert.assertEquals(1, source.closeCount());
    }

    @Test
    public void stalePreparedReplyClosesSequenceSourceWithoutRendering() {
        TrackingSequence source = new TrackingSequence(bytes("value"));
        PreparedCommand prepared = PreparedCommands.ownedAction(
                DbReplies.sequence(source).shape(), source,
                () -> ValidationResult.STALE,
                context -> CommandResult.reply(DbReplies.sequence(source)));

        Assert.assertEquals(ValidationResult.STALE, prepared.validateBeforeExecute());
        Assert.assertEquals(0, source.emitCount());
        prepared.close();
        prepared.close();
        Assert.assertEquals(0, source.emitCount());
        Assert.assertEquals(1, source.closeCount());
    }

    @Test
    public void sourceCloseFailureIsPropagatedOnlyOnce() {
        TrackingSequence source = new TrackingSequence(bytes("value"));
        IllegalStateException failure = new IllegalStateException("close failed");
        source.failOnClose(failure);
        PreparedCommand prepared = PreparedCommands.owned(
                CommandResult.reply(DbReplies.sequence(source)), source);

        Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, prepared::close));
        prepared.close();
        Assert.assertEquals(1, source.closeCount());
    }

    private static void assertValueReply(ByteValue value, ReplyShape shape, List<String> expectedEvents) {
        RedisReply reply = DbReplies.value(value);
        PreparedCommand prepared = PreparedCommands.owned(CommandResult.reply(reply), value);

        Assert.assertEquals(shape, reply.shape());
        Assert.assertEquals(expectedEvents, render(prepared));
        prepared.close();
    }

    private static List<String> render(PreparedCommand prepared) {
        List<String> events = new ArrayList<>();
        RedisReplyWriter writer = recordingWriter(events);
        CommandResult result;
        try (ByteArrayExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("TEST", List.of());
             CommandExecutionContext context = CommandExecutionContext.forRequest(
                     session(), request)) {
            result = prepared.execute(context);
        }
        RedisReplyRenderer.render(result.reply(), writer);
        return events;
    }

    private static RedisReplyWriter recordingWriter(List<String> events) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                return objectMethod(proxy, method.getName(), args);
            }
            return switch (method.getName()) {
                case "arrayHeader" -> add(events, "array:" + args[0]);
                case "mapHeader" -> add(events, "map:" + args[0]);
                case "nullValue" -> add(events, "null");
                case "bulkStringNull" -> add(events, "bulk:null");
                case "bulkString" -> recordBulkString(events, args);
                case "bulkStringLongAscii" -> add(events, "bulk-long:" + args[0]);
                default -> throw new UnsupportedOperationException(method.getName());
            };
        };
        return (RedisReplyWriter) Proxy.newProxyInstance(
                RedisReplyWriter.class.getClassLoader(), new Class<?>[]{RedisReplyWriter.class}, handler);
    }

    private static Object recordBulkString(List<String> events, Object[] args) {
        if (args[0] instanceof BytesSlice slice) {
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            events.add("bulk-slice:" + new String(data, StandardCharsets.US_ASCII));
            return null;
        }
        byte[] data = (byte[]) args[0];
        if (data == null) {
            events.add("bulk:null");
        } else if (args.length == 1) {
            events.add("bulk:" + new String(data, StandardCharsets.US_ASCII));
        } else {
            events.add("bulk:" + new String(data, (int) args[1], (int) args[2], StandardCharsets.US_ASCII));
        }
        return null;
    }

    private static Object add(List<String> events, String event) {
        events.add(event);
        return null;
    }

    private static Object objectMethod(Object proxy, String name, Object[] args) {
        return switch (name) {
            case "equals" -> proxy == args[0];
            case "hashCode" -> System.identityHashCode(proxy);
            case "toString" -> "recording writer";
            default -> throw new UnsupportedOperationException(name);
        };
    }

    private static CommandSession session() {
        return (CommandSession) Proxy.newProxyInstance(
                CommandSession.class.getClassLoader(), new Class<?>[]{CommandSession.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(proxy, method.getName(), args);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class TrackingSequence implements ByteSequenceSource {
        private final byte[][] values;
        private int emitCount;
        private int lengthVisitCount;
        private int closeCount;
        private RuntimeException emitFailure;
        private RuntimeException closeFailure;

        private TrackingSequence(byte[]... values) {
            this.values = values;
        }

        void failOnEmit(RuntimeException failure) {
            emitFailure = failure;
        }

        void failOnClose(RuntimeException failure) {
            closeFailure = failure;
        }

        int emitCount() {
            return emitCount;
        }

        int lengthVisitCount() {
            return lengthVisitCount;
        }

        int closeCount() {
            return closeCount;
        }

        @Override
        public int elementCount() {
            return values.length;
        }

        @Override
        public long retainedMemoryBytes() {
            return 0L;
        }

        @Override
        public void visitElementLengths(yier.bubu.redis.storage.api.result.PayloadLengthSink out) {
            for (byte[] value : values) {
                lengthVisitCount++;
                out.payloadLength(value == null ? -1 : value.length);
            }
        }

        @Override
        public void emitTo(ByteValueSink out) {
            emitCount++;
            if (emitFailure != null) {
                throw emitFailure;
            }
            for (byte[] value : values) {
                if (value == null) {
                    out.nullValue();
                } else {
                    out.value(value);
                }
            }
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class TrackingMap implements ByteMapSource {
        private final byte[][] values;
        private int emitCount;
        private int lengthVisitCount;
        private int closeCount;

        private TrackingMap(byte[]... values) {
            this.values = values;
        }

        int emitCount() {
            return emitCount;
        }

        int lengthVisitCount() {
            return lengthVisitCount;
        }

        int closeCount() {
            return closeCount;
        }

        @Override
        public int pairCount() {
            return values.length / 2;
        }

        @Override
        public long retainedMemoryBytes() {
            return 0L;
        }

        @Override
        public void visitPairLengths(yier.bubu.redis.storage.api.result.PayloadLengthSink out) {
            for (byte[] value : values) {
                lengthVisitCount++;
                out.payloadLength(value == null ? -1 : value.length);
            }
        }

        @Override
        public void emitPairsTo(ByteValueSink out) {
            emitCount++;
            for (byte[] value : values) {
                if (value == null) {
                    out.nullValue();
                } else {
                    out.value(value);
                }
            }
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

    private record ArraySlice(byte[] data) implements BytesSlice {
        @Override
        public int length() {
            return data.length;
        }

        @Override
        public byte getByte(int index) {
            return data[index];
        }

        @Override
        public void writeTo(BytesSink out) {
            out.writeBytes(data, 0, data.length);
        }
    }
}
