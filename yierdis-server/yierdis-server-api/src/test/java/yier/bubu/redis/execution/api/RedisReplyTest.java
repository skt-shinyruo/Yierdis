package yier.bubu.redis.execution.api;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import org.junit.Assert;
import org.junit.Test;

public class RedisReplyTest {
    @Test
    public void exactRepliesCarryTheirOwnDataAndShape() {
        byte[] verbatimData = bytes("body");
        byte[] bulkData = bytes("value");
        List<ReplyCase> cases = List.of(
                new ReplyCase(
                        RedisReplies.simpleString("OK"),
                        ReplyShapes.simpleString("OK"),
                        reply -> Assert.assertEquals("OK", ((RedisReply.SimpleString) reply).value())),
                new ReplyCase(
                        RedisReplies.error("wrong"),
                        ReplyShapes.error("wrong"),
                        reply -> Assert.assertEquals("wrong", ((RedisReply.Error) reply).message())),
                new ReplyCase(
                        RedisReplies.controlError("failed"),
                        ReplyShapes.maximum(),
                        reply -> Assert.assertEquals("failed", ((RedisReply.ControlError) reply).message())),
                new ReplyCase(
                        RedisReplies.integer(7),
                        ReplyShapes.integer(7),
                        reply -> Assert.assertEquals(7L, ((RedisReply.IntegerValue) reply).value())),
                new ReplyCase(
                        new RedisReply.BooleanValue(true),
                        ReplyShapes.booleanValue(true),
                        reply -> Assert.assertTrue(((RedisReply.BooleanValue) reply).value())),
                new ReplyCase(
                        new RedisReply.DoubleValue(1.5D),
                        ReplyShapes.doubleValue(1.5D),
                        reply -> Assert.assertEquals(1.5D, ((RedisReply.DoubleValue) reply).value(), 0D)),
                new ReplyCase(
                        new RedisReply.BigNumber("12345678901234567890"),
                        ReplyShapes.bigNumber("12345678901234567890"),
                        reply -> Assert.assertEquals(
                                "12345678901234567890", ((RedisReply.BigNumber) reply).ascii())),
                new ReplyCase(
                        new RedisReply.VerbatimString("txt", verbatimData),
                        ReplyShapes.verbatimString("txt", 4),
                        reply -> {
                            RedisReply.VerbatimString value = (RedisReply.VerbatimString) reply;
                            Assert.assertEquals("txt", value.format());
                            Assert.assertArrayEquals(bytes("body"), value.data());
                        }),
                new ReplyCase(
                        new RedisReply.BlobError("bad"),
                        ReplyShapes.blobError("bad"),
                        reply -> Assert.assertEquals("bad", ((RedisReply.BlobError) reply).message())),
                new ReplyCase(
                        RedisReplies.bulkString(bulkData),
                        ReplyShapes.bulkString(5, 0),
                        reply -> {
                            RedisReply.BulkString value = (RedisReply.BulkString) reply;
                            Assert.assertEquals(5, value.payloadLength());
                            Assert.assertEquals(0L, value.retainedSourceBytes());
                        }),
                new ReplyCase(
                        RedisReplies.nullValue(),
                        ReplyShapes.nullValue(),
                        reply -> Assert.assertTrue(reply instanceof RedisReply.NullValue)),
                new ReplyCase(
                        RedisReplies.nullArray(),
                        ReplyShapes.nullArray(),
                        reply -> Assert.assertTrue(reply instanceof RedisReply.NullArray))
        );

        verbatimData[0] = 'X';
        bulkData[0] = 'X';
        for (ReplyCase replyCase : cases) {
            Assert.assertEquals(replyCase.shape(), replyCase.reply().shape());
            replyCase.dataAssertion().accept(replyCase.reply());
        }
    }

    @Test
    public void aggregateRepliesCarryKindElementsAndDerivedShape() {
        RedisReply simple = RedisReplies.simpleString("OK");
        RedisReply bulk = RedisReplies.bulkString(bytes("value"));
        RedisReply nested = RedisReplies.array(List.of(
                simple,
                RedisReplies.map(List.of(RedisReplies.bulkString(bytes("k")), RedisReplies.integer(7)))
        ));
        List<AggregateCase> cases = List.of(
                new AggregateCase(nested, ReplyShape.AggregateKind.ARRAY, 2),
                new AggregateCase(
                        RedisReplies.map(List.of(bulk, RedisReplies.integer(1))),
                        ReplyShape.AggregateKind.MAP,
                        2),
                new AggregateCase(new RedisReply.Aggregate(ReplyShape.AggregateKind.SET,
                        List.of(simple, bulk)), ReplyShape.AggregateKind.SET, 2),
                new AggregateCase(new RedisReply.Aggregate(ReplyShape.AggregateKind.PUSH,
                        List.of(simple)), ReplyShape.AggregateKind.PUSH, 1),
                new AggregateCase(
                        new RedisReply.Aggregate(ReplyShape.AggregateKind.ATTRIBUTE,
                                List.of(simple, RedisReplies.integer(1))),
                        ReplyShape.AggregateKind.ATTRIBUTE,
                        2)
        );

        Assert.assertEquals(ReplyShapes.simpleString("OK"), simple.shape());
        Assert.assertEquals(ReplyShapes.bulkString(5, 0), bulk.shape());
        for (AggregateCase aggregateCase : cases) {
            RedisReply.Aggregate reply = (RedisReply.Aggregate) aggregateCase.reply();
            ReplyShape.Aggregate shape = (ReplyShape.Aggregate) reply.shape();
            Assert.assertEquals(aggregateCase.kind(), reply.kind());
            Assert.assertEquals(aggregateCase.kind(), shape.kind());
            Assert.assertEquals(aggregateCase.elementCount(), reply.elements().size());
            Assert.assertEquals(aggregateCase.elementCount(), shape.elements().size());
        }
    }

    @Test
    public void aggregateReplyCopiesItsElementList() {
        List<RedisReply> elements = new ArrayList<>();
        elements.add(RedisReplies.integer(1));

        RedisReply.Aggregate reply = (RedisReply.Aggregate) RedisReplies.array(elements);
        elements.clear();

        Assert.assertEquals(1, reply.elements().size());
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> reply.elements().add(RedisReplies.integer(2)));
    }

    @Test
    public void streamingReplyDoesNotMaterializeItsPayload() {
        AtomicInteger emitted = new AtomicInteger();
        RedisReply reply = RedisReplies.sequence(
                2,
                19L,
                lengths -> {
                    lengths.accept(1);
                    lengths.accept(-1);
                },
                sink -> emitted.incrementAndGet()
        );

        Assert.assertEquals(0, emitted.get());
        Assert.assertEquals(19L, reply.shape().retainedSourceBytes());
        Assert.assertEquals(0, emitted.get());
    }

    @Test
    public void streamingRepliesCarryCountsLengthsRetainedBytesAndEmitters() {
        Consumer<IntConsumer> sequenceLengths = consumer -> {
            consumer.accept(1);
            consumer.accept(-1);
        };
        Consumer<IntConsumer> mapLengths = consumer -> {
            consumer.accept(1);
            consumer.accept(3);
        };
        Consumer<IntConsumer> setLengths = consumer -> {
            consumer.accept(1);
            consumer.accept(-1);
        };
        AtomicInteger sequenceEmissions = new AtomicInteger();
        AtomicInteger mapEmissions = new AtomicInteger();
        AtomicInteger setEmissions = new AtomicInteger();
        RedisReply.ByteSequence sequence = (RedisReply.ByteSequence) RedisReplies.sequence(
                2, 19L, sequenceLengths, sink -> sequenceEmissions.incrementAndGet());
        RedisReply.ByteMap map = (RedisReply.ByteMap) RedisReplies.byteMap(
                1, 23L, mapLengths, sink -> mapEmissions.incrementAndGet());
        RedisReply.ByteSet set = (RedisReply.ByteSet) RedisReplies.byteSet(
                2, 29L, setLengths, sink -> setEmissions.incrementAndGet());

        Assert.assertEquals(2, sequence.elementCount());
        Assert.assertSame(sequenceLengths, sequence.payloadLengths());
        Assert.assertEquals(19L, sequence.retainedSourceBytes());
        Assert.assertEquals(new ReplyShape.ByteSequence(2, sequenceLengths, 19L), sequence.shape());
        Assert.assertEquals(1, map.pairCount());
        Assert.assertSame(mapLengths, map.payloadLengths());
        Assert.assertEquals(23L, map.retainedSourceBytes());
        Assert.assertEquals(new ReplyShape.ByteMap(1, mapLengths, 23L), map.shape());
        Assert.assertEquals(2, set.elementCount());
        Assert.assertSame(setLengths, set.payloadLengths());
        Assert.assertEquals(29L, set.retainedSourceBytes());
        Assert.assertEquals(new ReplyShape.ByteSet(2, setLengths, 29L), set.shape());
        Assert.assertEquals(0, sequenceEmissions.get());
        Assert.assertEquals(0, mapEmissions.get());
        Assert.assertEquals(0, setEmissions.get());
    }

    @Test
    public void publicConstructorsAndFactoriesRejectInvalidReplyMetadata() {
        Consumer<ReplySink> emitter = sink -> { };
        Consumer<IntConsumer> lengths = consumer -> { };

        assertIllegalArgument(() -> new RedisReply.BulkString(-1, 0, emitter));
        assertIllegalArgument(() -> RedisReplies.bulkString(-1, 0, emitter));
        assertIllegalArgument(() -> new RedisReply.BulkString(0, -1, emitter));
        assertIllegalArgument(() -> RedisReplies.bulkString(0, -1, emitter));
        assertIllegalArgument(() -> new RedisReply.ByteSequence(-1, 0, lengths, emitter));
        assertIllegalArgument(() -> RedisReplies.sequence(-1, 0, lengths, emitter));
        assertIllegalArgument(() -> new RedisReply.ByteSequence(0, -1, lengths, emitter));
        assertIllegalArgument(() -> RedisReplies.sequence(0, -1, lengths, emitter));
        assertIllegalArgument(() -> new RedisReply.ByteSet(-1, 0, lengths, emitter));
        assertIllegalArgument(() -> RedisReplies.byteSet(-1, 0, lengths, emitter));
        assertIllegalArgument(() -> new RedisReply.ByteSet(0, -1, lengths, emitter));
        assertIllegalArgument(() -> RedisReplies.byteSet(0, -1, lengths, emitter));
        assertIllegalArgument(() -> new RedisReply.ByteMap(-1, 0, lengths, emitter));
        assertIllegalArgument(() -> RedisReplies.byteMap(-1, 0, lengths, emitter));
        assertIllegalArgument(() -> new RedisReply.ByteMap(0, -1, lengths, emitter));
        assertIllegalArgument(() -> RedisReplies.byteMap(0, -1, lengths, emitter));
    }

    @Test
    public void publicConstructorsAndFactoriesRejectInvalidReplyReferences() {
        Consumer<ReplySink> emitter = sink -> { };
        Consumer<IntConsumer> lengths = consumer -> { };

        assertNullPointer(() -> new RedisReply.BulkString(0, 0, null));
        assertNullPointer(() -> RedisReplies.bulkString(0, 0, null));
        assertNullPointer(() -> new RedisReply.ByteSequence(0, 0, null, emitter));
        assertNullPointer(() -> new RedisReply.ByteSequence(0, 0, lengths, null));
        assertNullPointer(() -> RedisReplies.sequence(0, 0, null, emitter));
        assertNullPointer(() -> RedisReplies.sequence(0, 0, lengths, null));
        assertNullPointer(() -> new RedisReply.ByteSet(0, 0, null, emitter));
        assertNullPointer(() -> new RedisReply.ByteSet(0, 0, lengths, null));
        assertNullPointer(() -> RedisReplies.byteSet(0, 0, null, emitter));
        assertNullPointer(() -> RedisReplies.byteSet(0, 0, lengths, null));
        assertNullPointer(() -> new RedisReply.ByteMap(0, 0, null, emitter));
        assertNullPointer(() -> new RedisReply.ByteMap(0, 0, lengths, null));
        assertNullPointer(() -> RedisReplies.byteMap(0, 0, null, emitter));
        assertNullPointer(() -> RedisReplies.byteMap(0, 0, lengths, null));
        assertNullPointer(() -> RedisReplies.bulkString((byte[]) null));
        assertNullPointer(() -> new RedisReply.VerbatimString("txt", null));
    }

    @Test
    public void mapLikeAggregatesRejectOddAndNullElements() {
        List<RedisReply> odd = List.of(RedisReplies.integer(1));
        List<RedisReply> withNull = new ArrayList<>();
        withNull.add(null);

        assertIllegalArgument(() -> RedisReplies.map(odd));
        assertIllegalArgument(() -> new RedisReply.Aggregate(ReplyShape.AggregateKind.ATTRIBUTE, odd));
        assertIllegalArgument(() -> new RedisReply.Aggregate(ReplyShape.AggregateKind.MAP, odd));
        assertIllegalArgument(() -> new RedisReply.Aggregate(ReplyShape.AggregateKind.ATTRIBUTE, odd));
        assertNullPointer(() -> RedisReplies.array(withNull));
        assertNullPointer(() -> new RedisReply.Aggregate(ReplyShape.AggregateKind.ARRAY, withNull));
    }

    @Test
    public void aggregatesRejectControlErrors() {
        RedisReply controlError = RedisReplies.controlError("OOM rejected");

        assertIllegalArgument(() -> RedisReplies.array(List.of(controlError)));
        assertIllegalArgument(() -> RedisReplies.map(List.of(RedisReplies.integer(1), controlError)));
        assertIllegalArgument(() -> new RedisReply.Aggregate(
                ReplyShape.AggregateKind.SET, List.of(controlError)));
        assertIllegalArgument(() -> new RedisReply.Aggregate(
                ReplyShape.AggregateKind.PUSH, List.of(controlError)));
        assertIllegalArgument(() -> new RedisReply.Aggregate(
                ReplyShape.AggregateKind.ATTRIBUTE, List.of(RedisReplies.integer(1), controlError)));
        assertIllegalArgument(() -> new RedisReply.Aggregate(
                ReplyShape.AggregateKind.ARRAY,
                List.of(controlError)));
    }

    @Test
    public void commandResultFactoriesSetOnlyTheRequestedCloseFlag() {
        RedisReply ok = RedisReplies.simpleString("OK");
        CommandResult ordinary = CommandResult.reply(ok);
        CommandResult error = CommandResult.error("bad");
        CommandResult control = CommandResult.controlError("failed");
        CommandResult closing = CommandResult.closeAfterReply(ok);

        Assert.assertSame(ok, ordinary.reply());
        Assert.assertFalse(ordinary.closeAfterReply());
        Assert.assertTrue(error.reply() instanceof RedisReply.Error);
        Assert.assertFalse(error.closeAfterReply());
        Assert.assertTrue(control.reply() instanceof RedisReply.ControlError);
        Assert.assertFalse(control.closeAfterReply());
        Assert.assertSame(ok, closing.reply());
        Assert.assertTrue(closing.closeAfterReply());
        assertNullPointer(() -> new CommandResult(null, false));
        assertNullPointer(() -> CommandResult.reply(null));
        assertNullPointer(() -> CommandResult.closeAfterReply(null));
    }

    private static void assertIllegalArgument(Runnable action) {
        Assert.assertThrows(IllegalArgumentException.class, action::run);
    }

    private static void assertNullPointer(Runnable action) {
        Assert.assertThrows(NullPointerException.class, action::run);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private record ReplyCase(RedisReply reply, ReplyShape shape, Consumer<RedisReply> dataAssertion) {
    }

    private record AggregateCase(RedisReply reply, ReplyShape.AggregateKind kind, int elementCount) {
    }
}
