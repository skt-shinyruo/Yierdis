package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

public class RespRequestDecoderTest {
    @Test
    public void exposesOnlyTheSupportedFourArgumentConstructor() {
        java.lang.reflect.Constructor<?>[] constructors = RespRequestDecoder.class.getConstructors();
        Assert.assertEquals("decoder should expose one supported public constructor", 1, constructors.length);
        Assert.assertArrayEquals(
                new Class<?>[]{int.class, int.class, int.class, int.class},
                constructors[0].getParameterTypes()
        );
    }

    @Test
    public void decodesArrayCommand() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n",
                    StandardCharsets.US_ASCII
            )));

            try (ExecutionRequest req = readExecutionRequest(ch)) {
                Assert.assertEquals(2, req.argc());
                Assert.assertArrayEquals(bytes("PING"), req.readOnlyByteArray(0));
                Assert.assertArrayEquals(bytes("hey"), req.readOnlyByteArray(1));
                Assert.assertEquals(7, req.retainedBytes());
            }
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesArrayCommandWithNullBulkString() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "*2\r\n$4\r\nECHO\r\n$-1\r\n",
                    StandardCharsets.US_ASCII
            )));

            try (ExecutionRequest req = readExecutionRequest(ch)) {
                Assert.assertEquals(2, req.argc());
                Assert.assertArrayEquals(bytes("ECHO"), req.readOnlyByteArray(0));
                Assert.assertNull(req.readOnlyByteArray(1));
                Assert.assertEquals(4, req.retainedBytes());
            }
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesFragmentedArrayCommandAcrossMultipleReads() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        try {
            Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("*2\r\n$4\r\nPI", StandardCharsets.US_ASCII)));
            Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("NG\r\n$3\r\nhe", StandardCharsets.US_ASCII)));
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("y\r\n", StandardCharsets.US_ASCII)));

            try (ExecutionRequest req = readExecutionRequest(ch)) {
                Assert.assertEquals(2, req.argc());
                Assert.assertArrayEquals(bytes("PING"), req.readOnlyByteArray(0));
                Assert.assertArrayEquals(bytes("hey"), req.readOnlyByteArray(1));
            }
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesEverySingleByteFragmentIncludingAnEmptyBulkString() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        byte[] payload = bytes("*2\r\n$4\r\nECHO\r\n$0\r\n\r\n");
        try {
            for (int index = 0; index < payload.length; index++) {
                boolean produced = ch.writeInbound(Unpooled.wrappedBuffer(new byte[]{payload[index]}));
                Assert.assertEquals(index == payload.length - 1, produced);
            }

            try (ExecutionRequest req = readExecutionRequest(ch)) {
                Assert.assertEquals(2, req.argc());
                Assert.assertArrayEquals(bytes("ECHO"), req.readOnlyByteArray(0));
                Assert.assertArrayEquals(new byte[0], req.readOnlyByteArray(1));
                Assert.assertEquals(4, req.retainedBytes());
            }
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void fragmentedOversizedCommandFailsOnceAndDropsRemainingInput() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 4));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("*2\r\n$3\r\nGET\r\n$2\r\n", StandardCharsets.US_ASCII)));

            Object msg = ch.readInbound();
            Assert.assertTrue(msg instanceof RespProtocolError);
            Assert.assertEquals("ERR Protocol error: command is too large", ((RespProtocolError) msg).message());
            Assert.assertNull(ch.readInbound());
            Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("ab\r\n*1\r\n$4\r\nPING\r\n", StandardCharsets.US_ASCII)));
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesPipelinedCommandsInOrder() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nECHO\r\n",
                    StandardCharsets.US_ASCII
            )));

            try (ExecutionRequest ping = readExecutionRequest(ch);
                 ExecutionRequest echo = readExecutionRequest(ch)) {
                Assert.assertArrayEquals(bytes("PING"), ping.readOnlyByteArray(0));
                Assert.assertArrayEquals(bytes("ECHO"), echo.readOnlyByteArray(0));
            }
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesInlineCommand() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("SET a 1\r\n", StandardCharsets.US_ASCII)));

            try (ExecutionRequest req = readExecutionRequest(ch)) {
                Assert.assertEquals(3, req.argc());
                Assert.assertArrayEquals(bytes("SET"), req.readOnlyByteArray(0));
                Assert.assertArrayEquals(bytes("a"), req.readOnlyByteArray(1));
                Assert.assertArrayEquals(bytes("1"), req.readOnlyByteArray(2));
                Assert.assertEquals(5, req.retainedBytes());
            }
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesInlineHexEscapesLikeCli() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("SET \"a\\x20b\" \"\\x41\"\r\n", StandardCharsets.US_ASCII)));

            try (ExecutionRequest req = readExecutionRequest(ch)) {
                Assert.assertEquals(3, req.argc());
                Assert.assertArrayEquals(bytes("SET"), req.readOnlyByteArray(0));
                Assert.assertArrayEquals(bytes("a b"), req.readOnlyByteArray(1));
                Assert.assertArrayEquals(bytes("A"), req.readOnlyByteArray(2));
                Assert.assertEquals(7, req.retainedBytes());
            }
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void ignoresBlankInlineLinesAndDecodesSingleQuotedEscapes() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "\r\n \t\r\nECHO 'a\\'b'\r\n",
                    StandardCharsets.US_ASCII
            )));

            try (ExecutionRequest req = readExecutionRequest(ch)) {
                Assert.assertEquals(2, req.argc());
                Assert.assertArrayEquals(bytes("ECHO"), req.readOnlyByteArray(0));
                Assert.assertArrayEquals(bytes("a'b"), req.readOnlyByteArray(1));
            }
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void rejectsConfiguredArrayInlineArgumentAndLineLimits() {
        assertProtocolError(
                new RespRequestDecoder(1024, 1, 1024, 1024),
                "*2\r\n$4\r\nECHO\r\n$1\r\nx\r\n",
                "ERR Protocol error: too many arguments"
        );
        assertProtocolError(
                new RespRequestDecoder(1024, 2, 1024, 1024),
                "SET a b\r\n",
                "ERR Protocol error: too many arguments"
        );
        assertProtocolError(
                new RespRequestDecoder(1024, 16, 4, 1024),
                "abcde",
                "ERR Protocol error: invalid inline command"
        );
        assertProtocolError(
                new RespRequestDecoder(1024, 16, 4, 1024),
                "PING\r\n",
                "ERR Protocol error: invalid inline command"
        );
    }

    @Test
    public void rejectsMalformedArrayAndBulkFrames() {
        assertProtocolError(
                new RespRequestDecoder(1024, 16, 1024, 1024),
                "*-1\r\n",
                "ERR Protocol error: invalid multibulk length"
        );
        assertProtocolError(
                new RespRequestDecoder(1024, 16, 1024, 1024),
                "*x\r\n",
                "ERR Protocol error: invalid multibulk length"
        );
        assertProtocolError(
                new RespRequestDecoder(1024, 16, 1024, 1024),
                "*1\n",
                "ERR Protocol error: invalid multibulk length"
        );
        assertProtocolError(
                new RespRequestDecoder(1024, 16, 1024, 1024),
                "*1\r\n+PING\r\n",
                "ERR Protocol error: expected '$', got other"
        );
        assertProtocolError(
                new RespRequestDecoder(1024, 16, 1024, 1024),
                "*1\r\n$x\r\n",
                "ERR Protocol error: invalid bulk length"
        );
        assertProtocolError(
                new RespRequestDecoder(1024, 16, 1024, 1024),
                "*1\r\n$1\r\naXX",
                "ERR Protocol error: invalid bulk string terminator"
        );
    }

    @Test
    public void passesThroughOtherMessagesAndReleasesInputAfterProtocolError() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        Object marker = new Object();
        try {
            Assert.assertTrue(ch.writeInbound(marker));
            Assert.assertSame(marker, ch.readInbound());

            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("*x\r\n", StandardCharsets.US_ASCII)));
            Assert.assertTrue(ch.readInbound() instanceof RespProtocolError);

            io.netty.buffer.ByteBuf ignored = Unpooled.buffer().writeByte('x');
            Assert.assertEquals(1, ignored.refCnt());
            Assert.assertFalse(ch.writeInbound(ignored));
            Assert.assertEquals(0, ignored.refCnt());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void emitsProtocolErrorForOversizedBulk() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(2, 16, 1024, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("*1\r\n$3\r\nabc\r\n", StandardCharsets.US_ASCII)));

            Object msg = ch.readInbound();
            Assert.assertTrue(msg instanceof RespProtocolError);
            Assert.assertTrue(((RespProtocolError) msg).closeAfterReply());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void rejectsBulkLengthAboveHardLimitBeforeAllocatingBody() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(Integer.MAX_VALUE, 16, 1024, 1024));
        try {
            Object msg = writeInboundAndReadFirst(ch, "*1\r\n$" + Integer.MAX_VALUE + "\r\n");

            Assert.assertTrue(msg instanceof RespProtocolError);
            Assert.assertTrue(((RespProtocolError) msg).closeAfterReply());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void rejectsBulkLengthBelowNegativeOne() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        try {
            Object msg = writeInboundAndReadFirst(ch, "*2\r\n$4\r\nECHO\r\n$-2\r\n");

            Assert.assertTrue(msg instanceof RespProtocolError);
            Assert.assertTrue(((RespProtocolError) msg).closeAfterReply());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void rejectsArrayLengthAboveHardLimitBeforeAllocatingArgv() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, Integer.MAX_VALUE, 1024, 1024));
        try {
            Object msg = writeInboundAndReadFirst(ch, "*" + Integer.MAX_VALUE + "\r\n");

            Assert.assertTrue(msg instanceof RespProtocolError);
            Assert.assertTrue(((RespProtocolError) msg).closeAfterReply());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void protocolErrorDropsPipelinedCommandsInSameRead() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(4, 16, 1024, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "*1\r\n$5\r\nabcde\r\n*1\r\n$4\r\nPING\r\n",
                    StandardCharsets.US_ASCII
            )));

            Assert.assertTrue(ch.readInbound() instanceof RespProtocolError);
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decoderEmitsExecutionRequestDirectly() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        ExecutionRequest request = null;
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("*1\r\n$4\r\nPING\r\n", StandardCharsets.US_ASCII)));

            Object msg = ch.readInbound();
            Assert.assertTrue(msg instanceof ExecutionRequest);
            request = (ExecutionRequest) msg;
            Assert.assertEquals(1, request.argc());
            Assert.assertArrayEquals(bytes("PING"), request.readOnlyByteArray(0));
        } finally {
            if (request != null) {
                request.close();
            }
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decoderPreservesNullBulkStringThroughExecutionRequest() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        ExecutionRequest request = null;
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "*2\r\n$4\r\nECHO\r\n$-1\r\n",
                    StandardCharsets.US_ASCII
            )));

            Object msg = ch.readInbound();
            Assert.assertTrue(msg instanceof ExecutionRequest);
            request = (ExecutionRequest) msg;
            Assert.assertEquals(2, request.argc());
            Assert.assertArrayEquals(bytes("ECHO"), request.readOnlyByteArray(0));
            Assert.assertTrue(request.isNull(1));
            Assert.assertNull(request.readOnlyByteArray(1));
            Assert.assertEquals(4, request.retainedBytes());
        } finally {
            if (request != null) {
                request.close();
            }
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodedRetainedRequestReleasesDetachedLeaseAfterChannelCloseFromAnotherThread() throws Exception {
        InboundMemoryBudget budget = new InboundMemoryBudget(4_096);
        InboundConnectionMemory connection = new InboundConnectionMemory("detached", 4_096, Runnable::run, () -> { });
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024,
                16,
                1_024,
                1_024,
                budget,
                connection,
                RespDecodedMessageGate.PASS_THROUGH
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        ExecutionRequest retained = null;
        try {
            Assert.assertTrue(channel.writeInbound(Unpooled.copiedBuffer(
                    "*1\r\n$4\r\nPING\r\n",
                    StandardCharsets.US_ASCII
            )));

            Object decoded = channel.readInbound();
            Assert.assertTrue(decoded instanceof RetainedRespExecutionRequest);
            ExecutionRequest request = (ExecutionRequest) decoded;
            retained = request.retain();
            request.close();

            Assert.assertTrue("the retained request must own the post-decode reservation",
                    budget.stats().reservedBytes() > 0L);
            channel.finishAndReleaseAll();
            connection.close();

            AtomicReference<Throwable> failure = new AtomicReference<>();
            ExecutionRequest finalRetained = retained;
            Thread finalizer = Thread.ofPlatform().start(() -> {
                try {
                    finalRetained.close();
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            finalizer.join(1_000L);

            Assert.assertFalse("detached close must not block on the closed channel", finalizer.isAlive());
            Assert.assertNull(failure.get());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
            Assert.assertEquals(0L, connection.reservedBytes());
            Assert.assertEquals(0L, budget.stats().retainedInputCapacityBytes());
            retained = null;
        } finally {
            if (retained != null) {
                retained.close();
            }
            channel.finishAndReleaseAll();
            connection.close();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static ExecutionRequest readExecutionRequest(EmbeddedChannel channel) {
        Object message = channel.readInbound();
        Assert.assertTrue(message instanceof ExecutionRequest);
        return (ExecutionRequest) message;
    }

    private static Object writeInboundAndReadFirst(EmbeddedChannel ch, String payload) {
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(payload, StandardCharsets.US_ASCII)));
        } catch (OutOfMemoryError e) {
            Assert.fail("decoder attempted to allocate from an invalid RESP length: " + e.getMessage());
        }
        return ch.readInbound();
    }

    private static void assertProtocolError(
            RespRequestDecoder decoder,
            String payload,
            String expectedMessage
    ) {
        EmbeddedChannel ch = new EmbeddedChannel(decoder);
        try {
            Object message = writeInboundAndReadFirst(ch, payload);
            Assert.assertTrue(message instanceof RespProtocolError);
            RespProtocolError error = (RespProtocolError) message;
            Assert.assertEquals(expectedMessage, error.message());
            Assert.assertTrue(error.closeAfterReply());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void emitsProtocolErrorWhenTotalCommandBytesExceedLimit() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 4));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "*2\r\n$3\r\nGET\r\n$2\r\nab\r\n",
                    StandardCharsets.US_ASCII
            )));

            Object msg = ch.readInbound();
            Assert.assertTrue(msg instanceof RespProtocolError);
            Assert.assertEquals("ERR Protocol error: command is too large", ((RespProtocolError) msg).message());
            Assert.assertTrue(((RespProtocolError) msg).closeAfterReply());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }
}
