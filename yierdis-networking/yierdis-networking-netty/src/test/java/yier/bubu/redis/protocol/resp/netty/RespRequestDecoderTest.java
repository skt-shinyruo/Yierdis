package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.protocol.resp.RespCommandRequest;

import java.nio.charset.StandardCharsets;

public class RespRequestDecoderTest {
    @Test
    public void decodesArrayCommand() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n",
                    StandardCharsets.US_ASCII
            )));

            RespCommandRequest req = ch.readInbound();
            Assert.assertEquals(2, req.argc());
            Assert.assertArrayEquals(bytes("PING"), req.readOnlyArg(0));
            Assert.assertArrayEquals(bytes("hey"), req.readOnlyArg(1));
            Assert.assertEquals(7, req.retainedBytes());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesArrayCommandWithNullBulkString() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "*2\r\n$4\r\nECHO\r\n$-1\r\n",
                    StandardCharsets.US_ASCII
            )));

            RespCommandRequest req = ch.readInbound();
            Assert.assertEquals(2, req.argc());
            Assert.assertArrayEquals(bytes("ECHO"), req.readOnlyArg(0));
            Assert.assertNull(req.readOnlyArg(1));
            Assert.assertEquals(4, req.retainedBytes());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesPipelinedCommandsInOrder() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nECHO\r\n",
                    StandardCharsets.US_ASCII
            )));

            Assert.assertArrayEquals(bytes("PING"), ((RespCommandRequest) ch.readInbound()).readOnlyArg(0));
            Assert.assertArrayEquals(bytes("ECHO"), ((RespCommandRequest) ch.readInbound()).readOnlyArg(0));
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesInlineCommand() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("SET a 1\r\n", StandardCharsets.US_ASCII)));

            RespCommandRequest req = ch.readInbound();
            Assert.assertEquals(3, req.argc());
            Assert.assertArrayEquals(bytes("SET"), req.readOnlyArg(0));
            Assert.assertArrayEquals(bytes("a"), req.readOnlyArg(1));
            Assert.assertArrayEquals(bytes("1"), req.readOnlyArg(2));
            Assert.assertEquals(5, req.retainedBytes());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesInlineHexEscapesLikeCli() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("SET \"a\\x20b\" \"\\x41\"\r\n", StandardCharsets.US_ASCII)));

            RespCommandRequest req = ch.readInbound();
            Assert.assertEquals(3, req.argc());
            Assert.assertArrayEquals(bytes("SET"), req.readOnlyArg(0));
            Assert.assertArrayEquals(bytes("a b"), req.readOnlyArg(1));
            Assert.assertArrayEquals(bytes("A"), req.readOnlyArg(2));
            Assert.assertEquals(7, req.retainedBytes());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void emitsProtocolErrorForOversizedBulk() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(2, 16, 1024));
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
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(Integer.MAX_VALUE, 16, 1024));
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
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
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
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, Integer.MAX_VALUE, 1024));
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
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(4, 16, 1024));
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
    public void adapterConvertsRespCommandRequestToExecutionRequest() {
        EmbeddedChannel ch = new EmbeddedChannel(
                new RespRequestDecoder(1024, 16, 1024),
                new RespCommandAdapter()
        );
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
    public void adapterPreservesNullBulkStringThroughExecutionRequest() {
        EmbeddedChannel ch = new EmbeddedChannel(
                new RespRequestDecoder(1024, 16, 1024),
                new RespCommandAdapter()
        );
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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static Object writeInboundAndReadFirst(EmbeddedChannel ch, String payload) {
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(payload, StandardCharsets.US_ASCII)));
        } catch (OutOfMemoryError e) {
            Assert.fail("decoder attempted to allocate from an invalid RESP length: " + e.getMessage());
        }
        return ch.readInbound();
    }
}
