package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.v1.CustomProtocolV1ArgvRequest;

import java.nio.charset.StandardCharsets;

public class CustomRequestDecoderTest {
    @Test
    public void decodesSingleFrame() {
        EmbeddedChannel ch = new EmbeddedChannel(new CustomRequestDecoder(1024, 16, 64));
        try {
            byte[] frame = frame("{\"cmd\":\"PING\",\"args\":[]}");
            Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(frame)));

            CustomProtocolV1ArgvRequest request = ch.readInbound();
            Assert.assertNotNull(request);
            Assert.assertEquals(1, request.argc());
            Assert.assertArrayEquals(utf8("PING"), request.readOnlyArg(0));
            Assert.assertEquals(4, request.retainedBytes());

            Assert.assertNull(ch.readOutbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesSingleFrameFromDirectBuffer() {
        EmbeddedChannel ch = new EmbeddedChannel(new CustomRequestDecoder(1024, 16, 64));
        try {
            byte[] frame = frame("{\"cmd\":\"PING\",\"args\":[]}");
            ByteBuf direct = Unpooled.directBuffer(frame.length);
            direct.writeBytes(frame);
            Assert.assertTrue(ch.writeInbound(direct));

            CustomProtocolV1ArgvRequest request = ch.readInbound();
            Assert.assertNotNull(request);
            Assert.assertEquals(1, request.argc());
            Assert.assertArrayEquals(utf8("PING"), request.readOnlyArg(0));
            Assert.assertEquals(4, request.retainedBytes());

            Assert.assertNull(ch.readOutbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesPipelinedFrames() {
        EmbeddedChannel ch = new EmbeddedChannel(new CustomRequestDecoder(1024, 16, 64));
        try {
            byte[] f1 = frame("{\"cmd\":\"PING\",\"args\":[]}");
            byte[] f2 = frame("{\"cmd\":\"ECHO\",\"args\":[\"hi\"]}");
            byte[] both = new byte[f1.length + f2.length];
            System.arraycopy(f1, 0, both, 0, f1.length);
            System.arraycopy(f2, 0, both, f1.length, f2.length);
            Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(both)));

            CustomProtocolV1ArgvRequest request1 = ch.readInbound();
            CustomProtocolV1ArgvRequest request2 = ch.readInbound();
            Assert.assertNotNull(request1);
            Assert.assertNotNull(request2);
            Assert.assertEquals(1, request1.argc());
            Assert.assertArrayEquals(utf8("PING"), request1.readOnlyArg(0));
            Assert.assertEquals(2, request2.argc());
            Assert.assertArrayEquals(utf8("ECHO"), request2.readOnlyArg(0));
            Assert.assertArrayEquals(utf8("hi"), request2.readOnlyArg(1));
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void rawNewlineInPayloadIsRejected() {
        EmbeddedChannel ch = new EmbeddedChannel(new CustomRequestDecoder(1024, 16, 64));
        try {
            byte[] bad = frame("{\"cmd\":\"PING\",\n\"args\":[]}");
            Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(bad)));

            Object e = ch.readInbound();
            Assert.assertNotNull(e);
            Assert.assertTrue(e instanceof ProtocolError);
            Assert.assertTrue(((ProtocolError) e).message().startsWith("Protocol error"));
            Assert.assertNull(ch.readInbound());
            Assert.assertNull(ch.readOutbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void rawNewlineInPayloadIsRejectedAndNextFrameIsStillDecoded() {
        EmbeddedChannel ch = new EmbeddedChannel(new CustomRequestDecoder(1024, 16, 64));
        try {
            byte[] bad = frame("{\"cmd\":\"PING\",\n\"args\":[]}");
            byte[] good = frame("{\"cmd\":\"PING\",\"args\":[]}");
            byte[] both = new byte[bad.length + good.length];
            System.arraycopy(bad, 0, both, 0, bad.length);
            System.arraycopy(good, 0, both, bad.length, good.length);

            Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(both)));

            Object e = ch.readInbound();
            Assert.assertNotNull(e);
            Assert.assertTrue(e instanceof ProtocolError);
            Assert.assertTrue(((ProtocolError) e).message().startsWith("Protocol error"));

            CustomProtocolV1ArgvRequest request = ch.readInbound();
            Assert.assertNotNull(request);
            Assert.assertEquals(1, request.argc());
            Assert.assertArrayEquals(utf8("PING"), request.readOnlyArg(0));

            Assert.assertNull(ch.readInbound());
            Assert.assertNull(ch.readOutbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void invalidJsonReturnsErrorAndContinues() {
        EmbeddedChannel ch = new EmbeddedChannel(new CustomRequestDecoder(1024, 16, 64));
        try {
            byte[] bad = frame("{\"cmd\":}");
            byte[] good = frame("{\"cmd\":\"PING\",\"args\":[]}");
            byte[] both = new byte[bad.length + good.length];
            System.arraycopy(bad, 0, both, 0, bad.length);
            System.arraycopy(good, 0, both, bad.length, good.length);

            Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(both)));

            Object e = ch.readInbound();
            Assert.assertNotNull(e);
            Assert.assertTrue(e instanceof ProtocolError);
            Assert.assertTrue(((ProtocolError) e).message().startsWith("Protocol error"));

            CustomProtocolV1ArgvRequest request = ch.readInbound();
            Assert.assertNotNull(request);
            Assert.assertEquals(1, request.argc());
            Assert.assertArrayEquals(utf8("PING"), request.readOnlyArg(0));
            Assert.assertNull(ch.readOutbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void invalidHeaderResyncsAtNewline() {
        EmbeddedChannel ch = new EmbeddedChannel(new CustomRequestDecoder(1024, 16, 64));
        try {
            byte[] junk = "abc\n".getBytes(StandardCharsets.US_ASCII);
            byte[] good = frame("{\"cmd\":\"PING\",\"args\":[]}");
            byte[] both = new byte[junk.length + good.length];
            System.arraycopy(junk, 0, both, 0, junk.length);
            System.arraycopy(good, 0, both, junk.length, good.length);

            Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(both)));

            Object e = ch.readInbound();
            Assert.assertNotNull(e);
            Assert.assertTrue(e instanceof ProtocolError);
            Assert.assertTrue(((ProtocolError) e).message().startsWith("Protocol error"));

            CustomProtocolV1ArgvRequest request = ch.readInbound();
            Assert.assertNotNull(request);
            Assert.assertEquals(1, request.argc());
            Assert.assertArrayEquals(utf8("PING"), request.readOnlyArg(0));
            Assert.assertNull(ch.readOutbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesArgsBeyondJsonDefaultWhenMaxArgsAllows() {
        int args = 1500;
        EmbeddedChannel ch = new EmbeddedChannel(new CustomRequestDecoder(1024 * 1024, 2000, 64));
        try {
            StringBuilder sb = new StringBuilder(16 * args);
            sb.append("{\"cmd\":\"ECHO\",\"args\":[");
            for (int i = 0; i < args; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append("\"a").append(i).append('"');
            }
            sb.append("]}");
            byte[] frame = frame(sb.toString());

            Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(frame)));

            Object inbound = ch.readInbound();
            Assert.assertNotNull(inbound);
            Assert.assertTrue(inbound instanceof CustomProtocolV1ArgvRequest);
            CustomProtocolV1ArgvRequest request = (CustomProtocolV1ArgvRequest) inbound;
            Assert.assertEquals(args + 1, request.argc());
            Assert.assertArrayEquals(utf8("ECHO"), request.readOnlyArg(0));
            Assert.assertArrayEquals(utf8("a0"), request.readOnlyArg(1));
            Assert.assertArrayEquals(utf8("a" + (args - 1)), request.readOnlyArg(args));
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] frame(String json) {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        String header = payload.length + ":";
        byte[] head = header.getBytes(StandardCharsets.US_ASCII);
        byte[] out = new byte[head.length + payload.length + 1];
        System.arraycopy(head, 0, out, 0, head.length);
        System.arraycopy(payload, 0, out, head.length, payload.length);
        out[out.length - 1] = '\n';
        return out;
    }
}
