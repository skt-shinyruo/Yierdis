package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.protocol.v1.CustomCommand;

import java.nio.charset.StandardCharsets;

public class CustomRequestDecoderTest {
    @Test
    public void decodesSingleFrame() {
        EmbeddedChannel ch = new EmbeddedChannel(new CustomRequestDecoder(1024, 16, 64));
        try {
            byte[] frame = frame("{\"cmd\":\"PING\",\"args\":[]}");
            Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(frame)));

            Command cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertTrue(cmd instanceof CustomCommand);
            Assert.assertEquals(1, cmd.argc());
            Assert.assertArrayEquals("PING".getBytes(StandardCharsets.UTF_8), cmd.toByteArray(0));
            cmd.close();

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

            Command cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertTrue(cmd instanceof CustomCommand);
            Assert.assertEquals(1, cmd.argc());
            Assert.assertArrayEquals("PING".getBytes(StandardCharsets.UTF_8), cmd.toByteArray(0));
            cmd.close();

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

            Command c1 = ch.readInbound();
            Command c2 = ch.readInbound();
            Assert.assertNotNull(c1);
            Assert.assertNotNull(c2);
            Assert.assertEquals("PING", new String(c1.toByteArray(0), StandardCharsets.UTF_8));
            Assert.assertEquals("ECHO", new String(c2.toByteArray(0), StandardCharsets.UTF_8));
            Assert.assertEquals(2, c2.argc());
            Assert.assertEquals("hi", new String(c2.toByteArray(1), StandardCharsets.UTF_8));
            c1.close();
            c2.close();
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

            Command cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertEquals("PING", new String(cmd.toByteArray(0), StandardCharsets.UTF_8));
            cmd.close();
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

            Command cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertEquals("PING", new String(cmd.toByteArray(0), StandardCharsets.UTF_8));
            cmd.close();
            Assert.assertNull(ch.readOutbound());
        } finally {
            ch.finishAndReleaseAll();
        }
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
