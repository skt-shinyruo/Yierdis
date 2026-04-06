package yier.bubu.redis;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.protocol.netty.CustomRequestDecoder;
import yier.bubu.redis.protocol.v1.CustomProtocolV1Request;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ProtocolCommandAdapterTest {
    @Test
    public void adaptsCustomProtocolRequestToExecutionRequestPreservingUtf8NullsAndRetainedBytes() {
        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter());
        ExecutionRequest adapted = null;
        try {
            Assert.assertTrue(ch.writeInbound(new CustomProtocolV1Request("PING", Arrays.asList("alpha", null, "你好", ""))));

            Object inbound = ch.readInbound();
            Assert.assertTrue(inbound instanceof ExecutionRequest);
            Assert.assertFalse(inbound instanceof Command);

            adapted = (ExecutionRequest) inbound;
            Assert.assertEquals(5, adapted.argc());
            Assert.assertArrayEquals(utf8("PING"), adapted.toByteArray(0));
            Assert.assertArrayEquals(utf8("alpha"), adapted.toByteArray(1));
            Assert.assertTrue(adapted.isNull(2));
            Assert.assertEquals(-1, adapted.len(2));
            Assert.assertArrayEquals(utf8("你好"), adapted.toByteArray(3));
            Assert.assertArrayEquals(utf8(""), adapted.toByteArray(4));

            int expectedRetainedBytes = utf8("PING").length
                    + utf8("alpha").length
                    + utf8("你好").length;
            Assert.assertEquals(expectedRetainedBytes, adapted.retainedBytes());
        } finally {
            if (adapted != null) {
                adapted.close();
            }
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decoderNormalizesWhitespaceBeforeAdapterConvertsToExecutionRequest() {
        EmbeddedChannel ch = new EmbeddedChannel(new CustomRequestDecoder(1024, 16, 64), new ProtocolCommandAdapter());
        ExecutionRequest adapted = null;
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(frame("{\"cmd\":\" \\tPING\\r\\n \",\"args\":[\"alpha\"]}"))));

            Object inbound = ch.readInbound();
            Assert.assertTrue(inbound instanceof ExecutionRequest);
            Assert.assertFalse(inbound instanceof Command);

            adapted = (ExecutionRequest) inbound;
            Assert.assertArrayEquals(utf8("PING"), adapted.toByteArray(0));
            Assert.assertArrayEquals(utf8("alpha"), adapted.toByteArray(1));
        } finally {
            if (adapted != null) {
                adapted.close();
            }
            ch.finishAndReleaseAll();
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] frame(String json) {
        byte[] payload = utf8(json);
        byte[] head = (payload.length + ":").getBytes(StandardCharsets.US_ASCII);
        byte[] frame = new byte[head.length + payload.length + 1];
        System.arraycopy(head, 0, frame, 0, head.length);
        System.arraycopy(payload, 0, frame, head.length, payload.length);
        frame[frame.length - 1] = '\n';
        return frame;
    }
}
