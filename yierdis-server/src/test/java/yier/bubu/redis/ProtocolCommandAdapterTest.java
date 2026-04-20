package yier.bubu.redis;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.protocol.netty.CustomRequestDecoder;
import yier.bubu.redis.protocol.v1.CustomProtocolV1ArgvRequest;
import yier.bubu.redis.protocol.v1.CustomProtocolV1Request;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ProtocolCommandAdapterTest {
    @Test
    public void adaptsByteBackedRequestWithoutUtf8Reencoding() {
        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter());
        byte[] cmd = utf8("PING");
        byte[] arg = utf8("alpha");
        ExecutionRequest adapted = null;
        try {
            Assert.assertTrue(ch.writeInbound(
                    CustomProtocolV1ArgvRequest.of(new byte[][]{cmd, arg, null}, cmd.length + arg.length)
            ));

            Object inbound = ch.readInbound();
            Assert.assertTrue(inbound instanceof ExecutionRequest);
            Assert.assertFalse(inbound instanceof Command);

            adapted = (ExecutionRequest) inbound;
            Assert.assertSame(cmd, adapted.readOnlyByteArray(0));
            Assert.assertSame(arg, adapted.readOnlyByteArray(1));
            Assert.assertTrue(adapted.isNull(2));
            Assert.assertEquals(cmd.length + arg.length, adapted.retainedBytes());
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
            Assert.assertArrayEquals(utf8("PING"), adapted.readOnlyByteArray(0));
            Assert.assertArrayEquals(utf8("alpha"), adapted.readOnlyByteArray(1));
        } finally {
            if (adapted != null) {
                adapted.close();
            }
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void legacyCustomProtocolRequestStillAdaptsForInternalCallers() {
        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter());
        ExecutionRequest adapted = null;
        try {
            Assert.assertTrue(ch.writeInbound(new CustomProtocolV1Request("PING", Arrays.asList("alpha", null))));

            Object inbound = ch.readInbound();
            Assert.assertTrue(inbound instanceof ExecutionRequest);
            adapted = (ExecutionRequest) inbound;
            Assert.assertArrayEquals(utf8("PING"), adapted.toByteArray(0));
            Assert.assertArrayEquals(utf8("alpha"), adapted.toByteArray(1));
            Assert.assertTrue(adapted.isNull(2));
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
