package yier.bubu.redis;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.protocol.v1.CustomProtocolV1Request;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ProtocolCommandAdapterTest {
    @Test
    public void adaptsCustomProtocolRequestToExecutionRequestPreservingUtf8NullsAndRetainedBytes() {
        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter());
        ExecutionRequest adapted = null;
        try {
            Assert.assertTrue(ch.writeInbound(new CustomProtocolV1Request(" ping 雪 ", Arrays.asList("alpha", null, "你好", ""))));

            Object inbound = ch.readInbound();
            Assert.assertTrue(inbound instanceof ExecutionRequest);
            Assert.assertFalse(inbound instanceof Command);

            adapted = (ExecutionRequest) inbound;
            Assert.assertEquals(5, adapted.argc());
            Assert.assertArrayEquals(utf8(" ping 雪 "), adapted.toByteArray(0));
            Assert.assertArrayEquals(utf8("alpha"), adapted.toByteArray(1));
            Assert.assertTrue(adapted.isNull(2));
            Assert.assertEquals(-1, adapted.len(2));
            Assert.assertArrayEquals(utf8("你好"), adapted.toByteArray(3));
            Assert.assertArrayEquals(utf8(""), adapted.toByteArray(4));

            int expectedRetainedBytes = utf8(" ping 雪 ").length
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

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
