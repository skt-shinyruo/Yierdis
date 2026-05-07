package yier.bubu.redis.protocol.custom.v1.execution;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.protocol.custom.v1.wire.CustomProtocolV1ArgvRequest;
import yier.bubu.redis.protocol.custom.v1.wire.CustomProtocolV1Request;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class CustomProtocolV1ExecutionAdapterTest {
    private final CustomProtocolV1ExecutionAdapter adapter = CustomProtocolV1ExecutionAdapter.DEFAULT;

    @Test
    public void adaptsByteBackedRequestWithoutUtf8Reencoding() {
        byte[] cmd = utf8("PING");
        byte[] arg = utf8("alpha");
        ExecutionRequest adapted = adapter.toExecutionRequest(
                CustomProtocolV1ArgvRequest.of(new byte[][]{cmd, arg, null}, cmd.length + arg.length)
        );
        try {
            Assert.assertSame(cmd, adapted.readOnlyByteArray(0));
            Assert.assertSame(arg, adapted.readOnlyByteArray(1));
            Assert.assertTrue(adapted.isNull(2));
            Assert.assertEquals(cmd.length + arg.length, adapted.retainedBytes());
        } finally {
            adapted.close();
        }
    }

    @Test
    public void adaptsLegacyUtf8Request() {
        ExecutionRequest adapted = adapter.toExecutionRequest(
                new CustomProtocolV1Request("PING", Arrays.asList("alpha", null))
        );
        try {
            Assert.assertArrayEquals(utf8("PING"), adapted.toByteArray(0));
            Assert.assertArrayEquals(utf8("alpha"), adapted.toByteArray(1));
            Assert.assertTrue(adapted.isNull(2));
        } finally {
            adapted.close();
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
