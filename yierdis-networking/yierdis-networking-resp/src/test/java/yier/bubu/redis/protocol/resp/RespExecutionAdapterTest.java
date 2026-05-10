package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class RespExecutionAdapterTest {
    @Test
    public void convertsBinarySafeArgvToExecutionRequest() {
        byte[] command = bytes("SET");
        byte[] key = bytes("k");
        byte[] value = new byte[]{0, 1, 2};
        RespCommandRequest request = RespCommandRequest.copyOf(List.of(
                command,
                key,
                value
        ));

        ExecutionRequest out = RespExecutionAdapter.DEFAULT.toExecutionRequest(request);

        Assert.assertEquals(3, out.argc());
        Assert.assertArrayEquals(command, out.readOnlyByteArray(0));
        Assert.assertArrayEquals(key, out.readOnlyByteArray(1));
        Assert.assertArrayEquals(value, out.readOnlyByteArray(2));
        Assert.assertEquals(command.length + key.length + value.length, out.retainedBytes());
    }

    @Test
    public void rejectsNullArgvElement() {
        Assert.assertThrows(IllegalArgumentException.class, () ->
                RespCommandRequest.copyOf(java.util.Arrays.asList(bytes("GET"), null))
        );
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
