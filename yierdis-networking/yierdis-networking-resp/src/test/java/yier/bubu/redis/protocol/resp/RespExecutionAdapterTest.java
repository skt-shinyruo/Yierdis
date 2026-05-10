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

    @Test
    public void copyOfDefensivelyCopiesArgvBytes() {
        byte[] command = bytes("SET");
        byte[] key = bytes("key");

        RespCommandRequest request = RespCommandRequest.copyOf(List.of(command, key));
        command[0] = (byte) 'G';
        key[0] = (byte) 'x';

        Assert.assertArrayEquals(bytes("SET"), request.readOnlyArg(0));
        Assert.assertArrayEquals(bytes("key"), request.readOnlyArg(1));
    }

    @Test
    public void wrapReadOnlyCopiesOuterArrayAndKeepsInnerBackingByConvention() {
        byte[] command = bytes("SET");
        byte[] key = bytes("key");
        byte[][] argv = new byte[][]{command, key};

        RespCommandRequest request = RespCommandRequest.wrapReadOnly(argv, -1);
        argv[0] = bytes("GET");

        Assert.assertSame(command, request.readOnlyArg(0));
        Assert.assertSame(key, request.readOnlyArg(1));
        Assert.assertEquals(0, request.retainedBytes());
    }

    @Test
    public void wrapReadOnlyRejectsNullArgvElement() {
        Assert.assertThrows(IllegalArgumentException.class, () ->
                RespCommandRequest.wrapReadOnly(new byte[][]{bytes("GET"), null}, 3)
        );
    }

    @Test
    public void copyOfSaturatesRetainedBytesOnOverflow() {
        Assert.assertEquals(Integer.MAX_VALUE, RespCommandRequest.saturatedRetainedBytes(Integer.MAX_VALUE - 1, 2));
        Assert.assertEquals(Integer.MAX_VALUE, RespCommandRequest.saturatedRetainedBytes(Integer.MAX_VALUE, 1));
        Assert.assertEquals(5, RespCommandRequest.saturatedRetainedBytes(3, 2));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
