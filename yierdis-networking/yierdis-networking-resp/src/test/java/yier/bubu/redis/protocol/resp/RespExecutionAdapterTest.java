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
    public void adaptsWrappedReadOnlyRequestWithoutCopyingInnerBytes() {
        byte[] command = bytes("SET");
        byte[] key = bytes("key");
        RespCommandRequest request = RespCommandRequest.wrapReadOnly(
                new byte[][]{command, key},
                command.length + key.length
        );

        ExecutionRequest out = RespExecutionAdapter.DEFAULT.toExecutionRequest(request);

        Assert.assertSame(command, out.readOnlyByteArray(0));
        Assert.assertSame(key, out.readOnlyByteArray(1));
        Assert.assertEquals(command.length + key.length, out.retainedBytes());
    }

    @Test
    public void copyOfPreservesNullArgvElement() {
        RespCommandRequest request = RespCommandRequest.copyOf(java.util.Arrays.asList(bytes("ECHO"), null));

        Assert.assertArrayEquals(bytes("ECHO"), request.readOnlyArg(0));
        Assert.assertNull(request.readOnlyArg(1));
        Assert.assertEquals(4, request.retainedBytes());
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
    public void wrapReadOnlyPreservesNullArgvElement() {
        RespCommandRequest request = RespCommandRequest.wrapReadOnly(
                new byte[][]{bytes("ECHO"), null},
                4
        );

        ExecutionRequest out = RespExecutionAdapter.DEFAULT.toExecutionRequest(request);
        Assert.assertArrayEquals(bytes("ECHO"), out.readOnlyByteArray(0));
        Assert.assertTrue(out.isNull(1));
        Assert.assertNull(out.readOnlyByteArray(1));
        Assert.assertEquals(4, out.retainedBytes());
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
