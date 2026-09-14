package yier.bubu.redis.execution.api;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ExecutionRequestContractTest {
    @Test
    public void copyOfListCopiesBytesPreservesNullsAndReportsRetainedBytes() {
        byte[] commandName = ascii("SET");
        byte[] value = ascii("value");
        List<byte[]> args = new ArrayList<>();
        args.add(commandName);
        args.add(null);
        args.add(value);

        ExecutionRequest request = ByteArrayExecutionRequest.copyOf(args);
        commandName[0] = (byte) 'G';
        value[0] = (byte) 'X';

        Assert.assertEquals(3, request.argc());
        Assert.assertEquals(120, request.retainedBytes());
        Assert.assertArrayEquals(ascii("SET"), request.toByteArray(0));
        Assert.assertTrue(request.isNull(1));
        Assert.assertArrayEquals(ascii("value"), request.toByteArray(2));

        byte[] leaked = request.toByteArray(0);
        leaked[0] = (byte) 'N';
        Assert.assertArrayEquals(ascii("SET"), request.toByteArray(0));
    }

    @Test
    public void heapBackedRequestsExposeStableReadOnlyFastPath() {
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("SET", List.of("key"));

        byte[] first = request.readOnlyByteArray(0);
        byte[] second = request.readOnlyByteArray(0);

        Assert.assertSame(first, second);
        Assert.assertArrayEquals(ascii("SET"), first);
        Assert.assertNotSame(first, request.toByteArray(0));
    }

    @Test
    public void genericRequestsKeepReadOnlyFastPathDefensiveByDefault() {
        MutableExecutionRequest request = new MutableExecutionRequest("SET", "key");

        byte[] first = request.readOnlyByteArray(0);
        byte[] second = request.readOnlyByteArray(0);
        first[0] = (byte) 'N';

        Assert.assertNotSame(first, second);
        Assert.assertArrayEquals(ascii("SET"), request.readOnlyByteArray(0));
    }

    @Test
    public void wrappedReadOnlyArgvRequestKeepsStableReadOnlyBacking() {
        byte[] cmd = ascii("SET");
        byte[] key = ascii("key");

        ExecutionRequest request = ByteArrayExecutionRequest.wrapReadOnly(new byte[][]{cmd, key, null});

        Assert.assertSame(cmd, request.readOnlyByteArray(0));
        Assert.assertSame(key, request.readOnlyByteArray(1));
        Assert.assertTrue(request.isNull(2));
    }

    @Test
    public void factoriesShareTheSameHeapFootprintEstimate() {
        ExecutionRequest copy = ByteArrayExecutionRequest.copyOf(List.of(ascii("SET"), ascii("key")));
        ExecutionRequest utf8 = ByteArrayExecutionRequest.fromUtf8("SET", List.of("key"));
        ExecutionRequest wrapped = ByteArrayExecutionRequest.wrapReadOnly(new byte[][]{ascii("SET"), ascii("key")});

        Assert.assertEquals(112, copy.retainedBytes());
        Assert.assertEquals(112, utf8.retainedBytes());
        Assert.assertEquals(112, wrapped.retainedBytes());
    }

    @Test
    public void retainedBytesGrowWithArgumentCountForSamePayload() {
        ExecutionRequest singleArgument = ByteArrayExecutionRequest.copyOf(List.of(ascii("ab")));
        ExecutionRequest splitArguments = ByteArrayExecutionRequest.copyOf(List.of(ascii("a"), ascii("b")));

        Assert.assertTrue(splitArguments.retainedBytes() > singleArgument.retainedBytes());
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class MutableExecutionRequest implements ExecutionRequest {
        private final byte[][] argv;

        private MutableExecutionRequest(String... args) {
            this.argv = new byte[args.length][];
            for (int i = 0; i < args.length; i++) {
                this.argv[i] = args[i] == null ? null : ascii(args[i]);
            }
        }

        @Override
        public int argc() {
            return argv.length;
        }

        @Override
        public boolean isNull(int index) {
            return argv[index] == null;
        }

        @Override
        public int len(int index) {
            byte[] arg = argv[index];
            return arg == null ? -1 : arg.length;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return argv[index][offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            byte[] arg = argv[index];
            System.arraycopy(arg, 0, dst, dstOff, arg.length);
        }

        @Override
        public void close() {
        }
    }
}
