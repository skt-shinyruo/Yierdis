package yier.bubu.redis.contract;

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
        Assert.assertEquals(8, request.retainedBytes());
        Assert.assertArrayEquals(ascii("SET"), request.toByteArray(0));
        Assert.assertTrue(request.isNull(1));
        Assert.assertArrayEquals(ascii("value"), request.toByteArray(2));

        byte[] leaked = request.toByteArray(0);
        leaked[0] = (byte) 'N';
        Assert.assertArrayEquals(ascii("SET"), request.toByteArray(0));
    }

    @Test
    public void executionRecordNormalizesNegativeDbIndexAndSnapshotsRequest() {
        MutableExecutionRequest source = new MutableExecutionRequest("SET", "key", "value");

        ExecutionRecord record = new ExecutionRecord(-3, source);
        source.set(0, "GET");
        source.set(2, "next");

        Assert.assertEquals(0, record.dbIndex());
        Assert.assertNotSame(source, record.request());
        Assert.assertArrayEquals(ascii("SET"), record.request().toByteArray(0));
        Assert.assertArrayEquals(ascii("key"), record.request().toByteArray(1));
        Assert.assertArrayEquals(ascii("value"), record.request().toByteArray(2));
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

        private void set(int index, String value) {
            argv[index] = value == null ? null : ascii(value);
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
        public byte[] toByteArray(int index) {
            byte[] arg = argv[index];
            return arg == null ? null : arg.clone();
        }

        @Override
        public int retainedBytes() {
            int retained = 0;
            for (byte[] arg : argv) {
                retained += arg == null ? 0 : arg.length;
            }
            return retained;
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
