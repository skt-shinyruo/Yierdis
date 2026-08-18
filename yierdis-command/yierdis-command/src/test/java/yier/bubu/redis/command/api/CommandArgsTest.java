package yier.bubu.redis.command.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class CommandArgsTest {
    @Test
    public void exposesNullAndByteArgumentsWithoutTakingRequestOwnership() {
        TrackingRequest request = request("CMD", "value", null);
        CommandArgs args = new CommandArgs(request);

        Assert.assertSame(request, args.request());
        Assert.assertEquals(3, args.argc());
        Assert.assertFalse(args.isNull(1));
        Assert.assertTrue(args.isNull(2));
        Assert.assertEquals(5, args.length(1));
        Assert.assertEquals(-1, args.length(2));
        Assert.assertSame(request.readOnlyByteArray(1), args.bytes(1));
        Assert.assertNull(args.bytes(2));
        Assert.assertNull(args.utf8(2));
        Assert.assertEquals(0, request.retainCount());
        Assert.assertEquals(0, request.closeCount());
    }

    @Test
    public void sliceReadsAndWritesTheRequestWithoutOwningOrClosingIt() {
        TrackingRequest request = request("CMD", "value");
        CommandArgs args = new CommandArgs(request);
        BytesSlice slice = args.slice(1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        Assert.assertEquals(5, slice.length());
        Assert.assertEquals('v', slice.getByte(0));
        slice.writeTo(output::write);
        Assert.assertArrayEquals(bytes("value"), output.toByteArray());
        Assert.assertEquals(0, request.retainCount());
        Assert.assertEquals(0, request.closeCount());
    }

    @Test
    public void comparesAsciiIgnoringCaseAndDecodesUtf8() {
        CommandArgs args = args("CMD", "Ex", "\u96ea", null);

        Assert.assertTrue(args.is(1, "ex"));
        Assert.assertTrue(args.is(1, "EX"));
        Assert.assertFalse(args.is(1, "px"));
        Assert.assertFalse(args.is(1, null));
        Assert.assertFalse(args.is(3, "ex"));
        Assert.assertEquals("\u96ea", args.utf8(2));
    }

    @Test
    public void parsesLongEndpointsAndRejectsInvalidNumbersWithExplicitRedisError() throws Exception {
        Assert.assertEquals(Long.MIN_VALUE, args("CMD", Long.toString(Long.MIN_VALUE)).longAt(1));
        Assert.assertEquals(Long.MAX_VALUE, args("CMD", Long.toString(Long.MAX_VALUE)).longAt(1));

        for (String invalid : List.of("", "+", "-", "9223372036854775808", "x")) {
            assertIntegerFailure(() -> args("CMD", invalid).longAt(1));
        }
        assertIntegerFailure(() -> args("CMD", (String) null).longAt(1));
    }

    @Test
    public void validatesRangesAndClampsIntegers() throws Exception {
        Assert.assertEquals(0L, args("CMD", "0").nonNegativeLongAt(1));
        Assert.assertEquals(1L, args("CMD", "1").positiveLongAt(1));
        assertIntegerFailure(() -> args("CMD", "-1").nonNegativeLongAt(1));
        assertIntegerFailure(() -> args("CMD", "0").positiveLongAt(1));

        Assert.assertEquals(Integer.MIN_VALUE, args("CMD", Long.toString(Long.MIN_VALUE)).intClampedAt(1));
        Assert.assertEquals(Integer.MAX_VALUE, args("CMD", Long.toString(Long.MAX_VALUE)).intClampedAt(1));
        Assert.assertEquals(42, args("CMD", "42").intClampedAt(1));
    }

    @Test
    public void returnsAnImmutableListOfReadOnlyByteArrays() {
        TrackingRequest request = request("CMD", "one", "two");
        CommandArgs args = new CommandArgs(request);

        List<byte[]> arrays = args.byteArraysFrom(1);

        Assert.assertEquals(2, arrays.size());
        Assert.assertSame(request.readOnlyByteArray(1), arrays.get(0));
        Assert.assertSame(request.readOnlyByteArray(2), arrays.get(1));
        Assert.assertThrows(UnsupportedOperationException.class, () -> arrays.add(bytes("three")));
        Assert.assertThrows(UnsupportedOperationException.class, () -> args.byteArraysFrom(3).add(bytes("x")));
        Assert.assertEquals(0, request.retainCount());
        Assert.assertEquals(0, request.closeCount());
    }

    @Test
    public void leavesIndexViolationsAsFrameworkDefects() {
        CommandArgs args = args("CMD", "1");

        Assert.assertThrows(IndexOutOfBoundsException.class, () -> args.length(2));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> args.longAt(2));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> args.byteArraysFrom(-1));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> args.byteArraysFrom(3));
    }

    private static void assertIntegerFailure(ThrowingOperation operation) {
        CommandParseException failure = Assert.assertThrows(CommandParseException.class, operation::run);
        Assert.assertEquals("ERR value is not an integer or out of range", failure.getMessage());
    }

    private static CommandArgs args(String command, String... rest) {
        return new CommandArgs(request(command, rest));
    }

    private static TrackingRequest request(String command, String... rest) {
        return new TrackingRequest(ByteArrayExecutionRequest.fromUtf8(command, Arrays.asList(rest)));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    private interface ThrowingOperation {
        void run() throws Exception;
    }

    private static final class TrackingRequest implements ExecutionRequest {
        private final ExecutionRequest delegate;
        private int retainCount;
        private int closeCount;

        private TrackingRequest(ExecutionRequest delegate) {
            this.delegate = delegate;
        }

        @Override
        public int argc() {
            return delegate.argc();
        }

        @Override
        public boolean isNull(int index) {
            return delegate.isNull(index);
        }

        @Override
        public int len(int index) {
            return delegate.len(index);
        }

        @Override
        public byte byteAt(int index, int offset) {
            return delegate.byteAt(index, offset);
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            delegate.copyToByteArray(index, dst, dstOff);
        }

        @Override
        public byte[] toByteArray(int index) {
            return delegate.toByteArray(index);
        }

        @Override
        public byte[] readOnlyByteArray(int index) {
            return delegate.readOnlyByteArray(index);
        }

        @Override
        public ExecutionRequest retain() {
            retainCount++;
            return this;
        }

        @Override
        public void close() {
            closeCount++;
        }

        private int retainCount() {
            return retainCount;
        }

        private int closeCount() {
            return closeCount;
        }
    }
}
