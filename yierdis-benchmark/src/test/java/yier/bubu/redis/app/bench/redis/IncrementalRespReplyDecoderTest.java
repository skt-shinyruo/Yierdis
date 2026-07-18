package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

public class IncrementalRespReplyDecoderTest {
    @Test
    public void simpleStringAndErrorRepliesPreserveText() throws Exception {
        BenchmarkRespReply simple = defaults().tryDecode(ascii("+PONG\r\n"));
        BenchmarkRespReply error = defaults().tryDecode(ascii("-ERR no such key\r\n"));

        Assert.assertEquals(BenchmarkRespReply.Kind.SIMPLE_STRING, simple.kind());
        Assert.assertEquals("PONG", simple.text());
        Assert.assertEquals(BenchmarkRespReply.Kind.ERROR, error.kind());
        Assert.assertEquals("ERR no such key", error.text());
    }

    @Test
    public void integersAcceptZeroSignedValuesAndLongBoundaries() throws Exception {
        Assert.assertEquals(0L, defaults().tryDecode(ascii(":0\r\n")).integerValue());
        Assert.assertEquals(-42L, defaults().tryDecode(ascii(":-42\r\n")).integerValue());
        Assert.assertEquals(Long.MAX_VALUE,
                defaults().tryDecode(ascii(":" + Long.MAX_VALUE + "\r\n")).integerValue());
        Assert.assertEquals(Long.MIN_VALUE,
                defaults().tryDecode(ascii(":" + Long.MIN_VALUE + "\r\n")).integerValue());
    }

    @Test
    public void bulkRepliesReportEmptyAndNonemptyLengths() throws Exception {
        BenchmarkRespReply empty = defaults().tryDecode(ascii("$0\r\n\r\n"));
        BenchmarkRespReply nonempty = defaults().tryDecode(ascii("$5\r\nhello\r\n"));

        Assert.assertEquals(BenchmarkRespReply.Kind.BULK_STRING, empty.kind());
        Assert.assertEquals(0, empty.bulkLength());
        Assert.assertEquals(BenchmarkRespReply.Kind.BULK_STRING, nonempty.kind());
        Assert.assertEquals(5, nonempty.bulkLength());
    }

    @Test
    public void resp2NullBulkAndNullArrayRemainDistinct() throws Exception {
        IncrementalRespReplyDecoder decoder = defaults();
        Assert.assertEquals(BenchmarkRespReply.Kind.NULL_BULK,
                decoder.tryDecode(ascii("$-1\r\n")).kind());
        Assert.assertEquals(BenchmarkRespReply.Kind.NULL_ARRAY,
                decoder.tryDecode(ascii("*-1\r\n")).kind());
    }

    @Test
    public void emptyAndNonemptyArraysReportOnlyTopLevelElementCount() throws Exception {
        BenchmarkRespReply empty = defaults().tryDecode(ascii("*0\r\n"));
        ByteBuffer nestedFrame = ascii(
                "*3\r\n:1\r\n$3\r\nfoo\r\n*2\r\n+OK\r\n$-1\r\n"
        );
        BenchmarkRespReply nested = defaults().tryDecode(nestedFrame);

        Assert.assertEquals(BenchmarkRespReply.Kind.ARRAY, empty.kind());
        Assert.assertEquals(0, empty.arrayLength());
        Assert.assertEquals(BenchmarkRespReply.Kind.ARRAY, nested.kind());
        Assert.assertEquals(3, nested.arrayLength());
        Assert.assertFalse(nestedFrame.hasRemaining());
    }

    @Test
    public void fragmentedBulkReplyConsumesFragmentsBeforeComplete() throws Exception {
        IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(1024, 1024, 1024, 32);
        ByteBuffer first = ascii("$3\r\nab");
        Assert.assertNull(decoder.tryDecode(first));
        Assert.assertFalse(first.hasRemaining());

        ByteBuffer second = ascii("c\r\n");
        BenchmarkRespReply reply = decoder.tryDecode(second);
        Assert.assertEquals(BenchmarkRespReply.Kind.BULK_STRING, reply.kind());
        Assert.assertEquals(3, reply.bulkLength());
        Assert.assertFalse(second.hasRemaining());
    }

    @Test
    public void fragmentedLargeArrayConsumesChildrenWithoutAggregateRetention() throws Exception {
        int childCount = 2048;
        byte[] header = ("*" + childCount + "\r\n").getBytes(StandardCharsets.US_ASCII);
        byte[] child = ":1\r\n".getBytes(StandardCharsets.US_ASCII);
        IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(16, 32, childCount, 4);
        ByteBuffer headerFragment = ByteBuffer.wrap(header);

        Assert.assertNull(decoder.tryDecode(headerFragment));
        Assert.assertFalse(headerFragment.hasRemaining());
        Assert.assertEquals(1, decoder.arrayDepth());

        for (int index = 0; index < childCount - 1; index++) {
            ByteBuffer fragment = ByteBuffer.wrap(child);
            Assert.assertNull(decoder.tryDecode(fragment));
            Assert.assertFalse(fragment.hasRemaining());
            Assert.assertEquals(1, decoder.arrayDepth());
        }

        for (int repeat = 0; repeat < 32; repeat++) {
            ByteBuffer empty = ByteBuffer.allocate(0);
            Assert.assertNull(decoder.tryDecode(empty));
            Assert.assertFalse(empty.hasRemaining());
            Assert.assertEquals(1, decoder.arrayDepth());
        }

        ByteBuffer finalChild = ByteBuffer.wrap(child);
        BenchmarkRespReply reply = decoder.tryDecode(finalChild);
        Assert.assertEquals(childCount, reply.arrayLength());
        Assert.assertFalse(finalChild.hasRemaining());
        Assert.assertEquals(0, decoder.arrayDepth());
    }

    @Test
    public void fragmentedLinesAndBulkBodiesResumeTheirPriorStage() throws Exception {
        IncrementalRespReplyDecoder lineDecoder = defaults();
        ByteBuffer lineText = ascii("+abc");
        Assert.assertNull(lineDecoder.tryDecode(lineText));
        Assert.assertFalse(lineText.hasRemaining());
        ByteBuffer lineCr = ascii("\r");
        Assert.assertNull(lineDecoder.tryDecode(lineCr));
        Assert.assertFalse(lineCr.hasRemaining());
        ByteBuffer lineLf = ascii("\n");
        Assert.assertEquals("abc", lineDecoder.tryDecode(lineLf).text());
        Assert.assertFalse(lineLf.hasRemaining());

        IncrementalRespReplyDecoder bulkDecoder = defaults();
        ByteBuffer bulkPrefix = ascii("$5\r\nhe");
        Assert.assertNull(bulkDecoder.tryDecode(bulkPrefix));
        Assert.assertFalse(bulkPrefix.hasRemaining());
        ByteBuffer bulkPayloadAndCr = ascii("llo\r");
        Assert.assertNull(bulkDecoder.tryDecode(bulkPayloadAndCr));
        Assert.assertFalse(bulkPayloadAndCr.hasRemaining());
        ByteBuffer bulkLf = ascii("\n");
        Assert.assertEquals(5, bulkDecoder.tryDecode(bulkLf).bulkLength());
        Assert.assertFalse(bulkLf.hasRemaining());
    }

    @Test
    public void fragmentedNestedArraysRetainPrimitiveStackProgress() throws Exception {
        IncrementalRespReplyDecoder decoder = defaults();
        ByteBuffer input = ascii("*1\r\n*2\r\n+one\r\n");

        Assert.assertNull(decoder.tryDecode(input));
        Assert.assertFalse(input.hasRemaining());
        Assert.assertEquals(2, decoder.arrayDepth());

        ByteBuffer second = ascii("+tw");
        Assert.assertNull(decoder.tryDecode(second));
        Assert.assertFalse(second.hasRemaining());
        Assert.assertEquals(2, decoder.arrayDepth());

        ByteBuffer third = ascii("o\r\n");
        BenchmarkRespReply reply = decoder.tryDecode(third);
        Assert.assertEquals(1, reply.arrayLength());
        Assert.assertFalse(third.hasRemaining());
    }

    @Test
    public void nonzeroTopLevelStartCompactsOnlyUnreadContinuation() throws Exception {
        IncrementalRespReplyDecoder decoder = defaults();
        ByteBuffer input = ByteBuffer.allocate(64);
        input.put("+OK\r\n*2\r\n:1\r\n".getBytes(StandardCharsets.US_ASCII)).flip();

        Assert.assertEquals("OK", decoder.tryDecode(input).text());
        Assert.assertEquals(5, input.position());
        Assert.assertNull(decoder.tryDecode(input));
        Assert.assertFalse(input.hasRemaining());

        input.compact().put(":2\r\n".getBytes(StandardCharsets.US_ASCII)).flip();
        BenchmarkRespReply reply = decoder.tryDecode(input);
        Assert.assertEquals(2, reply.arrayLength());
        Assert.assertFalse(input.hasRemaining());
    }

    @Test
    public void replacementDirectBufferContinuesFromConsumedState() throws Exception {
        IncrementalRespReplyDecoder decoder = defaults();
        ByteBuffer small = ByteBuffer.allocate(16);
        small.put("*2\r\n$3\r\nab".getBytes(StandardCharsets.US_ASCII)).flip();

        Assert.assertNull(decoder.tryDecode(small));
        Assert.assertFalse(small.hasRemaining());

        ByteBuffer grown = ByteBuffer.allocateDirect(64);
        grown.put("c\r\n:2\r\n".getBytes(StandardCharsets.US_ASCII)).flip();
        BenchmarkRespReply reply = decoder.tryDecode(grown);
        Assert.assertEquals(2, reply.arrayLength());
        Assert.assertFalse(grown.hasRemaining());
    }

    @Test
    public void oneByteDirectFragmentsDecodeLargeArrayWithoutPrefixReplay() throws Exception {
        int childCount = 2048;
        String header = "*" + childCount + "\r\n";
        String child = ":1\r\n";
        byte[] frame = (header + child.repeat(childCount)).getBytes(StandardCharsets.US_ASCII);
        IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(
                16, 32, childCount, 1
        );

        BenchmarkRespReply reply = null;
        for (int index = 0; index < frame.length; index++) {
            ByteBuffer fragment = ByteBuffer.allocateDirect(2);
            fragment.put((byte) 'x').put(frame[index]).flip();
            fragment.position(1);
            reply = decoder.tryDecode(fragment);
            Assert.assertFalse(fragment.hasRemaining());
            if (index + 1 < frame.length) {
                Assert.assertNull(reply);
            }
        }

        Assert.assertNotNull(reply);
        Assert.assertEquals(childCount, reply.arrayLength());
    }

    @Test
    public void malformedContinuationFailsAndClearsDecoderState() throws Exception {
        IncrementalRespReplyDecoder decoder = defaults();
        ByteBuffer partial = ascii("*2\r\n:1\r\n");
        Assert.assertNull(decoder.tryDecode(partial));
        Assert.assertFalse(partial.hasRemaining());

        IOException failure = Assert.assertThrows(IOException.class,
                () -> decoder.tryDecode(ascii("?")));
        Assert.assertTrue(failure.getMessage().contains("unsupported reply marker"));
        Assert.assertEquals("OK", decoder.tryDecode(ascii("+OK\r\n")).text());
    }

    @Test
    public void everyReplyShapeDecodesDeterministicallyFromOneByteFragments() throws Exception {
        String[] frames = {
                "+PONG\r\n",
                "-ERR failed\r\n",
                ":-123\r\n",
                "$-1\r\n",
                "$0\r\n\r\n",
                "$3\r\nabc\r\n",
                "*-1\r\n",
                "*0\r\n",
                "*2\r\n$1\r\na\r\n*1\r\n:2\r\n"
        };

        for (String frame : frames) {
            byte[] bytes = frame.getBytes(StandardCharsets.US_ASCII);
            IncrementalRespReplyDecoder decoder = defaults();
            BenchmarkRespReply reply = null;
            for (int index = 0; index < bytes.length; index++) {
                ByteBuffer fragment = ByteBuffer.wrap(new byte[]{bytes[index]});
                reply = decoder.tryDecode(fragment);
                Assert.assertFalse(fragment.hasRemaining());
                if (index + 1 < bytes.length) {
                    Assert.assertNull(reply);
                }
            }
            Assert.assertNotNull(reply);
        }
    }

    @Test
    public void coalescedRepliesAreDecodedOneAtATime() throws Exception {
        ByteBuffer input = ascii("+PONG\r\n:1\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n");
        IncrementalRespReplyDecoder decoder = defaults();
        Assert.assertEquals("PONG", decoder.tryDecode(input).text());
        Assert.assertEquals(7, input.position());
        Assert.assertEquals(1L, decoder.tryDecode(input).integerValue());
        Assert.assertEquals(11, input.position());
        Assert.assertEquals(2, decoder.tryDecode(input).arrayLength());
        Assert.assertFalse(input.hasRemaining());
    }

    @Test
    public void oversizedBulkAndMalformedCrlfFail() {
        IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(3, 10, 10, 4);
        Assert.assertThrows(IOException.class, () -> decoder.tryDecode(ascii("$4\r\ntest\r\n")));
        Assert.assertThrows(IOException.class, () -> decoder.tryDecode(ascii("+OK\n")));
    }

    @Test
    public void lineRepliesRequireStrictCrlf() {
        assertInvalid("+OK\n");
        assertInvalid("+OK\rX");
        assertInvalid("+O\nK\r\n");
        assertInvalid("-ERR\rX");
    }

    @Test
    public void finalLineCrIsConsumedWhileAwaitingLf() throws Exception {
        ByteBuffer input = ascii("xx+OK\r");
        input.position(2);

        Assert.assertNull(defaults().tryDecode(input));
        Assert.assertFalse(input.hasRemaining());
    }

    @Test
    public void bulkTerminatorsRequireStrictCrlf() {
        assertInvalid("$1\r\na\n");
        assertInvalid("$1\r\naX");
        assertInvalid("$1\r\na\rX");
    }

    @Test
    public void incompleteBulkTerminatorConsumesAvailableBytes() throws Exception {
        for (String frame : new String[]{"$1\r\na", "$1\r\na\r"}) {
            ByteBuffer input = ascii(frame);
            Assert.assertNull(defaults().tryDecode(input));
            Assert.assertFalse(input.hasRemaining());
        }
    }

    @Test
    public void numericFieldsRejectMalformedGrammar() {
        String[] malformed = {
                ":\r\n",
                ":-\r\n",
                ":+1\r\n",
                ": 1\r\n",
                ":1 \r\n",
                ":1x\r\n",
                "$+1\r\n",
                "$ 1\r\n",
                "*+1\r\n",
                "*1x\r\n"
        };

        for (String frame : malformed) {
            assertInvalid(frame);
        }
    }

    @Test
    public void numericFieldsRejectLongOverflow() {
        for (String frame : new String[]{
                ":9223372036854775808\r\n",
                ":-9223372036854775809\r\n",
                "$9223372036854775808\r\n",
                "*9223372036854775808\r\n"
        }) {
            assertInvalid(frame);
        }
    }

    @Test
    public void negativeLengthsOtherThanMinusOneAreRejected() {
        assertInvalid("$-2\r\n");
        assertInvalid("*-2\r\n");
        assertInvalid("$-9223372036854775808\r\n");
        assertInvalid("*-9223372036854775808\r\n");
    }

    @Test
    public void bulkLimitAcceptsBoundaryAndRejectsBoundaryPlusOneBeforePayload() throws Exception {
        IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(3, 32, 10, 4);

        Assert.assertEquals(3, decoder.tryDecode(ascii("$3\r\nabc\r\n")).bulkLength());
        Assert.assertThrows(IOException.class, () -> decoder.tryDecode(ascii("$4\r\n")));
    }

    @Test
    public void lineLimitCountsOnlyBytesBeforeCrlf() throws Exception {
        IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(10, 3, 10, 4);
        Assert.assertEquals("abc", decoder.tryDecode(ascii("+abc\r\n")).text());
        Assert.assertThrows(IOException.class, () -> decoder.tryDecode(ascii("+abcd\r\n")));
        Assert.assertThrows(IOException.class, () -> decoder.tryDecode(ascii("+abcd")));

        ByteBuffer incomplete = ascii("+abc\r");
        Assert.assertNull(decoder.tryDecode(incomplete));
        Assert.assertFalse(incomplete.hasRemaining());
    }

    @Test
    public void arrayLimitAcceptsBoundaryAndRejectsBoundaryPlusOneBeforeChildren() throws Exception {
        IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(10, 32, 2, 4);

        Assert.assertEquals(2, decoder.tryDecode(ascii("*2\r\n$-1\r\n$-1\r\n")).arrayLength());
        Assert.assertThrows(IOException.class, () -> decoder.tryDecode(ascii("*3\r\n")));
    }

    @Test
    public void depthLimitAcceptsExactBoundaryAndRejectsBoundaryPlusOne() throws Exception {
        IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(10, 32, 10, 2);

        BenchmarkRespReply reply = decoder.tryDecode(ascii("*1\r\n*1\r\n+OK\r\n"));
        Assert.assertEquals(1, reply.arrayLength());
        Assert.assertThrows(IOException.class,
                () -> decoder.tryDecode(ascii("*1\r\n*1\r\n*1\r\n+OK\r\n")));
    }

    @Test
    public void supportedMaximumDepthDecodesWithoutJvmStackGrowth() throws Exception {
        int supportedMaximumDepth = IncrementalRespReplyDecoder.MAX_SUPPORTED_DEPTH;
        IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(
                16, 32, 1, supportedMaximumDepth
        );

        BenchmarkRespReply reply = decoder.tryDecode(singletonArrays(supportedMaximumDepth));
        Assert.assertEquals(1, reply.arrayLength());
    }

    @Test
    public void constructorRejectsDepthAboveFiniteSupportedMaximum() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new IncrementalRespReplyDecoder(
                        16, 32, 1, IncrementalRespReplyDecoder.MAX_SUPPORTED_DEPTH + 1
                ));
    }

    @Test
    public void zeroDepthAllowsRootAndRejectsEveryArrayChild() throws Exception {
        IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(10, 32, 10, 0);

        Assert.assertEquals(0, decoder.tryDecode(ascii("*0\r\n")).arrayLength());
        Assert.assertEquals(BenchmarkRespReply.Kind.NULL_ARRAY,
                decoder.tryDecode(ascii("*-1\r\n")).kind());
        Assert.assertEquals("OK", decoder.tryDecode(ascii("+OK\r\n")).text());
        Assert.assertThrows(IOException.class,
                () -> decoder.tryDecode(ascii("*1\r\n*0\r\n")));
    }

    @Test
    public void resp3AndUnknownMarkersAreRejected() {
        for (char marker : new char[]{'_', '#', '%', '~', '>', ',', '(', '='}) {
            assertInvalid(marker + "0\r\n");
        }
    }

    @Test
    public void constructorRejectsNegativeLimitsAndNullInputClearly() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new IncrementalRespReplyDecoder(-1, 1, 1, 1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new IncrementalRespReplyDecoder(1, -1, 1, 1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new IncrementalRespReplyDecoder(1, 1, -1, 1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new IncrementalRespReplyDecoder(1, 1, 1, -1));

        IncrementalRespReplyDecoder zeroLimits = new IncrementalRespReplyDecoder(0, 0, 0, 0);
        NullPointerException failure = Assert.assertThrows(NullPointerException.class,
                () -> zeroLimits.tryDecode(null));
        Assert.assertTrue(failure.getMessage().contains("input"));
    }

    @Test
    public void readOnlyAndSlicedBuffersAdvanceOnlyTheirOwnPositions() throws Exception {
        ByteBuffer readOnly = ascii("xx+OK\r\n:2\r\nzz").asReadOnlyBuffer();
        readOnly.position(2);
        int readOnlyLimit = readOnly.limit();

        Assert.assertEquals("OK", defaults().tryDecode(readOnly).text());
        Assert.assertEquals(7, readOnly.position());
        Assert.assertEquals(2L, defaults().tryDecode(readOnly).integerValue());
        Assert.assertEquals(11, readOnly.position());
        Assert.assertEquals(readOnlyLimit, readOnly.limit());

        ByteBuffer parent = ascii("xx$3\r\nabc\r\nyy");
        parent.position(2);
        parent.limit(parent.limit() - 2);
        int parentPosition = parent.position();
        int parentLimit = parent.limit();
        ByteBuffer slice = parent.slice().asReadOnlyBuffer();

        Assert.assertEquals(3, defaults().tryDecode(slice).bulkLength());
        Assert.assertFalse(slice.hasRemaining());
        Assert.assertEquals(parentPosition, parent.position());
        Assert.assertEquals(parentLimit, parent.limit());
    }

    @Test
    public void bulkPayloadAndArrayChildrenAreNotRetainedByReplyShape() throws Exception {
        BenchmarkRespReply bulk = defaults().tryDecode(ascii("$5\r\nhello\r\n"));
        BenchmarkRespReply array = defaults().tryDecode(ascii("*2\r\n+first\r\n$6\r\nsecond\r\n"));

        Assert.assertEquals(5, bulk.bulkLength());
        Assert.assertNull(bulk.text());
        Assert.assertEquals(2, array.arrayLength());
        Assert.assertNull(array.text());
        for (Field field : BenchmarkRespReply.class.getDeclaredFields()) {
            Assert.assertFalse(field.getType().isArray());
            Assert.assertFalse(Collection.class.isAssignableFrom(field.getType()));
        }
    }

    @Test
    public void topLevelSimpleAndErrorTextUseUtf8() throws Exception {
        String text = "ready-\u4f60\u597d";

        BenchmarkRespReply simple = defaults().tryDecode(utf8("+" + text + "\r\n"));
        BenchmarkRespReply error = defaults().tryDecode(utf8("-" + text + "\r\n"));

        Assert.assertEquals(text, simple.text());
        Assert.assertEquals(text, error.text());
    }

    @Test
    public void integerReplyStoresPrimitiveValueAndBoxesOnlyOnExplicitAccess() throws Exception {
        Field integerField = BenchmarkRespReply.class.getDeclaredField("integer");
        Assert.assertEquals(long.class, integerField.getType());

        BenchmarkRespReply reply = defaults().tryDecode(ascii(":257\r\n"));
        Assert.assertEquals(257L, reply.integerValue());
        Assert.assertEquals(Long.valueOf(257L), reply.integer());
    }

    private static IncrementalRespReplyDecoder defaults() {
        return new IncrementalRespReplyDecoder(1024, 1024, 1024, 32);
    }

    private static ByteBuffer ascii(String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.US_ASCII));
    }

    private static ByteBuffer utf8(String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static ByteBuffer singletonArrays(int depth) {
        StringBuilder frame = new StringBuilder(depth * 4 + 5);
        for (int index = 0; index < depth; index++) {
            frame.append("*1\r\n");
        }
        frame.append("+OK\r\n");
        return ascii(frame.toString());
    }

    private static void assertInvalid(String frame) {
        Assert.assertThrows(IOException.class, () -> defaults().tryDecode(ascii(frame)));
    }
}
