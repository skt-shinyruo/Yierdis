package yier.bubu.redis.protocol.v1;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class CustomProtocolV1NdjsonEncoderTest {
    @Test
    public void writeBytesValueUtf8NoEscapeProducesJsonString() {
        ByteArraySink sink = new ByteArraySink();
        CustomProtocolV1NdjsonEncoder.writeBytesValue(sink, "hello".getBytes(StandardCharsets.UTF_8), 0, 5);
        Assert.assertEquals("\"hello\"", sink.utf8());
    }

    @Test
    public void writeBytesValueUtf8WithEscapeProducesEscapedJsonString() {
        ByteArraySink sink = new ByteArraySink();
        byte[] bytes = "a\nb\"c\\d\t".getBytes(StandardCharsets.UTF_8);
        CustomProtocolV1NdjsonEncoder.writeBytesValue(sink, bytes, 0, bytes.length);
        Assert.assertEquals("\"a\\nb\\\"c\\\\d\\t\"", sink.utf8());
    }

    @Test
    public void writeBytesValueInvalidUtf8FallsBackToTaggedB64() {
        ByteArraySink sink = new ByteArraySink();
        CustomProtocolV1NdjsonEncoder.writeBytesValue(sink, new byte[]{(byte) 0xC3, 0x28}, 0, 2);
        Assert.assertEquals("{\"$b64\":\"wyg=\"}", sink.utf8());
    }

    @Test
    public void writeBytesValueByteArrayAndSliceProduceSameOutput() {
        byte[] bytes = ("ASCII-" + "x".repeat(10) + "-\n-\"-\\-\t-中文").getBytes(StandardCharsets.UTF_8);
        ChunkLimitedBytesSlice slice = new ChunkLimitedBytesSlice(bytes, 8 * 1024);

        String fromArray = write(bytes);
        String fromSlice = write(slice);
        Assert.assertEquals(fromArray, fromSlice);
    }

    @Test
    public void writeBytesValueLargeInvalidUtf8B64MatchesJdkEncoder() {
        byte[] bytes = new byte[16 * 1024 + 1];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) 0xFF;
        }

        String out = write(bytes);
        Assert.assertTrue(out.startsWith("{\"$b64\":\""));
        Assert.assertTrue(out.endsWith("\"}"));

        String b64 = out.substring("{\"$b64\":\"".length(), out.length() - "\"}".length());
        Assert.assertEquals(Base64.getEncoder().encodeToString(bytes), b64);
    }

    private static String write(byte[] bytes) {
        ByteArraySink sink = new ByteArraySink();
        CustomProtocolV1NdjsonEncoder.writeBytesValue(sink, bytes, 0, bytes.length);
        return sink.utf8();
    }

    private static String write(BytesSlice slice) {
        ByteArraySink sink = new ByteArraySink();
        CustomProtocolV1NdjsonEncoder.writeBytesValue(sink, slice);
        return sink.utf8();
    }

    private static final class ChunkLimitedBytesSlice implements BytesSlice {
        private final byte[] data;
        private final int maxChunkBytes;

        private ChunkLimitedBytesSlice(byte[] data, int maxChunkBytes) {
            this.data = data;
            this.maxChunkBytes = maxChunkBytes;
        }

        @Override
        public int length() {
            return data.length;
        }

        @Override
        public byte getByte(int index) {
            throw new UnsupportedOperationException("getByte not supported in this test slice");
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            if (len > maxChunkBytes) {
                throw new AssertionError("getBytes len too large: " + len);
            }
            System.arraycopy(data, index, dst, dstOff, len);
        }

        @Override
        public void writeTo(BytesSink out) {
            out.writeBytes(data, 0, data.length);
        }
    }

    private static final class ByteArraySink implements BytesSink {
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        @Override
        public void writeBytes(byte[] src, int srcIndex, int len) {
            baos.write(src, srcIndex, len);
        }

        String utf8() {
            return baos.toString(StandardCharsets.UTF_8);
        }
    }
}

