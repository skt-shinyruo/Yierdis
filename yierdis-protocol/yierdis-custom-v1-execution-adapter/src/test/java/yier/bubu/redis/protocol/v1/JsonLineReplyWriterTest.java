package yier.bubu.redis.protocol.v1;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

public class JsonLineReplyWriterTest {
    @Test
    public void simpleStringProducesSuccessEnvelope() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.simpleString("OK");
        Assert.assertEquals("{\"ok\":true,\"result\":\"OK\"}\n", sink.utf8());
    }

    @Test
    public void errorProducesErrorEnvelope() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.error("ERR wrong");
        Assert.assertEquals("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"ERR wrong\"}}\n", sink.utf8());
    }

    @Test
    public void protocolErrorProducesErrorEnvelope() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.protocolError("Protocol error: bad frame");
        Assert.assertEquals("{\"ok\":false,\"error\":{\"kind\":\"protocol\",\"message\":\"Protocol error: bad frame\"}}\n", sink.utf8());
    }

    @Test
    public void internalErrorProducesErrorEnvelope() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.internalError("ERR internal error");
        Assert.assertEquals("{\"ok\":false,\"error\":{\"kind\":\"internal\",\"message\":\"ERR internal error\"}}\n", sink.utf8());
    }

    @Test
    public void arrayWithMixedValuesAndErrors() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.arrayHeader(3);
        w.integer(1);
        w.error("ERR nope");
        w.nullValue();
        Assert.assertEquals("{\"ok\":true,\"result\":[1,{\"$error\":{\"kind\":\"command\",\"message\":\"ERR nope\"}},null]}\n", sink.utf8());
    }

    @Test
    public void mapUsesStringKeys() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.mapHeader(2);
        w.bulkString("a".getBytes(StandardCharsets.UTF_8));
        w.integer(1);
        w.bulkString("b".getBytes(StandardCharsets.UTF_8));
        w.bulkString((byte[]) null);
        Assert.assertEquals("{\"ok\":true,\"result\":{\"$map\":[[\"a\",1],[\"b\",null]]}}\n", sink.utf8());
    }

    @Test
    public void utf8BytesAreEscapedInBulkString() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.bulkString("a\nb\"c\\d\t".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("{\"ok\":true,\"result\":\"a\\nb\\\"c\\\\d\\t\"}\n", sink.utf8());
    }

    @Test
    public void invalidUtf8BytesFallbackToB64TaggedValue() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.bulkString(new byte[]{(byte) 0xC3, 0x28});
        Assert.assertEquals("{\"ok\":true,\"result\":{\"$b64\":\"wyg=\"}}\n", sink.utf8());
    }

    @Test
    public void bytesSliceUtf8NoEscapeUsesWriteToAndAvoidsFullCopy() {
        byte[] bytes = "x".repeat(16 * 1024 + 3).getBytes(StandardCharsets.UTF_8);
        ChunkLimitedBytesSlice slice = new ChunkLimitedBytesSlice(bytes, 8 * 1024);

        String fromArray = writeBulkString(bytes);
        String fromSlice = writeBulkString(slice);
        Assert.assertEquals(fromArray, fromSlice);
        Assert.assertTrue(slice.writeToCalled);
        Assert.assertTrue(slice.getBytesCalls > 1);
    }

    @Test
    public void bytesSliceUtf8WithEscapeIsEscapedAndChunked() {
        byte[] bytes = new byte[16 * 1024 + 3];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) 'x';
        }
        bytes[1] = (byte) '\n';
        bytes[bytes.length / 2] = (byte) '"';
        bytes[bytes.length - 2] = (byte) '\\';

        ChunkLimitedBytesSlice slice = new ChunkLimitedBytesSlice(bytes, 8 * 1024);

        String fromArray = writeBulkString(bytes);
        String fromSlice = writeBulkString(slice);
        Assert.assertEquals(fromArray, fromSlice);
        Assert.assertFalse(slice.writeToCalled);
        Assert.assertTrue(slice.getBytesCalls > 1);
    }

    @Test
    public void bytesSliceInvalidUtf8FallbackToB64TaggedValueAndChunked() {
        byte[] bytes = new byte[16 * 1024 + 3];
        for (int i = 0; i + 1 < bytes.length; i += 2) {
            bytes[i] = (byte) 0xC3;
            bytes[i + 1] = 0x28;
        }
        if ((bytes.length & 1) == 1) {
            bytes[bytes.length - 1] = (byte) 0x80;
        }

        ChunkLimitedBytesSlice slice = new ChunkLimitedBytesSlice(bytes, 8 * 1024);

        String fromArray = writeBulkString(bytes);
        String fromSlice = writeBulkString(slice);
        Assert.assertEquals(fromArray, fromSlice);
        Assert.assertFalse(slice.writeToCalled);
        Assert.assertTrue(slice.getBytesCalls > 1);
    }

    @Test
    public void errorMessageIsSanitizedAndLimited() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        String msg = "ERR hi\r\nthere " + "x".repeat(300);
        w.error(msg);

        String sanitized = msg.replace('\r', ' ').replace('\n', ' ');
        sanitized = sanitized.substring(0, 256);
        Assert.assertEquals("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"" + sanitized + "\"}}\n", sink.utf8());
    }

    @Test
    public void replyWriterAuthorityMustStayDocumentedInReadmeAndProtocolSources() throws Exception {
        Path workspaceRoot = resolveWorkspaceRoot();
        Assert.assertNotNull("missing workspace root", workspaceRoot);

        String readme = Files.readString(workspaceRoot.resolve("README.md"), StandardCharsets.UTF_8);
        Assert.assertTrue(readme.contains("server command execution write-back still uses ReplyWriter"));

        String replyValueSource = Files.readString(
                workspaceRoot.resolve("yierdis-protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/reply/ReplyValue.java"),
                StandardCharsets.UTF_8
        );
        Assert.assertTrue(replyValueSource.contains("server 命令执行写回仍以 {@code ReplyWriter} 为准"));

        String encoderSource = Files.readString(
                workspaceRoot.resolve("yierdis-protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1NdjsonEncoder.java"),
                StandardCharsets.UTF_8
        );
        Assert.assertTrue(
                encoderSource.contains("server 主写回路径的语义 owner 仍是 {@code ReplyWriter}")
        );
    }

    private static String writeBulkString(byte[] bytes) {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.bulkString(bytes);
        return sink.utf8();
    }

    private static String writeBulkString(BytesSlice slice) {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.bulkString(slice);
        return sink.utf8();
    }

    private static final class ChunkLimitedBytesSlice implements BytesSlice {
        private final byte[] data;
        private final int maxChunkBytes;

        private int getBytesCalls;
        private boolean writeToCalled;

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
            getBytesCalls++;
            if (len > maxChunkBytes) {
                throw new AssertionError("getBytes len too large: " + len);
            }
            System.arraycopy(data, index, dst, dstOff, len);
        }

        @Override
        public void writeTo(BytesSink out) {
            writeToCalled = true;
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

    private static Path resolveWorkspaceRoot() {
        Path cwd = Paths.get("").toAbsolutePath().normalize();
        Path cursor = cwd;
        while (cursor != null) {
            if (Files.isRegularFile(cursor.resolve("README.md"))
                    && Files.isDirectory(cursor.resolve("yierdis-server/src/main/java"))
                    && Files.isDirectory(cursor.resolve("yierdis-protocol/yierdis-custom-v1-wire/src/main/java"))
                    && Files.isDirectory(cursor.resolve("yierdis-protocol/yierdis-custom-v1-execution-adapter/src/main/java"))) {
                return cursor;
            }
            cursor = cursor.getParent();
        }
        return null;
    }
}
