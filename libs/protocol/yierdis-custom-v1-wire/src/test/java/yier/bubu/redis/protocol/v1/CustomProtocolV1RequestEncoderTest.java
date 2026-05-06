package yier.bubu.redis.protocol.v1;

import com.sun.management.ThreadMXBean;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.protocol.json.JsonArray;
import yier.bubu.redis.protocol.json.JsonLimits;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonParser;
import yier.bubu.redis.protocol.json.JsonString;
import yier.bubu.redis.protocol.json.JsonValue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class CustomProtocolV1RequestEncoderTest {
    @Test
    public void encodeRequestFrameUsesLenPrefixedJsonFrame() {
        byte[] frame = CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8("PING")));

        assertFrameEquals(frame, "{\"cmd\":\"PING\",\"args\":[]}");
    }

    @Test
    public void encodeRequestFrameTrimsAsciiWhitespaceAroundCommand() {
        byte[] frame = CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8(" \tPING\r\n ")));

        assertFrameEquals(frame, "{\"cmd\":\"PING\",\"args\":[]}");
    }

    @Test
    public void encodeRequestFrameSerializesNullArgsAsJsonNull() {
        byte[] frame = CustomProtocolV1RequestEncoder.encodeRequestFrame(Arrays.asList(utf8("ECHO"), null));

        assertFrameEquals(frame, "{\"cmd\":\"ECHO\",\"args\":[null]}");
    }

    @Test
    public void encodeRequestFrameUtf8ArgsAreEncodedExactlyOnce() {
        byte[] frame = CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8("ECHO"), utf8("中文🙂")));

        assertFrameEquals(frame, "{\"cmd\":\"ECHO\",\"args\":[\"中文🙂\"]}");
    }

    @Test
    public void encodeRequestFrameReplacesMalformedUtf8ArgsWithReplacementCharacter() {
        byte[] frame = CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8("ECHO"), new byte[]{(byte) 0xFF}));

        JsonObject payload = parsedPayload(frame);
        Assert.assertEquals("ECHO", stringField(payload, "cmd"));
        JsonArray args = arrayField(payload, "args");
        Assert.assertEquals(1, args.values().size());
        Assert.assertEquals("\uFFFD", ((JsonString) args.values().get(0)).value());
    }

    @Test
    public void writeRequestFrameOutputStreamMatchesByteArrayApi() throws Exception {
        List<byte[]> args = List.of(utf8("SET"), utf8("bench\"key"), utf8("line1\\line2\n中文"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        CustomProtocolV1RequestEncoder.writeRequestFrame((OutputStream) out, args);

        Assert.assertArrayEquals(CustomProtocolV1RequestEncoder.encodeRequestFrame(args), out.toByteArray());
    }

    @Test
    public void writeRequestFrameBytesSinkMatchesByteArrayApi() {
        List<byte[]> args = List.of(utf8("SET"), utf8("bench\"key"), utf8("line1\\line2\n中文"));

        Assert.assertArrayEquals(CustomProtocolV1RequestEncoder.encodeRequestFrame(args), writeFrameViaBytesSink(args));
    }

    @Test
    public void writeRequestFrameBytesSinkNormalizesMalformedUtf8LikeByteArrayApi() {
        List<byte[]> args = List.of(utf8("ECHO"), new byte[]{(byte) 0xFF});

        Assert.assertArrayEquals(CustomProtocolV1RequestEncoder.encodeRequestFrame(args), writeFrameViaBytesSink(args));
    }

    @Test
    public void writeRequestFrameBytesSinkAvoidsPerCallIntermediateAllocations() {
        ThreadMXBean threadMxBean = threadMxBeanOrNull();
        Assume.assumeNotNull(threadMxBean);

        if (!threadMxBean.isThreadAllocatedMemoryEnabled()) {
            threadMxBean.setThreadAllocatedMemoryEnabled(true);
        }

        List<byte[]> args = List.of(utf8("SET"), utf8("bench\"key"), utf8("line1\\line2\n中文"));
        byte[] intBuf = new byte[16];
        BytesSink sink = (src, srcIndex, len) -> {
        };

        for (int i = 0; i < 20_000; i++) {
            CustomProtocolV1RequestEncoder.writeRequestFrame(sink, args, intBuf);
        }

        long threadId = Thread.currentThread().getId();
        long before = threadMxBean.getThreadAllocatedBytes(threadId);
        for (int i = 0; i < 2_000; i++) {
            CustomProtocolV1RequestEncoder.writeRequestFrame(sink, args, intBuf);
        }
        long allocated = threadMxBean.getThreadAllocatedBytes(threadId) - before;

        Assert.assertTrue("allocated bytes for streaming path: " + allocated, allocated <= 8_192L);
    }

    @Test
    public void writeRequestFrameRejectsTooSmallAsciiIntBuffer() {
        IllegalArgumentException ex = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> CustomProtocolV1RequestEncoder.writeRequestFrame((src, srcIndex, len) -> {
                }, List.of(utf8("PING")), new byte[9])
        );
        Assert.assertEquals("asciiIntBuffer must be at least 10 bytes", ex.getMessage());
    }

    @Test
    public void encodeRequestFrameRejectsEmptyArgs() {
        IllegalArgumentException ex = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of())
        );
        Assert.assertEquals("args must not be empty", ex.getMessage());
    }

    @Test
    public void encodeRequestFrameRejectsBlankCommand() {
        IllegalArgumentException ex = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8("   ")))
        );
        Assert.assertEquals("command name must not be blank", ex.getMessage());
    }

    @Test
    public void encodeRequestFrameRejectsNullCommand() {
        IllegalArgumentException ex = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> CustomProtocolV1RequestEncoder.encodeRequestFrame(Arrays.asList((byte[]) null))
        );
        Assert.assertEquals("command name must not be null/empty", ex.getMessage());
    }

    private static void assertFrameEquals(byte[] frame, String expectedJson) {
        byte[] payload = payload(frame);
        Assert.assertEquals(payload.length, payloadLength(frame));
        Assert.assertArrayEquals(expectedJson.getBytes(StandardCharsets.UTF_8), payload);
    }

    private static JsonObject parsedPayload(byte[] frame) {
        byte[] payload = payload(frame);
        JsonValue parsed = JsonParser.parseStrictUtf8(payload, 0, payload.length, JsonLimits.DEFAULT);
        Assert.assertTrue(parsed instanceof JsonObject);
        return (JsonObject) parsed;
    }

    private static JsonArray arrayField(JsonObject obj, String key) {
        JsonValue value = obj.values().get(key);
        Assert.assertTrue("expected array field: " + key, value instanceof JsonArray);
        return (JsonArray) value;
    }

    private static String stringField(JsonObject obj, String key) {
        JsonValue value = obj.values().get(key);
        Assert.assertTrue("expected string field: " + key, value instanceof JsonString);
        return ((JsonString) value).value();
    }

    private static int payloadLength(byte[] frame) {
        int colon = indexOf(frame, (byte) ':');
        Assert.assertTrue("missing header colon", colon > 0);
        Assert.assertEquals('\n', frame[frame.length - 1]);
        return Integer.parseInt(new String(frame, 0, colon, StandardCharsets.US_ASCII));
    }

    private static byte[] payload(byte[] frame) {
        int colon = indexOf(frame, (byte) ':');
        Assert.assertTrue("missing header colon", colon > 0);
        Assert.assertEquals('\n', frame[frame.length - 1]);
        return Arrays.copyOfRange(frame, colon + 1, frame.length - 1);
    }

    private static int indexOf(byte[] bytes, byte expected) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == expected) {
                return i;
            }
        }
        return -1;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] writeFrameViaBytesSink(List<byte[]> args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BytesSink sink = (src, srcIndex, len) -> out.write(src, srcIndex, len);
        CustomProtocolV1RequestEncoder.writeRequestFrame(sink, args, new byte[16]);
        return out.toByteArray();
    }

    private static ThreadMXBean threadMxBeanOrNull() {
        if (!(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean threadMxBean)) {
            return null;
        }
        if (!threadMxBean.isThreadAllocatedMemorySupported()) {
            return null;
        }
        return threadMxBean;
    }
}
