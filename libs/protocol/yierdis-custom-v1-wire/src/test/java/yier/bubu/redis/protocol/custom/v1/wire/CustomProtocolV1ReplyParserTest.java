package yier.bubu.redis.protocol.custom.v1.wire;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.custom.v1.json.JsonArray;
import yier.bubu.redis.protocol.custom.v1.json.JsonLong;
import yier.bubu.redis.protocol.custom.v1.json.JsonNull;
import yier.bubu.redis.protocol.custom.v1.json.JsonObject;
import yier.bubu.redis.protocol.custom.v1.json.JsonString;
import yier.bubu.redis.protocol.custom.v1.json.JsonValue;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CustomProtocolV1ReplyParserTest {
    @Test
    public void parseReadsOkEnvelopeFromNdjsonLine() {
        CustomProtocolV1ReplyParser.ParsedReply reply = parse("{\"ok\":true,\"result\":\"PONG\"}\n");

        Assert.assertTrue(reply.isOkEnvelope());
        Assert.assertEquals(new JsonString("PONG"), reply.resultValue());
        Assert.assertEquals("{\"ok\":true,\"result\":\"PONG\"}\n", reply.lineUtf8());
    }

    @Test
    public void parseReadsErrorEnvelopeFromNdjsonLine() {
        CustomProtocolV1ReplyParser.ParsedReply reply =
                parse("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"ERR wrong\"}}");

        Assert.assertFalse(reply.isOkEnvelope());
        JsonObject error = reply.errorObject();
        Assert.assertEquals(new JsonString("command"), error.values().get("kind"));
        Assert.assertEquals(new JsonString("ERR wrong"), error.values().get("message"));
    }

    @Test
    public void parseDecodesTaggedValues() {
        CustomProtocolV1ReplyParser.ParsedReply reply =
                parse("{\"ok\":true,\"result\":{\"$map\":[[\"a\",1],[\"b\",{\"$b64\":\"wyg=\"}]]}}");

        Map<String, JsonValue> result = reply.decodeResultMapStringKeys();
        Assert.assertEquals(new JsonLong(1), result.get("a"));
        Assert.assertArrayEquals(new byte[]{(byte) 0xC3, 0x28}, CustomProtocolV1ReplyParser.decodeBytesOrNull(result.get("b")));
    }

    @Test
    public void parseDecodesUtf8StringsViaTaggedValueHelpers() {
        CustomProtocolV1ReplyParser.ParsedReply reply = parse("{\"ok\":true,\"result\":\"中文🙂\"}");

        Assert.assertEquals("中文🙂", CustomProtocolV1ReplyParser.decodeUtf8StringOrNull(reply.resultValue()));
    }

    @Test
    public void parseByteArrayCopiesInputAndReturnedLineBytes() {
        byte[] line = utf8("{\"ok\":true,\"result\":\"PONG\"}\n");

        CustomProtocolV1ReplyParser.ParsedReply reply = CustomProtocolV1ReplyParser.parse(line);
        line[0] = '!';

        byte[] exposed = reply.line();
        exposed[1] = '!';

        Assert.assertEquals("{\"ok\":true,\"result\":\"PONG\"}\n", reply.lineUtf8());
        Assert.assertNotSame(exposed, reply.line());
    }

    @Test
    public void parseByteArrayOffsetAndLengthUsesSelectedNdjsonSlice() {
        byte[] buffer = utf8("xx{\"ok\":true,\"result\":\"PONG\"}\nyy");

        CustomProtocolV1ReplyParser.ParsedReply reply =
                CustomProtocolV1ReplyParser.parse(buffer, 2, "{\"ok\":true,\"result\":\"PONG\"}\n".getBytes(StandardCharsets.UTF_8).length);

        Assert.assertTrue(reply.isOkEnvelope());
        Assert.assertEquals("{\"ok\":true,\"result\":\"PONG\"}\n", reply.lineUtf8());
    }

    @Test
    public void envelopeAccessorReturnsImmutableDetachedTree() {
        CustomProtocolV1ReplyParser.ParsedReply reply =
                parse("{\"ok\":true,\"result\":{\"nested\":{\"x\":1}}}\n");

        JsonObject envelope = reply.envelope();
        JsonObject result = (JsonObject) envelope.values().get("result");

        Assert.assertThrows(UnsupportedOperationException.class, () -> envelope.values().put("evil", JsonNull.INSTANCE));
        Assert.assertThrows(UnsupportedOperationException.class, () -> result.values().put("extra", JsonNull.INSTANCE));
        Assert.assertTrue(reply.isOkEnvelope());
    }

    @Test
    public void resultValueAndErrorObjectReturnImmutableDetachedTrees() {
        CustomProtocolV1ReplyParser.ParsedReply okReply =
                parse("{\"ok\":true,\"result\":{\"items\":[1,2]}}\n");
        JsonObject result = (JsonObject) okReply.resultValue();
        JsonArray items = (JsonArray) result.values().get("items");

        Assert.assertThrows(UnsupportedOperationException.class, () -> result.values().put("evil", JsonNull.INSTANCE));
        Assert.assertThrows(UnsupportedOperationException.class, () -> items.values().add(new JsonLong(3)));

        CustomProtocolV1ReplyParser.ParsedReply errorReply =
                parse("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"ERR wrong\"}}\n");
        JsonObject error = errorReply.errorObject();
        Assert.assertThrows(UnsupportedOperationException.class, () -> error.values().put("kind", new JsonString("protocol")));
    }

    @Test
    public void decodeResultMapStringKeysReturnsImmutableDetachedNestedValues() {
        CustomProtocolV1ReplyParser.ParsedReply plainReply =
                parse("{\"ok\":true,\"result\":{\"outer\":{\"x\":1}}}\n");

        Map<String, JsonValue> plain = plainReply.decodeResultMapStringKeys();
        Assert.assertThrows(UnsupportedOperationException.class, () -> plain.put("b", new JsonString("extra")));
        Assert.assertThrows(
                UnsupportedOperationException.class,
                () -> ((JsonObject) plain.get("outer")).values().put("y", new JsonLong(2))
        );

        CustomProtocolV1ReplyParser.ParsedReply taggedReply =
                parse("{\"ok\":true,\"result\":{\"$map\":[[\"outer\",{\"items\":[1]}]]}}\n");
        Map<String, JsonValue> tagged = taggedReply.decodeResultMapStringKeys();

        Assert.assertThrows(UnsupportedOperationException.class, () -> tagged.put("x", JsonNull.INSTANCE));
        Assert.assertThrows(
                UnsupportedOperationException.class,
                () -> ((JsonArray) ((JsonObject) tagged.get("outer")).values().get("items")).values().add(new JsonLong(2))
        );
    }

    @Test
    public void parseRejectsNonObjectEnvelope() {
        IllegalArgumentException ex = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> CustomProtocolV1ReplyParser.parse(utf8("[1,2,3]"))
        );
        Assert.assertEquals("expected reply envelope to be a JSON object", ex.getMessage());
    }

    private static CustomProtocolV1ReplyParser.ParsedReply parse(String line) {
        return CustomProtocolV1ReplyParser.parse(utf8(line));
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
