package yier.bubu.redis.protocol.custom.v1.json;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class JsonParserTest {
    @Test
    public void parseRequestObject() {
        String json = "{\"cmd\":\"PING\",\"args\":[]}";
        JsonValue v = JsonParser.parse(json, JsonLimits.DEFAULT);
        Assert.assertTrue(v instanceof JsonObject);
        JsonObject obj = (JsonObject) v;
        Assert.assertEquals("PING", ((JsonString) obj.values().get("cmd")).value());
        Assert.assertTrue(obj.values().get("args") instanceof JsonArray);
        Assert.assertEquals(List.of(), ((JsonArray) obj.values().get("args")).values());
    }

    @Test
    public void parseStringEscapes() {
        String json = "\"a\\\\b\\n\\t\\r\\\"c\"";
        JsonValue v = JsonParser.parse(json, JsonLimits.DEFAULT);
        Assert.assertTrue(v instanceof JsonString);
        Assert.assertEquals("a\\b\n\t\r\"c", ((JsonString) v).value());
    }

    @Test
    public void parseArrayAndNumbers() {
        JsonValue v = JsonParser.parse("[1,-2,3.5,true,false,null]", JsonLimits.DEFAULT);
        Assert.assertTrue(v instanceof JsonArray);
        JsonArray a = (JsonArray) v;
        Assert.assertEquals(6, a.values().size());
        Assert.assertEquals(1L, ((JsonLong) a.values().get(0)).value());
        Assert.assertEquals(-2L, ((JsonLong) a.values().get(1)).value());
        Assert.assertEquals(3.5d, ((JsonDouble) a.values().get(2)).value(), 0.0d);
        Assert.assertEquals(true, ((JsonBoolean) a.values().get(3)).value());
        Assert.assertEquals(false, ((JsonBoolean) a.values().get(4)).value());
        Assert.assertSame(JsonNull.INSTANCE, a.values().get(5));
    }

    @Test
    public void strictUtf8RejectsInvalid() {
        byte[] bad = new byte[]{(byte) 0xC3, (byte) 0x28}; // invalid UTF-8
        try {
            JsonParser.parseStrictUtf8(bad, 0, bad.length, JsonLimits.DEFAULT);
            Assert.fail("Expected JsonParseException");
        } catch (JsonParseException e) {
            Assert.assertTrue(e.getMessage().toLowerCase().contains("utf-8"));
        }
    }

    @Test
    public void strictUtf8ByteBufferRejectsInvalid() {
        byte[] bad = new byte[]{(byte) 0xC3, (byte) 0x28}; // invalid UTF-8
        ByteBuffer buf = ByteBuffer.wrap(bad);
        try {
            JsonParser.parseStrictUtf8(buf, JsonLimits.DEFAULT);
            Assert.fail("Expected JsonParseException");
        } catch (JsonParseException e) {
            Assert.assertTrue(e.getMessage().toLowerCase().contains("utf-8"));
        }
        Assert.assertEquals(0, buf.position());
    }

    @Test
    public void parseStrictUtf8RoundTrip() {
        byte[] utf8 = "{\"k\":\"v\"}".getBytes(StandardCharsets.UTF_8);
        JsonValue v = JsonParser.parseStrictUtf8(utf8, 0, utf8.length, JsonLimits.DEFAULT);
        Assert.assertEquals(new JsonObject(Map.of("k", new JsonString("v"))), v);
    }

    @Test
    public void parseStrictUtf8ByteBufferRoundTrip() {
        byte[] utf8 = "{\"k\":\"v\"}".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.wrap(utf8);
        JsonValue v = JsonParser.parseStrictUtf8(buf, JsonLimits.DEFAULT);
        Assert.assertEquals(new JsonObject(Map.of("k", new JsonString("v"))), v);
        Assert.assertEquals(0, buf.position());
    }

    @Test
    public void parseStrictUtf8DirectByteBufferRoundTrip() {
        byte[] utf8 = "{\"k\":\"v\"}".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocateDirect(utf8.length + 16);
        buf.position(8);
        buf.put(utf8);
        buf.flip();
        buf.position(8);

        ByteBuffer slice = buf.slice();
        JsonValue v = JsonParser.parseStrictUtf8(slice, JsonLimits.DEFAULT);
        Assert.assertEquals(new JsonObject(Map.of("k", new JsonString("v"))), v);

        Assert.assertEquals(8, buf.position());
        Assert.assertEquals(utf8.length + 8, buf.limit());
    }
}
