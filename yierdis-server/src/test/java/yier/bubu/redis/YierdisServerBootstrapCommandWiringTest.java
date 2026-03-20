package yier.bubu.redis;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.json.JsonArray;
import yier.bubu.redis.protocol.json.JsonBoolean;
import yier.bubu.redis.protocol.json.JsonLimits;
import yier.bubu.redis.protocol.json.JsonLong;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonParser;
import yier.bubu.redis.protocol.json.JsonString;
import yier.bubu.redis.protocol.json.JsonValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class YierdisServerBootstrapCommandWiringTest {
    @Test
    public void bootstrapWiresServerAndCoreConnectionCommandsTogether() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0", "--databases", "2")) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                socket.setSoTimeout(2000);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                JsonObject hello = roundTrip(out, in, "{\"cmd\":\"HELLO\",\"args\":[]}");
                Assert.assertTrue(booleanField(hello, "ok"));
                JsonObject helloResult = objectField(hello, "result");
                Assert.assertEquals("yierdis", stringValue(mapValue(helloResult, "server")));
                Assert.assertEquals(1L, longValue(mapValue(helloResult, "proto")));

                JsonObject info = roundTrip(out, in, "{\"cmd\":\"INFO\",\"args\":[\"yierdis\"]}");
                Assert.assertTrue(booleanField(info, "ok"));
                JsonObject infoResult = objectField(info, "result");
                Assert.assertEquals("yierdis", stringValue(mapValue(infoResult, "server")));
                Assert.assertTrue("expected structured INFO fields", mapContainsKey(infoResult, "executor_policy"));

                JsonObject stats = roundTrip(out, in, "{\"cmd\":\"STATS\",\"args\":[]}");
                Assert.assertTrue(booleanField(stats, "ok"));
                JsonObject statsResult = objectField(stats, "result");
                Assert.assertTrue(mapContainsKey(statsResult, "queued_tasks"));
                Assert.assertTrue(mapContainsKey(statsResult, "commands_executed_total"));

                JsonObject command = roundTrip(out, in, "{\"cmd\":\"COMMAND\",\"args\":[\"INFO\",\"HELLO\"]}");
                Assert.assertTrue(booleanField(command, "ok"));
                JsonArray commandResult = arrayField(command, "result");
                Assert.assertEquals(1, commandResult.values().size());
                Assert.assertTrue(commandResult.values().get(0) instanceof JsonArray);
                JsonArray helloInfo = (JsonArray) commandResult.values().get(0);
                Assert.assertEquals("hello", stringValue(helloInfo.values().get(0)));

                JsonObject select = roundTrip(out, in, "{\"cmd\":\"SELECT\",\"args\":[\"1\"]}");
                Assert.assertTrue(booleanField(select, "ok"));
                Assert.assertEquals("OK", stringField(select, "result"));
            }
        }
    }

    private static JsonObject roundTrip(OutputStream out, InputStream in, String json) throws IOException {
        writeFrame(out, json);
        return parseJsonObject(readReplyLine(in));
    }

    private static void writeFrame(OutputStream out, String json) throws IOException {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        byte[] head = (Integer.toString(payload.length) + ":").getBytes(StandardCharsets.US_ASCII);
        out.write(head);
        out.write(payload);
        out.write('\n');
        out.flush();
    }

    private static byte[] readReplyLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();
            if (b < 0) {
                if (buf.size() == 0) {
                    return null;
                }
                throw new IOException("unexpected EOF before reply newline");
            }
            if (b == '\n') {
                return buf.toByteArray();
            }
            buf.write(b);
        }
    }

    private static JsonObject parseJsonObject(byte[] bytes) {
        Assert.assertNotNull("expected JSON reply", bytes);
        JsonValue v = JsonParser.parseStrictUtf8(bytes, 0, bytes.length, JsonLimits.DEFAULT);
        Assert.assertTrue("expected JSON object", v instanceof JsonObject);
        return (JsonObject) v;
    }

    private static boolean booleanField(JsonObject obj, String key) {
        JsonValue v = requiredField(obj, key);
        Assert.assertTrue("expected boolean field: " + key, v instanceof JsonBoolean);
        return ((JsonBoolean) v).value();
    }

    private static JsonObject objectField(JsonObject obj, String key) {
        JsonValue v = requiredField(obj, key);
        Assert.assertTrue("expected object field: " + key, v instanceof JsonObject);
        return (JsonObject) v;
    }

    private static JsonArray arrayField(JsonObject obj, String key) {
        JsonValue v = requiredField(obj, key);
        Assert.assertTrue("expected array field: " + key, v instanceof JsonArray);
        return (JsonArray) v;
    }

    private static String stringField(JsonObject obj, String key) {
        return stringValue(requiredField(obj, key));
    }

    private static String stringValue(JsonValue v) {
        Assert.assertTrue("expected string value", v instanceof JsonString);
        return ((JsonString) v).value();
    }

    private static long longField(JsonObject obj, String key) {
        JsonValue v = requiredField(obj, key);
        return longValue(v);
    }

    private static long longValue(JsonValue v) {
        Assert.assertTrue("expected integer value", v instanceof JsonLong);
        return ((JsonLong) v).value();
    }

    private static boolean mapContainsKey(JsonObject mapObject, String key) {
        try {
            mapValue(mapObject, key);
            return true;
        } catch (AssertionError e) {
            return false;
        }
    }

    private static JsonValue mapValue(JsonObject mapObject, String key) {
        JsonArray entries = arrayField(mapObject, "$map");
        for (JsonValue entryValue : entries.values()) {
            Assert.assertTrue("expected map entry array", entryValue instanceof JsonArray);
            JsonArray entry = (JsonArray) entryValue;
            Assert.assertEquals("expected [key, value] entry", 2, entry.values().size());
            if (key.equals(stringValue(entry.values().get(0)))) {
                return entry.values().get(1);
            }
        }
        Assert.fail("missing map entry: " + key);
        return null;
    }

    private static JsonValue requiredField(JsonObject obj, String key) {
        Assert.assertNotNull(obj);
        Map<String, JsonValue> map = obj.values();
        JsonValue v = map.get(key);
        Assert.assertNotNull("missing field: " + key, v);
        return v;
    }
}
