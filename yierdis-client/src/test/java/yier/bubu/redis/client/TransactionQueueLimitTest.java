package yier.bubu.redis.client;

// 事务队列上限与 EXECABORT 回归测试：覆盖 command/bytes 上限、EXECABORT、DISCARD 复位等关键路径。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.YierdisServerBootstrap;
import yier.bubu.redis.protocol.json.JsonArray;
import yier.bubu.redis.protocol.json.JsonBoolean;
import yier.bubu.redis.protocol.json.JsonNull;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonString;
import yier.bubu.redis.protocol.json.JsonValue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TransactionQueueLimitTest {
    @Test
    public void transactionQueueMaxCommandsTriggersExecAbortAndDiscardResets() throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--transactionQueueMaxCommands", "1",
                "--transactionQueueMaxBytes", "0"
        )) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                Assert.assertEquals("OK", stringResult(execute(client, b("MULTI"))));
                Assert.assertEquals("QUEUED", stringResult(execute(client, b("SET"), b("k"), b("v"))));

                JsonObject queueFull = errorObject(execute(client, b("GET"), b("k")));
                Assert.assertEquals("ERR Transaction queue is full", stringField(queueFull, "message"));

                JsonObject execAbort = errorObject(execute(client, b("EXEC")));
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", stringField(execAbort, "message"));

                JsonValue missing = resultValue(execute(client, b("GET"), b("k")));
                Assert.assertTrue(missing == null || missing instanceof JsonNull);

                // DISCARD 复位路径：超限后 DISCARD 应回到可用状态。
                Assert.assertEquals("OK", stringResult(execute(client, b("MULTI"))));
                Assert.assertEquals("QUEUED", stringResult(execute(client, b("SET"), b("x"), b("1"))));
                JsonObject queueFull2 = errorObject(execute(client, b("GET"), b("x")));
                Assert.assertEquals("ERR Transaction queue is full", stringField(queueFull2, "message"));
                Assert.assertEquals("OK", stringResult(execute(client, b("DISCARD"))));

                Assert.assertEquals("OK", stringResult(execute(client, b("MULTI"))));
                Assert.assertEquals("QUEUED", stringResult(execute(client, b("SET"), b("k"), b("v"))));
                JsonValue execResult = resultValue(execute(client, b("EXEC")));
                Assert.assertTrue(execResult instanceof JsonArray);
                List<JsonValue> values = ((JsonArray) execResult).values();
                Assert.assertNotNull(values);
                Assert.assertEquals(1, values.size());
                Assert.assertTrue(values.get(0) instanceof JsonString);
                Assert.assertEquals("OK", ((JsonString) values.get(0)).value());

                Assert.assertEquals("v", stringResult(execute(client, b("GET"), b("k"))));
            }
        }
    }

    @Test
    public void transactionQueueMaxBytesTriggersExecAbort() throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--transactionQueueMaxCommands", "0",
                "--transactionQueueMaxBytes", "16"
        )) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                Assert.assertEquals("OK", stringResult(execute(client, b("MULTI"))));
                byte[] big = new byte[64];
                Arrays.fill(big, (byte) 'a');

                JsonObject queueFull = errorObject(execute(client, b("SET"), b("k"), big));
                Assert.assertEquals("ERR Transaction queue is full", stringField(queueFull, "message"));

                JsonObject execAbort = errorObject(execute(client, b("EXEC")));
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", stringField(execAbort, "message"));
            }
        }
    }

    private static JsonValue execute(YierdisClient client, byte[]... args) throws Exception {
        return client.execute(Arrays.asList(args), 2000).envelope();
    }

    private static boolean okEnvelope(JsonValue envelope) {
        Assert.assertTrue(envelope instanceof JsonObject);
        JsonValue ok = ((JsonObject) envelope).values().get("ok");
        return ok instanceof JsonBoolean b && b.value();
    }

    private static JsonValue resultValue(JsonValue envelope) {
        Assert.assertTrue(envelope instanceof JsonObject);
        return ((JsonObject) envelope).values().get("result");
    }

    private static String stringResult(JsonValue envelope) {
        Assert.assertTrue(okEnvelope(envelope));
        JsonValue v = resultValue(envelope);
        Assert.assertTrue(v instanceof JsonString);
        return ((JsonString) v).value();
    }

    private static JsonObject errorObject(JsonValue envelope) {
        Assert.assertFalse(okEnvelope(envelope));
        Assert.assertTrue(envelope instanceof JsonObject);
        JsonValue e = ((JsonObject) envelope).values().get("error");
        Assert.assertTrue(e instanceof JsonObject);
        return (JsonObject) e;
    }

    private static String stringField(JsonObject obj, String key) {
        Assert.assertNotNull(obj);
        Map<String, JsonValue> map = obj.values();
        JsonValue v = map.get(key);
        Assert.assertTrue(v instanceof JsonString);
        return ((JsonString) v).value();
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class TestServer implements AutoCloseable {
        private final YierdisServerBootstrap server;

        private TestServer(YierdisServerBootstrap server) {
            this.server = server;
        }

        static TestServer startWithArgs(String... extraArgs) throws Exception {
            String[] base = new String[]{
                    "--port", "0",
                    "--ioThreads", "1",
                    "--noCleanup"
            };
            String[] argv = Arrays.copyOf(base, base.length + extraArgs.length);
            System.arraycopy(extraArgs, 0, argv, base.length, extraArgs.length);
            return new TestServer(YierdisServerBootstrap.start(argv));
        }

        int port() {
            return server.port();
        }

        @Override
        public void close() {
            server.close();
        }
    }
}

