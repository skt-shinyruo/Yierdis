package yier.bubu.redis.app.client;

// 事务队列上限与 EXECABORT 回归测试：覆盖 command/bytes 上限、EXECABORT、DISCARD 复位等关键路径。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.YierdisServerBootstrap;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class TransactionQueueLimitTest {
    @Test
    public void transactionQueueMaxCommandsTriggersExecAbortAndDiscardResets() throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--transactionQueueMaxCommands", "1",
                "--transactionQueueMaxBytes", "0"
        );
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            Assert.assertEquals("OK", stringResult(execute(client, b("MULTI"))));
            Assert.assertEquals("QUEUED", stringResult(execute(client, b("SET"), b("k"), b("v"))));

            YierdisClient.RespReply queueFull = execute(client, b("GET"), b("k"));
            Assert.assertEquals("ERR Transaction queue is full", errorText(queueFull));

            YierdisClient.RespReply execAbort = execute(client, b("EXEC"));
            Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", errorText(execAbort));

            YierdisClient.RespReply missing = execute(client, b("GET"), b("k"));
            Assert.assertTrue(missing.isNull());

            // DISCARD 复位路径：超限后 DISCARD 应回到可用状态。
            Assert.assertEquals("OK", stringResult(execute(client, b("MULTI"))));
            Assert.assertEquals("QUEUED", stringResult(execute(client, b("SET"), b("x"), b("1"))));
            YierdisClient.RespReply queueFull2 = execute(client, b("GET"), b("x"));
            Assert.assertEquals("ERR Transaction queue is full", errorText(queueFull2));
            Assert.assertEquals("OK", stringResult(execute(client, b("DISCARD"))));

            Assert.assertEquals("OK", stringResult(execute(client, b("MULTI"))));
            Assert.assertEquals("QUEUED", stringResult(execute(client, b("SET"), b("k"), b("v"))));
            YierdisClient.RespReply execResult = execute(client, b("EXEC"));
            Assert.assertEquals(YierdisClient.RespReply.Kind.ARRAY, execResult.kind());
            List<YierdisClient.RespReply> values = execResult.values();
            Assert.assertNotNull(values);
            Assert.assertEquals(1, values.size());
            Assert.assertEquals("OK", stringResult(values.get(0)));

            Assert.assertEquals("v", stringResult(execute(client, b("GET"), b("k"))));
        }
    }

    @Test
    public void transactionQueueMaxBytesTriggersExecAbort() throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--transactionQueueMaxCommands", "0",
                "--transactionQueueMaxBytes", "16"
        );
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            Assert.assertEquals("OK", stringResult(execute(client, b("MULTI"))));
            byte[] big = new byte[64];
            Arrays.fill(big, (byte) 'a');

            YierdisClient.RespReply queueFull = execute(client, b("SET"), b("k"), big);
            Assert.assertEquals("ERR Transaction queue is full", errorText(queueFull));

            YierdisClient.RespReply execAbort = execute(client, b("EXEC"));
            Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", errorText(execAbort));
        }
    }

    private static YierdisClient.RespReply execute(YierdisClient client, byte[]... args) throws Exception {
        return client.execute(Arrays.asList(args), 2000);
    }

    private static String stringResult(YierdisClient.RespReply reply) {
        Assert.assertNotNull(reply);
        if (reply.kind() == YierdisClient.RespReply.Kind.SIMPLE_STRING) {
            return reply.text();
        }
        Assert.assertEquals(YierdisClient.RespReply.Kind.BULK_STRING, reply.kind());
        return new String(reply.bytes(), StandardCharsets.UTF_8);
    }

    private static String errorText(YierdisClient.RespReply reply) {
        Assert.assertNotNull(reply);
        Assert.assertEquals(YierdisClient.RespReply.Kind.ERROR, reply.kind());
        return reply.text();
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
                    "--noCleanup",
                    "--maxmemoryBytes", "0"
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
