package yier.bubu.redis.client;

// 事务队列上限与 EXECABORT 回归测试：覆盖 command/bytes 上限、EXECABORT、DISCARD 复位等关键路径。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.YierdisServerBootstrap;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespFrame;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespObjectParser;
import yier.bubu.redis.protocol.RespSimpleString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class TransactionQueueLimitTest {
    @Test
    public void transactionQueueMaxCommandsTriggersExecAbortAndDiscardResets() throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--transactionQueueMaxCommands", "1",
                "--transactionQueueMaxBytes", "0"
        )) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                Assert.assertEquals("OK", simple(client.execute(Arrays.asList(b("MULTI")), 1000)));
                Assert.assertEquals("QUEUED", simple(client.execute(Arrays.asList(b("SET"), b("k"), b("v")), 1000)));

                RespError queueFull = (RespError) parse(client.execute(Arrays.asList(b("GET"), b("k")), 1000));
                Assert.assertEquals("ERR Transaction queue is full", queueFull.message());

                RespError execAbort = (RespError) parse(client.execute(Arrays.asList(b("EXEC")), 1000));
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", execAbort.message());

                RespBulkString missing = (RespBulkString) parse(client.execute(Arrays.asList(b("GET"), b("k")), 1000));
                Assert.assertTrue(missing.isNull());

                // DISCARD 复位路径：超限后 DISCARD 应回到可用状态。
                Assert.assertEquals("OK", simple(client.execute(Arrays.asList(b("MULTI")), 1000)));
                Assert.assertEquals("QUEUED", simple(client.execute(Arrays.asList(b("SET"), b("x"), b("1")), 1000)));
                RespError queueFull2 = (RespError) parse(client.execute(Arrays.asList(b("GET"), b("x")), 1000));
                Assert.assertEquals("ERR Transaction queue is full", queueFull2.message());
                Assert.assertEquals("OK", simple(client.execute(Arrays.asList(b("DISCARD")), 1000)));

                Assert.assertEquals("OK", simple(client.execute(Arrays.asList(b("MULTI")), 1000)));
                Assert.assertEquals("QUEUED", simple(client.execute(Arrays.asList(b("SET"), b("k"), b("v")), 1000)));
                RespArray execOk = (RespArray) parse(client.execute(Arrays.asList(b("EXEC")), 1000));
                Assert.assertNotNull(execOk.values());
                Assert.assertEquals(1, execOk.values().size());
                Assert.assertEquals("OK", ((RespSimpleString) execOk.values().get(0)).value());

                RespBulkString value = (RespBulkString) parse(client.execute(Arrays.asList(b("GET"), b("k")), 1000));
                Assert.assertEquals("v", value.asString());
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
                Assert.assertEquals("OK", simple(client.execute(Arrays.asList(b("MULTI")), 1000)));
                byte[] big = new byte[64];
                Arrays.fill(big, (byte) 'a');

                RespError queueFull = (RespError) parse(client.execute(Arrays.asList(b("SET"), b("k"), big), 1000));
                Assert.assertEquals("ERR Transaction queue is full", queueFull.message());

                RespError execAbort = (RespError) parse(client.execute(Arrays.asList(b("EXEC")), 1000));
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", execAbort.message());
            }
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RespObject parse(RespFrame frame) {
        try (RespFrame f = frame) {
            return RespObjectParser.parse(f);
        }
    }

    private static String simple(RespFrame frame) {
        RespObject obj = parse(frame);
        Assert.assertTrue(obj instanceof RespSimpleString);
        return ((RespSimpleString) obj).value();
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
