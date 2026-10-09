package yier.bubu.redis.client;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.client.exception.CommandTimeoutException;
import yier.bubu.redis.client.exception.ConnectionException;
import yier.bubu.redis.client.exception.DecodeException;
import yier.bubu.redis.client.exception.ServerException;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ConnectionTest {
    @Test
    public void nestedResp3IsNotMaskedByAnEarlierUtf8Error() throws Exception {
        for (String marker : List.of("_\r\n", "%0\r\n", "~0\r\n")) {
            ByteArrayInputStream input = new ByteArrayInputStream(
                    ("*2\r\n+ÿ\r\n*1\r\n" + marker).getBytes(StandardCharsets.ISO_8859_1));
            RespClientCodec.RespReply reply = RespClientCodec.readReplyWithRawText(input, 1024);
            Assert.assertThrows(IOException.class, () -> Connection.convert(reply, false));
        }
    }

    @Test
    public void rawExecRecordsSelectEvenWhenAnotherResultFailsUtf8Decoding() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            writeBytes(server.port(), List.of(
                    "SET".getBytes(StandardCharsets.US_ASCII),
                    "binary".getBytes(StandardCharsets.US_ASCII),
                    new byte[]{(byte) 0xFF}));
            java.util.concurrent.CompletableFuture<Object> multi = connection.command("MULTI");
            java.util.concurrent.CompletableFuture<Object> get = connection.command("GET", "binary");
            java.util.concurrent.CompletableFuture<Object> select = connection.command("SELECT", "1");
            java.util.concurrent.CompletableFuture<Object> exec = connection.command("EXEC");
            Assert.assertEquals("OK", Await.join(multi));
            Assert.assertThrows(DecodeException.class, () -> Await.join(get));
            Assert.assertEquals("OK", Await.join(select));
            Assert.assertTrue(Await.join(exec) instanceof List);
            Assert.assertEquals(1, connection.database());
            Assert.assertEquals("OK", Await.join(connection.set("k", "on-one")));
            try (Connection db0 = connect(server)) {
                Assert.assertNull(Await.join(db0.get("k")));
            }
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void longMinimumIsReturnedByRawTypedAndTransactionCommands() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            String aboveMinimum = Long.toString(Long.MIN_VALUE + 1);
            Await.join(connection.set("minimum", aboveMinimum));
            Assert.assertEquals(Long.valueOf(Long.MIN_VALUE), Await.join(connection.decr("minimum")));
            Assert.assertEquals(Long.toString(Long.MIN_VALUE), Await.join(connection.get("minimum")));
            Await.join(connection.set("minimum", aboveMinimum));
            Assert.assertEquals(Long.valueOf(Long.MIN_VALUE), Await.join(connection.command("DECR", "minimum")));
            Await.join(connection.set("minimum", aboveMinimum));
            java.util.concurrent.CompletableFuture<Object> outstanding = connection.command("DECR", "minimum");
            java.util.concurrent.CompletableFuture<String> followed = connection.ping();
            Assert.assertEquals(Long.valueOf(Long.MIN_VALUE), Await.join(outstanding));
            Assert.assertEquals("PONG", Await.join(followed));
            Await.join(connection.set("minimum", aboveMinimum));
            Transaction transaction = connection.multi();
            transaction.decr("minimum");
            Assert.assertEquals(List.of(Long.MIN_VALUE), Await.join(transaction.exec()));
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void nestedInvalidUtf8IsRejectedAfterTheWholeFrameIsRead() throws Exception {
        for (String marker : List.of("+", "-")) {
            for (boolean preserveErrors : List.of(false, true)) {
                ByteArrayInputStream input = new ByteArrayInputStream(
                        ("*2\r\n*1\r\n" + marker + "ÿ\r\n+later\r\n+PONG\r\n")
                                .getBytes(StandardCharsets.ISO_8859_1));
                RespClientCodec.RespReply reply = RespClientCodec.readReplyWithRawText(input, 1024);
                Assert.assertThrows(DecodeException.class, () -> Connection.convert(reply, preserveErrors));
                Assert.assertEquals("PONG", Connection.convert(RespClientCodec.readReplyWithRawText(input, 1024), false));
            }
        }
        ByteArrayInputStream valid = new ByteArrayInputStream("*1\r\n+中文\uFFFD\r\n".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(List.of("中文\uFFFD"), Connection.convert(RespClientCodec.readReplyWithRawText(valid, 1024), false));
    }

    @Test
    public void defaultAddressAndOverriddenHostPortCompleteRawPing() throws Exception {
        try (TestServer server = TestServer.start(ConnectionSettings.DEFAULT_PORT);
             Connection connection = Connection.connect()) {
            Assert.assertEquals("PONG", Await.join(connection.command("PING")));
        }
        try (TestServer server = TestServer.start();
             Connection connection = Connection.connect("127.0.0.1", server.port())) {
            Assert.assertEquals("PONG", Await.join(connection.command(1_000, "PING")));
            Assert.assertEquals(0, connection.database());
        }
    }

    @Test
    public void missingGetIsNullAndSetThenGetReturnsTheString() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertNull(Await.join(connection.command("GET", "missing")));
            Assert.assertEquals("OK", Await.join(connection.command("SET", "k", "v")));
            Assert.assertEquals("v", Await.join(connection.command("GET", "k")));
        }
    }

    @Test
    public void incrIsLongAndUnknownCommandLeavesTheConnectionUsable() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.command("INCR", "n")));
            try {
                Await.join(connection.command("NO_SUCH"));
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR unknown command 'NO_SUCH'", e.getMessage());
            }
            Assert.assertEquals("PONG", Await.join(connection.command("PING")));
        }
    }

    @Test
    public void invalidUtf8BulkThrowsDecodeExceptionAndConnectionStaysUsable() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            writeBytes(server.port(), List.of(
                    "SET".getBytes(StandardCharsets.US_ASCII),
                    "bin".getBytes(StandardCharsets.US_ASCII),
                    new byte[]{(byte) 0xFF}
            ));
            try {
                Await.join(connection.command("GET", "bin"));
                Assert.fail("expected DecodeException");
            } catch (DecodeException expected) {
            }
            Assert.assertEquals("PONG", Await.join(connection.command("PING")));
        }
    }

    @Test
    public void unencodableStringIsRejectedBeforeWrite() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", Await.join(connection.command("SET", "k", "stored")));
            try {
                Await.join(connection.command("SET", "k", "\uD800"));
                Assert.fail("expected DecodeException");
            } catch (DecodeException expected) {
            }
            Assert.assertEquals("stored", Await.join(connection.command("GET", "k")));
        }
    }

    @Test
    public void nonPositiveCommandTimeoutIsRejectedAndPingStillReturnsPong() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", Await.join(connection.command("SET", "k", "stored")));
            try {
                Await.join(connection.command(0, "GET", "k"));
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("commandTimeoutMillis"));
            }
            try {
                Await.join(connection.command(-1, "PING"));
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("commandTimeoutMillis"));
            }
            Assert.assertEquals("PONG", Await.join(connection.command("PING")));
            Assert.assertEquals("stored", Await.join(connection.command("GET", "k")));
        }
    }

    @Test
    public void rejectsInvalidSettingsBeforeConnect() {
        Assert.assertFalse(ConnectionException.class.isAssignableFrom(CommandTimeoutException.class));
        try {
            ConnectionSettings.defaults().withConnectTimeoutMillis(0);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("connectTimeoutMillis"));
        }
        try {
            ConnectionSettings.defaults().withCommandTimeoutMillis(-5);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("commandTimeoutMillis"));
        }
        try {
            ConnectionSettings.defaults().withDatabase(-1);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("database"));
        }
        try {
            ConnectionSettings.defaults().withHost("");
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("host"));
        }
        try {
            ConnectionSettings.defaults().withPort(0);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("port"));
        }
        try {
            ConnectionSettings.defaults().withIoThreadCount(0);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("ioThreadCount"));
        }
    }

    @Test
    public void openedDatabaseSeesItsOwnValueAndFailedSetupSelectReturnsNoConnection() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (Connection db0 = connect(server);
                 Connection db1 = Connection.connect(ConnectionSettings.defaults()
                         .withPort(server.port())
                         .withDatabase(1))) {
                Assert.assertEquals(0, db0.database());
                Assert.assertEquals(1, db1.database());
                Assert.assertEquals("OK", Await.join(db0.command("SET", "k", "from-zero")));
                Assert.assertNull(Await.join(db1.command("GET", "k")));
                Assert.assertEquals("OK", Await.join(db1.command("SET", "k", "from-one")));
                Assert.assertEquals("from-one", Await.join(db1.command("GET", "k")));
                Assert.assertEquals("from-zero", Await.join(db0.command("GET", "k")));

                Assert.assertEquals("OK", Await.join(db0.command("select", "2")));
                Assert.assertEquals(2, db0.database());
                try {
                    Await.join(db0.command("SELECT", "16"));
                    Assert.fail("expected ServerException");
                } catch (ServerException e) {
                    Assert.assertEquals("ERR DB index is out of range", e.getMessage());
                }
                Assert.assertEquals(2, db0.database());
                Assert.assertEquals("PONG", Await.join(db0.command("PING")));
            }

            try {
                Connection.connect(ConnectionSettings.defaults()
                        .withPort(server.port())
                        .withDatabase(16));
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR DB index is out of range", e.getMessage());
            }
            try (Connection connection = connect(server)) {
                Assert.assertEquals(0, connection.database());
                Assert.assertEquals("PONG", Await.join(connection.command("PING")));
            }
        }
    }

    @Test
    public void quitClosesTheConnectionAndAServerErrorDoesNot() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            try {
                Await.join(connection.command("QUIT", "extra"));
                Assert.fail("expected ServerException");
            } catch (ServerException expected) {
            }
            Assert.assertEquals("PONG", Await.join(connection.command("PING")));
            Assert.assertEquals("OK", Await.join(connection.command("quit")));
            try {
                Await.join(connection.command("PING"));
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
        }
    }

    @Test
    public void hello3FailsThatCommandAndLeavesTheConnectionUsable() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            try {
                Await.join(connection.command("HELLO", "3"));
                Assert.fail("expected ConnectionException");
            } catch (ConnectionException expected) {
            }
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void rawHgetallStaysANestedList() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Await.join(connection.command("HSET", "h", "f", "v"));
            Object reply = Await.join(connection.command("HGETALL", "h"));
            Assert.assertTrue(reply instanceof List);
            Assert.assertFalse(reply instanceof java.util.Map);
            Assert.assertEquals(List.of("f", "v"), reply);
        }
    }

    @Test
    public void emptyCommandIsRejectedAndCloseRejectsLaterCommands() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            try {
                Await.join(connection.command());
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
            }
            try {
                Await.join(connection.command(""));
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
            }
            Assert.assertEquals("PONG", Await.join(connection.command("PING")));
            connection.close();
            try {
                Await.join(connection.command("PING"));
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
        }
    }

    private static Connection connect(TestServer server) {
        return Connection.connect("127.0.0.1", server.port());
    }

    private static void writeBytes(int port, List<byte[]> args) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(5_000);
            OutputStream out = socket.getOutputStream();
            RespClientCodec.writeCommand(out, args);
            out.flush();
            RespClientCodec.readReply(socket.getInputStream(), RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
        }
    }
}
