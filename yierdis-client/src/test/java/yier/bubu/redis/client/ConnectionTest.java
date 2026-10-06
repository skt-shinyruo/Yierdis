package yier.bubu.redis.client;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ConnectionTest {
    @Test
    public void defaultAddressAndOverriddenHostPortCompleteRawPing() throws Exception {
        try (TestServer server = TestServer.start(ConnectionSettings.DEFAULT_PORT);
             Connection connection = Connection.connect()) {
            Assert.assertEquals("PONG", connection.command("PING"));
        }
        try (TestServer server = TestServer.start();
             Connection connection = Connection.connect("127.0.0.1", server.port())) {
            Assert.assertEquals("PONG", connection.command(1_000, "PING"));
            Assert.assertEquals(0, connection.database());
        }
    }

    @Test
    public void missingGetIsNullAndSetThenGetReturnsTheString() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertNull(connection.command("GET", "missing"));
            Assert.assertEquals("OK", connection.command("SET", "k", "v"));
            Assert.assertEquals("v", connection.command("GET", "k"));
        }
    }

    @Test
    public void incrIsLongAndUnknownCommandLeavesTheConnectionUsable() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(Long.valueOf(1), connection.command("INCR", "n"));
            try {
                connection.command("NO_SUCH");
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR unknown command 'NO_SUCH'", e.getMessage());
            }
            Assert.assertEquals("PONG", connection.command("PING"));
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
                connection.command("GET", "bin");
                Assert.fail("expected DecodeException");
            } catch (DecodeException expected) {
            }
            Assert.assertEquals("PONG", connection.command("PING"));
        }
    }

    @Test
    public void unencodableStringIsRejectedBeforeWrite() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", connection.command("SET", "k", "stored"));
            try {
                connection.command("SET", "k", "\uD800");
                Assert.fail("expected DecodeException");
            } catch (DecodeException expected) {
            }
            Assert.assertEquals("stored", connection.command("GET", "k"));
        }
    }

    @Test
    public void nonPositiveCommandTimeoutIsRejectedAndPingStillReturnsPong() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", connection.command("SET", "k", "stored"));
            try {
                connection.command(0, "GET", "k");
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("commandTimeoutMillis"));
            }
            try {
                connection.command(-1, "PING");
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("commandTimeoutMillis"));
            }
            Assert.assertEquals("PONG", connection.command("PING"));
            Assert.assertEquals("stored", connection.command("GET", "k"));
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
                Assert.assertEquals("OK", db0.command("SET", "k", "from-zero"));
                Assert.assertNull(db1.command("GET", "k"));
                Assert.assertEquals("OK", db1.command("SET", "k", "from-one"));
                Assert.assertEquals("from-one", db1.command("GET", "k"));
                Assert.assertEquals("from-zero", db0.command("GET", "k"));

                Assert.assertEquals("OK", db0.command("select", "2"));
                Assert.assertEquals(2, db0.database());
                try {
                    db0.command("SELECT", "16");
                    Assert.fail("expected ServerException");
                } catch (ServerException e) {
                    Assert.assertEquals("ERR DB index is out of range", e.getMessage());
                }
                Assert.assertEquals(2, db0.database());
                Assert.assertEquals("PONG", db0.command("PING"));
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
                Assert.assertEquals("PONG", connection.command("PING"));
            }
        }
    }

    @Test
    public void quitClosesTheConnectionAndAServerErrorDoesNot() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            try {
                connection.command("QUIT", "extra");
                Assert.fail("expected ServerException");
            } catch (ServerException expected) {
            }
            Assert.assertEquals("PONG", connection.command("PING"));
            Assert.assertEquals("OK", connection.command("quit"));
            try {
                connection.command("PING");
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
        }
    }

    @Test
    public void hello3ClosesWhenTheReplyMarkerIsAMap() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            try {
                connection.command("HELLO", "3");
                Assert.fail("expected ConnectionException");
            } catch (ConnectionException expected) {
            }
            try {
                connection.command("PING");
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
        }
    }

    @Test
    public void rawHgetallStaysANestedList() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            connection.command("HSET", "h", "f", "v");
            Object reply = connection.command("HGETALL", "h");
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
                connection.command();
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
            }
            try {
                connection.command("");
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
            }
            Assert.assertEquals("PONG", connection.command("PING"));
            connection.close();
            try {
                connection.command("PING");
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
