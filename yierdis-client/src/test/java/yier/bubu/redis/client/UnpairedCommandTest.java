package yier.bubu.redis.client;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.client.exception.CommandTimeoutException;
import yier.bubu.redis.client.exception.ConnectionException;
import yier.bubu.redis.client.exception.DecodeException;
import yier.bubu.redis.client.exception.ServerException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class UnpairedCommandTest {
    @Test
    public void outstandingCommandsCompleteInSubmitOrder() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            CompletableFuture<String> first = connection.set("a", "1");
            CompletableFuture<String> second = connection.set("b", "2");
            CompletableFuture<Object> bad = connection.command("NO_SUCH");
            CompletableFuture<String> third = connection.set("c", "3");
            Assert.assertEquals("OK", Await.join(first));
            Assert.assertEquals("OK", Await.join(second));
            try {
                Await.join(bad);
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR unknown command 'NO_SUCH'", e.getMessage());
            }
            Assert.assertEquals("OK", Await.join(third));
            Assert.assertEquals("1", Await.join(connection.get("a")));
            Assert.assertEquals("2", Await.join(connection.get("b")));
            Assert.assertEquals("3", Await.join(connection.get("c")));
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void multiIsRejectedWhileCommandsAreUnpaired() throws Exception {
        try (ServerSocket peer = new ServerSocket(0)) {
            Thread accepted = new Thread(() -> holdOpen(peer));
            accepted.setDaemon(true);
            accepted.start();
            try (Connection connection = Connection.connect("127.0.0.1", peer.getLocalPort())) {
                connection.ping();
                IllegalStateException rejected = Assert.assertThrows(IllegalStateException.class, connection::multi);
                Assert.assertTrue(rejected.getMessage().contains("unpaired"));
            }
        }
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction transaction = connection.multi();
            CompletableFuture<String> set = transaction.set("after", "stored");
            Assert.assertEquals(List.of("OK"), Await.join(transaction.exec()));
            Assert.assertEquals("OK", Await.join(set));
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    private static void holdOpen(ServerSocket peer) {
        try (Socket socket = peer.accept()) {
            socket.setSoTimeout(5_000);
            // 读到对端关闭为止。读完第一条命令就关套接字会让 MULTI 看到的是已关闭连接。
            while (socket.getInputStream().read() != -1) {
            }
        } catch (IOException ignored) {
        }
    }

    @Test
    public void rawConnectionTransactionUsesTheSameModeAndCanFinishNormally() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", Await.join(connection.command("mUlTi")));
            Assert.assertThrows(IllegalStateException.class, connection::multi);
            Assert.assertThrows(IllegalStateException.class, connection::ping);
            Assert.assertThrows(IllegalStateException.class, () -> connection.command("MULTI"));
            Assert.assertThrows(IllegalArgumentException.class, () -> connection.command(0, "SET", "k", "bad"));
            CompletableFuture<Object> set = connection.command(1_000, "SET", "k", "kept");
            Assert.assertEquals(List.of("OK"), Await.join(connection.command(1_000, "eXeC")));
            Assert.assertEquals("OK", Await.join(set));
            Assert.assertEquals("kept", Await.join(connection.get("k")));
            Assert.assertEquals("OK", Await.join(connection.command("MULTI")));
            CompletableFuture<Object> discarded = connection.command("SET", "k", "discarded");
            Assert.assertEquals("OK", Await.join(connection.command("dIsCaRd")));
            Assert.assertThrows(IllegalStateException.class, () -> Await.join(discarded));
            Assert.assertEquals("kept", Await.join(connection.get("k")));
        }
    }

    @Test
    public void rawExecPreservesRespShapesAndRawDiscardFinishesTheObject() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Await.join(connection.hset("hash", "field", "value"));
            Transaction executed = connection.multi();
            executed.hgetall("hash");
            executed.set("n", "abc");
            executed.incr("n");
            Assert.assertThrows(IllegalStateException.class, () -> executed.command("MULTI"));
            Assert.assertEquals(
                    List.of(List.of("field", "value"), "OK", "ERR value is not an integer or out of range"),
                    Await.join(executed.command("EXEC"))
            );
            Assert.assertThrows(IllegalStateException.class, () -> executed.command("PING"));
            Assert.assertEquals("PONG", Await.join(connection.ping()));

            Transaction discarded = connection.multi();
            discarded.set("n", "discarded");
            Assert.assertEquals("OK", Await.join(discarded.command("DISCARD")));
            Assert.assertThrows(IllegalStateException.class, discarded::exec);
            Assert.assertEquals("abc", Await.join(connection.get("n")));
        }
    }

    @Test
    public void rawExecAbortEndsTheTransactionAndSuccessfulSelectUpdatesTheDatabase() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction aborted = connection.multi();
            aborted.set("k", "discarded");
            Assert.assertThrows(ServerException.class, () -> Await.join(aborted.command("SET", "only-key")));
            Assert.assertThrows(ServerException.class, () -> Await.join(aborted.command("EXEC")));
            Assert.assertThrows(IllegalStateException.class, aborted::exec);
            Assert.assertNull(Await.join(connection.get("k")));

            Assert.assertEquals("OK", Await.join(connection.command("MULTI")));
            CompletableFuture<Object> select = connection.command("SELECT", "1");
            Assert.assertEquals(0, connection.database());
            Assert.assertEquals(List.of("OK"), Await.join(connection.command("EXEC")));
            Assert.assertEquals("OK", Await.join(select));
            Assert.assertEquals(1, connection.database());
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void multiSetCompletesAtExecAndExecListsResultsInOrder() throws Exception {
        Method set = Transaction.class.getMethod("set", String.class, String.class);
        Assert.assertEquals(CompletableFuture.class, set.getReturnType());
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction transaction = connection.multi();
            CompletableFuture<String> first = transaction.set("a", "1");
            CompletableFuture<Object> second = transaction.command("SET", "b", "2");
            transaction.set("n", "abc");
            CompletableFuture<Long> incr = transaction.incr("n");
            transaction.set("m", "ok");
            List<Object> results = Await.join(transaction.exec());
            Assert.assertEquals(5, results.size());
            Assert.assertEquals("OK", results.get(0));
            Assert.assertEquals("OK", results.get(1));
            Assert.assertEquals("OK", results.get(2));
            Assert.assertTrue(results.get(3) instanceof ServerException);
            Assert.assertEquals(
                    "ERR value is not an integer or out of range",
                    ((ServerException) results.get(3)).getMessage()
            );
            Assert.assertEquals("OK", results.get(4));
            Assert.assertEquals("OK", Await.join(first));
            Assert.assertEquals("OK", Await.join(second));
            Assert.assertThrows(ServerException.class, () -> Await.join(incr));
            Assert.assertEquals("1", Await.join(connection.get("a")));
            Assert.assertEquals("2", Await.join(connection.get("b")));
            Assert.assertEquals("abc", Await.join(connection.get("n")));
            Assert.assertEquals("ok", Await.join(connection.get("m")));
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void queueTimeErrorKeepsTransactionModeUntilDiscardOrExec() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction discarded = connection.multi();
            discarded.set("k", "v");
            try {
                Await.join(discarded.command("SET", "only-key"));
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR wrong number of arguments for 'set' command", e.getMessage());
            }
            Assert.assertThrows(IllegalStateException.class, connection::ping);
            Assert.assertEquals("OK", Await.join(discarded.discard()));
            Assert.assertNull(Await.join(connection.get("k")));
            Assert.assertThrows(IllegalStateException.class, discarded::exec);
            Assert.assertThrows(IllegalStateException.class, () -> discarded.set("k", "again"));
            Assert.assertEquals("PONG", Await.join(connection.ping()));

            Transaction aborted = connection.multi();
            aborted.set("k", "v");
            Assert.assertThrows(ServerException.class, () -> Await.join(aborted.command("SET", "only-key")));
            Assert.assertThrows(IllegalStateException.class, () -> connection.get("k"));
            try {
                Await.join(aborted.exec());
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals(
                        "EXECABORT Transaction discarded because of previous errors.",
                        e.getMessage()
                );
            }
            Assert.assertThrows(IllegalStateException.class, aborted::exec);
            Assert.assertThrows(IllegalStateException.class, () -> aborted.command("PING"));
            Assert.assertNull(Await.join(connection.get("k")));
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void emptyExecReturnsAnEmptyList() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction transaction = connection.multi();
            Assert.assertEquals(List.of(), Await.join(transaction.exec()));
            Assert.assertEquals("PONG", Await.join(connection.ping()));
            Assert.assertThrows(IllegalStateException.class, transaction::exec);
        }
    }

    @Test
    public void typedCommandDuringTransactionIsRejectedLocally() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", Await.join(connection.set("k", "old")));
            Transaction transaction = connection.multi();
            CompletableFuture<String> set = transaction.set("k", "new");
            Assert.assertThrows(IllegalStateException.class, () -> connection.set("k", "from-connection"));
            Assert.assertThrows(IllegalStateException.class, connection::multi);
            Assert.assertEquals(List.of("OK"), Await.join(transaction.exec()));
            Assert.assertEquals("OK", Await.join(set));
            Assert.assertEquals("new", Await.join(connection.get("k")));
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void closingMidTransactionDoesNotExec() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (Connection connection = connect(server)) {
                Assert.assertEquals("OK", Await.join(connection.set("k", "kept")));
                Transaction transaction = connection.multi();
                transaction.set("k", "lost");
                connection.close();
                Assert.assertThrows(IllegalStateException.class, transaction::exec);
            }
            try (Connection again = connect(server)) {
                Assert.assertEquals("kept", Await.join(again.get("k")));
            }
        }
    }

    @Test
    public void unencodableArgumentDoesNotLeaveTransactionMode() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction transaction = connection.multi();
            Assert.assertThrows(DecodeException.class, () -> transaction.set("k", "\uD800"));
            Assert.assertThrows(IllegalStateException.class, connection::ping);
            Assert.assertEquals("OK", Await.join(transaction.discard()));
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void closingWithAnUnpairedCommandFailsThatCommand() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            CompletableFuture<String> pending = connection.set("k", "v");
            connection.close();
            Assert.assertThrows(ConnectionException.class, () -> Await.join(pending));
            Assert.assertThrows(IllegalStateException.class, connection::ping);
        }
    }

    @Test
    public void transactionTimeoutFailsTheCallerFutureAndSurvivesQueued() throws Exception {
        try (ServerSocket peer = new ServerSocket(0)) {
            CountDownLatch sawExec = new CountDownLatch(1);
            CountDownLatch releaseExec = new CountDownLatch(1);
            Thread accepted = new Thread(() -> replyUntilExec(peer, sawExec, releaseExec));
            accepted.setDaemon(true);
            accepted.start();
            ConnectionSettings settings = ConnectionSettings.defaults()
                    .withHost("127.0.0.1")
                    .withPort(peer.getLocalPort())
                    .withCommandTimeoutMillis(400);
            try (Connection connection = Connection.connect(settings)) {
                Transaction transaction = connection.multi();
                CompletableFuture<Object> set = transaction.command(400, new String[] {"SET", "k", "v"});
                CompletableFuture<List<Object>> exec = transaction.exec();
                Assert.assertTrue(sawExec.await(2, TimeUnit.SECONDS));
                Assert.assertThrows(CommandTimeoutException.class, () -> Await.join(exec));
                Assert.assertThrows(CommandTimeoutException.class, () -> Await.join(set));
                releaseExec.countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (!connection.inNormalMode() && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                Assert.assertTrue(exec.isCompletedExceptionally());
                Assert.assertTrue(set.isCompletedExceptionally());
                Assert.assertTrue(connection.inNormalMode());
                Assert.assertEquals("PONG", Await.join(connection.ping()));
            }
        }
    }

    private static void replyUntilExec(ServerSocket peer, CountDownLatch sawExec, CountDownLatch releaseExec) {
        try (Socket socket = peer.accept()) {
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();
            while (true) {
                String name = readCommand(input);
                if (name == null) {
                    return;
                }
                if ("EXEC".equalsIgnoreCase(name)) {
                    sawExec.countDown();
                    if (!releaseExec.await(5, TimeUnit.SECONDS)) {
                        return;
                    }
                    output.write("*1\r\n+OK\r\n".getBytes(StandardCharsets.US_ASCII));
                } else if ("MULTI".equalsIgnoreCase(name)) {
                    output.write("+OK\r\n".getBytes(StandardCharsets.US_ASCII));
                } else if ("PING".equalsIgnoreCase(name)) {
                    output.write("+PONG\r\n".getBytes(StandardCharsets.US_ASCII));
                } else {
                    output.write("+QUEUED\r\n".getBytes(StandardCharsets.US_ASCII));
                }
                output.flush();
            }
        } catch (Exception ignored) {
        }
    }

    private static String readCommand(InputStream input) throws IOException {
        int star = input.read();
        if (star < 0) {
            return null;
        }
        if (star != '*') {
            throw new IOException("expected an array");
        }
        int count = Integer.parseInt(readLine(input));
        String name = null;
        for (int i = 0; i < count; i++) {
            if (input.read() != '$') {
                throw new IOException("expected a bulk string");
            }
            int length = Integer.parseInt(readLine(input));
            byte[] body = input.readNBytes(length);
            if (input.read() != '\r' || input.read() != '\n') {
                throw new IOException("bulk string did not end");
            }
            if (i == 0) {
                name = new String(body, StandardCharsets.US_ASCII);
            }
        }
        return name;
    }

    private static String readLine(InputStream input) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int previous = -1;
        while (true) {
            int current = input.read();
            if (current < 0) {
                throw new IOException("line ended early");
            }
            if (previous == '\r' && current == '\n') {
                byte[] bytes = line.toByteArray();
                return new String(bytes, 0, bytes.length - 1, StandardCharsets.US_ASCII);
            }
            line.write(current);
            previous = current;
        }
    }

    private static Connection connect(TestServer server) {
        return Connection.connect("127.0.0.1", server.port());
    }
}
