package yier.bubu.redis.app.client;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.YierdisServerBootstrap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class YierdisCliTest {
    @Test
    public void helpAndInvalidArgumentsReturnWithoutConnecting() {
        CliResult help = runWithIo("", "--help");
        Assert.assertEquals(0, help.status());
        Assert.assertTrue(help.out().contains("Usage: yierdis-cli"));
        Assert.assertTrue(help.err().isEmpty());

        CliResult invalid = runWithIo("", "--port", "not-a-number");
        Assert.assertEquals(2, invalid.status());
        Assert.assertTrue(invalid.err().contains("--port"));
        Assert.assertTrue(invalid.err().contains("Usage: yierdis-cli"));
    }

    @Test
    public void oneShotCommandsPrintEveryCommonReplyShape() throws Exception {
        try (TestServer server = TestServer.start()) {
            CliResult simple = run(server, "PING");
            Assert.assertEquals(0, simple.status());
            Assert.assertEquals("PONG\n", simple.out());

            CliResult bulk = run(server, "ECHO", "hello");
            Assert.assertEquals(0, bulk.status());
            Assert.assertEquals("hello\n", bulk.out());

            CliResult integer = run(server, "EXISTS", "missing");
            Assert.assertEquals(integer.toString(), 0, integer.status());
            Assert.assertEquals("0\n", integer.out());

            CliResult nil = run(server, "GET", "missing");
            Assert.assertEquals(0, nil.status());
            Assert.assertEquals("(nil)\n", nil.out());

            CliResult array = run(server, "HELLO");
            Assert.assertEquals(0, array.status());
            Assert.assertTrue(array.out().contains("1) server"));
            Assert.assertTrue(array.out().contains("2) yierdis"));

            CliResult error = run(server, "NO_SUCH_COMMAND");
            Assert.assertEquals(1, error.status());
            Assert.assertTrue(error.out().startsWith("(error) ERR unknown command"));
        }
    }

    @Test
    public void replRunsCommandsReportsParseErrorsAndQuits() throws Exception {
        try (TestServer server = TestServer.start()) {
            CliResult result = runWithIo(
                    "\nPING\nECHO \"hello world\"\nSET 'unterminated\nquit\n",
                    "--host", "127.0.0.1",
                    "--port", Integer.toString(server.port()),
                    "--timeoutMillis", "2000"
            );

            Assert.assertEquals(result.toString(), 0, result.status());
            Assert.assertTrue(result.out().contains("PONG\n"));
            Assert.assertTrue(result.out().contains("hello world\n"));
            Assert.assertTrue(result.out().contains("yierdis> "));
            Assert.assertTrue(result.err().contains("(error)"));
        }
    }

    @Test
    public void replReturnsSuccessfullyAtEndOfInput() throws Exception {
        try (TestServer server = TestServer.start()) {
            CliResult result = runWithIo(
                    "",
                    "--host", "127.0.0.1",
                    "--port", Integer.toString(server.port()),
                    "--timeoutMillis", "2000"
            );

            Assert.assertEquals(0, result.status());
            Assert.assertEquals("yierdis> ", result.out());
            Assert.assertTrue(result.err().isEmpty());
        }
    }

    @Test
    public void hexModePrintsInvalidUtf8BulkBytesWithoutReplacement() throws Exception {
        byte[] reply = new byte[]{'$', '1', '\r', '\n', (byte) 0xff, '\r', '\n'};
        try (ScriptedSocketServer server = ScriptedSocketServer.start(socket -> {
            readAtLeastOneRequest(socket);
            socket.getOutputStream().write(reply);
            socket.getOutputStream().flush();
            while (socket.getInputStream().read() >= 0) {
            }
        })) {
            CliResult result = runWithIo(
                    "",
                    "--host", "127.0.0.1",
                    "--port", Integer.toString(server.port()),
                    "--timeoutMillis", "2000",
                    "--hex",
                    "PING"
            );

            Assert.assertEquals(result.toString(), 0, result.status());
            Assert.assertEquals("0xff\n", result.out());
            Assert.assertTrue(result.err().isEmpty());
            server.assertSucceeded();
        }
    }

    @Test
    public void connectionRefusalIsReportedByTheCliEntryPoint() throws Exception {
        int unusedPort;
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            unusedPort = socket.getLocalPort();
        }

        CliResult result = runWithIo(
                "",
                "--host", "127.0.0.1",
                "--port", Integer.toString(unusedPort),
                "--timeoutMillis", "100",
                "PING"
        );

        Assert.assertEquals(result.toString(), 1, result.status());
        Assert.assertTrue(result.out().isEmpty());
        Assert.assertTrue(result.err().startsWith("(error) "));
    }

    @Test
    public void responseTimeoutIsReportedAndClosesTheOneShotConnection() throws Exception {
        try (ScriptedSocketServer server = ScriptedSocketServer.start(socket -> {
            readAtLeastOneRequest(socket);
            try {
                while (socket.getInputStream().read() >= 0) {
                }
            } catch (IOException ignored) {
                // 超时路径会主动关闭客户端 socket，服务端只需确认请求已经到达。
            }
        })) {
            CliResult result = runOneShot(server.port(), 50, "PING");

            Assert.assertEquals(result.toString(), 1, result.status());
            Assert.assertTrue(result.out().isEmpty());
            Assert.assertTrue(result.err().contains("Timeout waiting for response"));
            server.assertSucceeded();
        }
    }

    @Test
    public void truncatedReplyAndImmediateServerEofAreReported() throws Exception {
        try (ScriptedSocketServer truncated = ScriptedSocketServer.start(socket -> {
            readAtLeastOneRequest(socket);
            socket.getOutputStream().write("$5\r\nabc".getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
        })) {
            CliResult result = runOneShot(truncated.port(), 1_000, "PING");

            Assert.assertEquals(result.toString(), 1, result.status());
            Assert.assertTrue(result.err().toLowerCase().contains("closed"));
            truncated.assertSucceeded();
        }

        try (ScriptedSocketServer eof = ScriptedSocketServer.start(YierdisCliTest::readAtLeastOneRequest)) {
            CliResult result = runOneShot(eof.port(), 1_000, "PING");

            Assert.assertEquals(result.toString(), 1, result.status());
            Assert.assertTrue(result.err().toLowerCase().contains("closed"));
            eof.assertSucceeded();
        }
    }

    @Test
    public void replContinuesAfterCommandFailureUntilInputEnds() throws Exception {
        try (ScriptedSocketServer server = ScriptedSocketServer.start(YierdisCliTest::readAtLeastOneRequest)) {
            CliResult result = runWithIo(
                    "PING\nPING\n",
                    "--host", "127.0.0.1",
                    "--port", Integer.toString(server.port()),
                    "--timeoutMillis", "1000"
            );

            Assert.assertEquals(result.toString(), 0, result.status());
            Assert.assertEquals(2, occurrences(result.err(), "(error) "));
            Assert.assertTrue(result.out().endsWith("yierdis> "));
            server.assertSucceeded();
        }
    }

    @Test
    public void bestEffortQuitFailureStillReturnsSuccess() throws Exception {
        try (ScriptedSocketServer server = ScriptedSocketServer.start(YierdisCliTest::readAtLeastOneRequest)) {
            CliResult result = runWithIo(
                    "quit\n",
                    "--host", "127.0.0.1",
                    "--port", Integer.toString(server.port()),
                    "--timeoutMillis", "1000"
            );

            Assert.assertEquals(result.toString(), 0, result.status());
            Assert.assertEquals("yierdis> ", result.out());
            Assert.assertTrue(result.err().isEmpty());
            server.assertSucceeded();
        }
    }

    private static CliResult run(TestServer server, String... command) {
        String[] args = new String[command.length + 6];
        args[0] = "--host";
        args[1] = "127.0.0.1";
        args[2] = "--port";
        args[3] = Integer.toString(server.port());
        args[4] = "--timeoutMillis";
        args[5] = "2000";
        System.arraycopy(command, 0, args, 6, command.length);
        return runWithIo("", args);
    }

    private static CliResult runOneShot(int port, int timeoutMillis, String... command) {
        String[] args = new String[command.length + 6];
        args[0] = "--host";
        args[1] = "127.0.0.1";
        args[2] = "--port";
        args[3] = Integer.toString(port);
        args[4] = "--timeoutMillis";
        args[5] = Integer.toString(timeoutMillis);
        System.arraycopy(command, 0, args, 6, command.length);
        return runWithIo("", args);
    }

    private static void readAtLeastOneRequest(Socket socket) throws IOException {
        socket.setSoTimeout(2_000);
        byte[] request = new byte[256];
        if (socket.getInputStream().read(request) < 0) {
            throw new AssertionError("client closed before sending a command");
        }
    }

    private static int occurrences(String value, String needle) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(needle, offset)) >= 0) {
            count++;
            offset += needle.length();
        }
        return count;
    }

    private static CliResult runWithIo(String input, String... args) {
        InputStream previousIn = System.in;
        PrintStream previousOut = System.out;
        PrintStream previousErr = System.err;
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        try {
            System.setIn(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
            System.setOut(out);
            System.setErr(err);
            return new CliResult(
                    YierdisCli.run(args),
                    outBytes.toString(StandardCharsets.UTF_8),
                    errBytes.toString(StandardCharsets.UTF_8)
            );
        } finally {
            out.flush();
            err.flush();
            System.setIn(previousIn);
            System.setOut(previousOut);
            System.setErr(previousErr);
            out.close();
            err.close();
        }
    }

    private record CliResult(int status, String out, String err) {
    }

    private static final class TestServer implements AutoCloseable {
        private final YierdisServerBootstrap server;

        private TestServer(YierdisServerBootstrap server) {
            this.server = server;
        }

        private static TestServer start() throws Exception {
            return new TestServer(YierdisServerBootstrap.start(
                    "--port", "0",
                    "--maxmemoryBytes", "0",
                    "--ioThreads", "1",
                    "--noCleanup"
            ));
        }

        private int port() {
            return server.port();
        }

        @Override
        public void close() {
            server.close();
        }
    }

}
