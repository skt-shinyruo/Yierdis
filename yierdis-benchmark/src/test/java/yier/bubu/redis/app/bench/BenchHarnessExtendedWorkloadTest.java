package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BenchHarnessExtendedWorkloadTest {
    @Test
    public void commandForEachExtendedWorkloadUsesTheExpectedRedisCommand() {
        byte[] value = ascii("payload");

        assertFrameContains(BenchWorkloadKind.MAXMEMORY_EVICTION, 3, 7, value, "SET");
        assertFrameContains(BenchWorkloadKind.TTL_EXPIRATION, 3, 7, value, "EXPIRE");
        assertFrameContains(BenchWorkloadKind.LIST_LPUSH, 3, 7, value, "LPUSH");
        assertFrameContains(BenchWorkloadKind.HASH_HSET, 3, 7, value, "HSET");
        assertFrameContains(BenchWorkloadKind.SET_SADD, 3, 7, value, "SADD");
        assertFrameContains(BenchWorkloadKind.ZSET_ZADD, 3, 7, value, "ZADD");
        assertFrameContains(BenchWorkloadKind.SCAN, 3, 7, value, "SCAN");
        assertFrameContains(BenchWorkloadKind.MIXED_READ_WRITE, 3, 11, value, "GET");
        assertFrameContains(BenchWorkloadKind.MIXED_READ_WRITE, 3, 10, value, "SET");
    }

    @Test
    public void extendedWorkloadsAreRecognizedByHarness() {
        for (BenchWorkloadKind workload : List.of(
                BenchWorkloadKind.MAXMEMORY_EVICTION,
                BenchWorkloadKind.TTL_EXPIRATION,
                BenchWorkloadKind.LIST_LPUSH,
                BenchWorkloadKind.HASH_HSET,
                BenchWorkloadKind.SET_SADD,
                BenchWorkloadKind.ZSET_ZADD,
                BenchWorkloadKind.SCAN,
                BenchWorkloadKind.MIXED_READ_WRITE
        )) {
            Assert.assertTrue(workload.name(), BenchHarness.isExtendedWorkload(workload));
        }

        Assert.assertFalse(BenchHarness.isExtendedWorkload(BenchWorkloadKind.PING));
        Assert.assertFalse(BenchHarness.isExtendedWorkload(BenchWorkloadKind.SET_GET));
    }

    @Test
    public void extendedLatencyWorkloadReturnsLatencyMetricsAndDoesNotThrowUnsupported() throws Exception {
        try (OkRespServer server = OkRespServer.start()) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.HASH_HSET,
                    "127.0.0.1",
                    server.port(),
                    6,
                    1,
                    3,
                    16,
                    8,
                    true,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);
            Map<String, Double> metrics = metricsByName(result);

            Assert.assertEquals(6, result.ops());
            Assert.assertEquals(0, result.errors());
            Assert.assertTrue(metrics.containsKey("p95_ms"));
            Assert.assertTrue(metrics.containsKey("p99_ms"));
            Assert.assertEquals(6, server.awaitCommands(6));
        }
    }

    private static void assertFrameContains(
            BenchWorkloadKind workload,
            int keyIndex,
            int opIndex,
            byte[] value,
            String command
    ) {
        String frame = new String(
                BenchHarness.encodeExtendedCommandForTest(workload, keyIndex, opIndex, value),
                StandardCharsets.US_ASCII
        );
        Assert.assertTrue(frame, frame.contains("\r\n" + command + "\r\n"));
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static Map<String, Double> metricsByName(BenchWorkloadResult result) {
        return result.toMetrics().stream()
                .collect(java.util.stream.Collectors.toMap(metric -> metric.name(), metric -> metric.value()));
    }

    private static final class NoopDenseHllPreparer implements BenchHarness.DenseHllPreparer {
        @Override
        public void prefill(String host, int port, int keyspace, int pipeline) {
        }
    }

    private static final class OkRespServer implements AutoCloseable {
        private static final byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] ONE = ":1\r\n".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] SCAN_REPLY = "*2\r\n$1\r\n0\r\n*0\r\n".getBytes(StandardCharsets.US_ASCII);

        private final ServerSocket serverSocket;
        private final CountDownLatch listening = new CountDownLatch(1);
        private final List<Socket> accepted = new ArrayList<>();
        private volatile boolean closed;
        private volatile int commands;
        private Thread thread;

        private OkRespServer(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        static OkRespServer start() throws IOException {
            OkRespServer server = new OkRespServer(new ServerSocket(0));
            server.thread = new Thread(server::acceptLoop, "bench-harness-ok-resp-server");
            server.thread.setDaemon(true);
            server.thread.start();
            return server;
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        boolean awaitListening() throws InterruptedException {
            return listening.await(1, TimeUnit.SECONDS);
        }

        int awaitCommands(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (System.nanoTime() < deadline) {
                if (commands >= expected) {
                    return commands;
                }
                Thread.sleep(10);
            }
            return commands;
        }

        private void acceptLoop() {
            listening.countDown();
            while (!closed) {
                try {
                    Socket socket = serverSocket.accept();
                    synchronized (accepted) {
                        accepted.add(socket);
                    }
                    handle(socket);
                } catch (IOException e) {
                    if (!closed && !isClientDisconnect(e)) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        }

        private void handle(Socket socket) throws IOException {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            while (!closed && !socket.isClosed()) {
                RespClientCodec.RespReply command = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
                if (command.kind() != RespClientCodec.RespReply.Kind.ARRAY || command.values().isEmpty()) {
                    out.write(OK);
                    out.flush();
                    continue;
                }
                commands++;
                String name = commandName(command);
                if ("SCAN".equals(name)) {
                    out.write(SCAN_REPLY);
                } else if ("GET".equals(name)) {
                    out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
                } else if ("EXPIRE".equals(name) || "LPUSH".equals(name) || "HSET".equals(name)
                        || "SADD".equals(name) || "ZADD".equals(name)) {
                    out.write(ONE);
                } else {
                    out.write(OK);
                }
                out.flush();
            }
        }

        private String commandName(RespClientCodec.RespReply command) {
            byte[] bytes = command.values().get(0).bytes();
            return bytes == null ? "" : new String(bytes, StandardCharsets.US_ASCII);
        }

        private boolean isClientDisconnect(IOException e) {
            return e.getMessage() != null && e.getMessage().contains("unexpected EOF");
        }

        @Override
        public void close() throws Exception {
            closed = true;
            serverSocket.close();
            synchronized (accepted) {
                for (Socket socket : accepted) {
                    socket.close();
                }
            }
            if (thread != null) {
                thread.join(1_000);
            }
        }
    }
}
