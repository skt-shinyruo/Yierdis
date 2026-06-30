package yier.bubu.redis.app.bench.suite;

import yier.bubu.redis.app.bench.BenchWorkloadKind;
import yier.bubu.redis.app.bench.YierdisBenchServerArgs;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class RedisSuiteTestSupport {
    private RedisSuiteTestSupport() {
    }

    public static ScenarioDefinition scenario(String id, BenchWorkloadKind workload, int warmups, int repeats, boolean latency) {
        return new ScenarioDefinition(id, id, workload, 100, workload == BenchWorkloadKind.PING ? 0 : 256,
                1000, 8, 4, warmups, repeats, latency);
    }

    public static ScenarioPassResult cleanPass(String artifactLabel, ScenarioDefinition scenario) {
        return cleanPass(artifactLabel, SuiteArtifact.Kind.YIERDIS_JAR, scenario);
    }

    public static ScenarioPassResult cleanPass(String artifactLabel, SuiteArtifact.Kind artifactKind, ScenarioDefinition scenario) {
        List<SuiteMetric> metrics = new ArrayList<>();
        metrics.add(new SuiteMetric("qps", 1000.0));
        metrics.add(new SuiteMetric("errors", 0.0));
        if (scenario.latency()) {
            metrics.add(new SuiteMetric("p95_ms", 10.0));
            metrics.add(new SuiteMetric("p99_ms", 20.0));
        }
        return new ScenarioPassResult(
                artifactLabel,
                artifactKind,
                scenario,
                false,
                "",
                List.of(IterationResult.repeat(0, metrics)),
                ObservationSnapshot.empty(),
                ObservationSnapshot.empty(),
                null
        );
    }

    public static SuiteConfig redisCurrentOnlyConfig(Path reportDir, int portBase, int redisPort) throws Exception {
        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        return new SuiteConfig(
                SuiteProfileName.RELEASE,
                SuiteArtifact.yierdisJar("current", regularTempJar("current"), "head"),
                Optional.empty(),
                List.of(
                        SuiteArtifact.externalRedis("redis", "127.0.0.1", redisPort, "", "", 0),
                        SuiteArtifact.yierdisJar("current", regularTempJar("current"), "head")
                ),
                reportDir,
                "127.0.0.1",
                portBase,
                "java",
                "4g",
                "4g",
                "6g",
                serverArgs,
                true
        );
    }

    private static Path regularTempJar(String prefix) throws IOException {
        Path jar = Files.createTempFile(prefix, ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }

    public static final class RedisLikeObservationServer implements AutoCloseable {
        private static final byte[] PONG = "+PONG\r\n".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] INFO = bulk("# Server\nredis_version:7.2.0\n");
        private static final byte[] MEMORY_STATS = "*4\r\n$10\r\npeak.allocated\r\n:11\r\n$14\r\ntotal.allocated\r\n:7\r\n"
                .getBytes(StandardCharsets.US_ASCII);
        private static final byte[] STATS = "-ERR unknown command 'STATS'\r\n".getBytes(StandardCharsets.US_ASCII);

        private final ServerSocket serverSocket;
        private final CountDownLatch listening = new CountDownLatch(1);
        private final List<Socket> accepted = new ArrayList<>();
        private final List<String> commands = Collections.synchronizedList(new ArrayList<>());
        private volatile boolean closed;
        private Thread thread;

        private RedisLikeObservationServer(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        public static RedisLikeObservationServer start() throws IOException {
            RedisLikeObservationServer server = new RedisLikeObservationServer(new ServerSocket(0));
            server.thread = new Thread(server::acceptLoop, "redis-suite-test-server");
            server.thread.setDaemon(true);
            server.thread.start();
            return server;
        }

        public int port() {
            return serverSocket.getLocalPort();
        }

        public List<String> commands() {
            synchronized (commands) {
                return List.copyOf(commands);
            }
        }

        public boolean awaitListening() throws InterruptedException {
            return listening.await(1, TimeUnit.SECONDS);
        }

        public List<String> awaitCommands(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (System.nanoTime() < deadline) {
                synchronized (commands) {
                    if (commands.size() >= expected) {
                        return List.copyOf(commands);
                    }
                }
                Thread.sleep(10);
            }
            synchronized (commands) {
                return List.copyOf(commands);
            }
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
                    continue;
                }
                String normalized = normalizedCommand(command);
                synchronized (commands) {
                    commands.add(normalized);
                }
                out.write(responseFor(normalized));
                out.flush();
            }
        }

        private static String normalizedCommand(RespClientCodec.RespReply command) {
            List<RespClientCodec.RespReply> values = command.values();
            StringBuilder out = new StringBuilder();
            for (int i = 0; i < values.size(); i++) {
                byte[] bytes = values.get(i).bytes();
                String part = bytes == null ? "" : new String(bytes, StandardCharsets.US_ASCII);
                if (i > 0) {
                    out.append(' ');
                }
                out.append(part.toUpperCase(java.util.Locale.ROOT));
            }
            return out.toString();
        }

        private static byte[] responseFor(String command) {
            return switch (command) {
                case "PING" -> PONG;
                case "FLUSHDB" -> OK;
                case "INFO" -> INFO;
                case "MEMORY STATS" -> MEMORY_STATS;
                case "STATS" -> STATS;
                default -> OK;
            };
        }

        private static byte[] bulk(String value) {
            return ("$" + value.length() + "\r\n" + value + "\r\n").getBytes(StandardCharsets.US_ASCII);
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
