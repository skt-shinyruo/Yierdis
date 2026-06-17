package yier.bubu.redis.app.bench;

import yier.bubu.redis.app.bench.suite.IterationResult;
import yier.bubu.redis.app.bench.suite.ObservationClient;
import yier.bubu.redis.app.bench.suite.ObservationSnapshot;
import yier.bubu.redis.app.bench.suite.ScenarioDefinition;
import yier.bubu.redis.app.bench.suite.SuiteArtifact;
import yier.bubu.redis.app.bench.suite.SuiteConfig;
import yier.bubu.redis.app.bench.suite.SuiteHarness;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class BenchHarness implements SuiteHarness {
    private static final int READY_TIMEOUT_MILLIS = 15_000;
    private static final int READY_CONNECT_TIMEOUT_MILLIS = 500;
    private static final int READY_READ_TIMEOUT_MILLIS = 500;
    private static final byte[] PING = "PING".getBytes(StandardCharsets.US_ASCII);
    private final ObservationClient observationClient = new ObservationClient();
    private final DenseHllPreparer denseHllPreparer;
    private final Set<PreparedPass> preparedPasses = ConcurrentHashMap.newKeySet();

    public BenchHarness() {
        this(YierdisBench::prefillDenseHll);
    }

    BenchHarness(DenseHllPreparer denseHllPreparer) {
        this.denseHllPreparer = Objects.requireNonNull(denseHllPreparer, "denseHllPreparer");
    }

    @Override
    public SuiteHarness.RunningServer startServer(
            SuiteArtifact artifact,
            ScenarioDefinition scenario,
            SuiteConfig config,
            int port,
            Path logFile
    ) throws Exception {
        Objects.requireNonNull(artifact, "artifact");
        Objects.requireNonNull(scenario, "scenario");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(logFile, "logFile");

        YierdisBenchServerArgs serverArgs = config.baseServerArgs();
        serverArgs.port = port;
        serverArgs.normalizeAndValidate();

        YierdisBench.ServerProcess server = new YierdisBench.ServerProcess(
                config.javaCmd(),
                artifact.jarPath(),
                config.xms(),
                config.xmx(),
                config.maxDirectMemory(),
                serverArgs,
                logFile
        );
        server.start();
        try {
            if (!waitReady(config.host(), port, READY_TIMEOUT_MILLIS, READY_READ_TIMEOUT_MILLIS)) {
                throw new IllegalStateException("suite server not ready within "
                        + READY_TIMEOUT_MILLIS + " ms: " + logFile);
            }
        } catch (Exception e) {
            server.stop();
            throw e;
        }
        return new SuiteHarness.RunningServer(artifact.label(), scenario.id(), port, logFile, server);
    }

    @Override
    public ObservationSnapshot captureObservation(String host, int port) {
        return observationClient.capture(host, port);
    }

    @Override
    public IterationResult runIteration(
            SuiteHarness.RunningServer server,
            ScenarioDefinition scenario,
            int index,
            IterationResult.Kind kind,
            SuiteConfig config
    ) throws Exception {
        Objects.requireNonNull(server, "server");
        Objects.requireNonNull(scenario, "scenario");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(config, "config");

        prepareScenario(server, scenario, config);
        BenchWorkloadRequest request = new BenchWorkloadRequest(
                scenario.workload(),
                config.host(),
                server.port(),
                scenario.requests(),
                scenario.clients(),
                scenario.pipeline(),
                scenario.keyspace(),
                scenario.dataSize(),
                scenario.latency(),
                config.strictReplies()
        );
        BenchWorkloadResult result = runWorkload(request);
        return new IterationResult(kind, index, result.toMetrics());
    }

    @Override
    public void stopServer(SuiteHarness.RunningServer server) {
        Objects.requireNonNull(server, "server");
        Object handle = server.handle();
        if (handle == null) {
            return;
        }
        if (!(handle instanceof YierdisBench.ServerProcess process)) {
            throw new IllegalArgumentException("unsupported suite server handle: " + handle.getClass().getName());
        }
        process.stop();
        preparedPasses.removeIf(pass -> pass.matches(server));
    }

    BenchWorkloadResult runWorkload(BenchWorkloadRequest request) throws InterruptedException {
        Objects.requireNonNull(request, "request");
        YierdisBench.Workload workload = mapWorkload(request.workload());
        if (request.latency()) {
            return runLatency(request, workload);
        }
        return runThroughput(request, workload);
    }

    void prepareScenario(SuiteHarness.RunningServer server, ScenarioDefinition scenario, SuiteConfig config) {
        Objects.requireNonNull(server, "server");
        Objects.requireNonNull(scenario, "scenario");
        Objects.requireNonNull(config, "config");
        if (!requiresDenseHllPrefill(scenario.workload())) {
            return;
        }
        PreparedPass pass = PreparedPass.from(server);
        if (preparedPasses.add(pass)) {
            denseHllPreparer.prefill(config.host(), server.port(), scenario.keyspace(), scenario.pipeline());
        }
    }

    static boolean waitReady(String host, int port, int timeoutMillis, int readTimeoutMillis) throws InterruptedException {
        Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be in range 1..65535");
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be > 0");
        }
        if (readTimeoutMillis <= 0) {
            throw new IllegalArgumentException("readTimeoutMillis must be > 0");
        }

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remainingMillis <= 0) {
                break;
            }
            int attemptReadTimeout = (int) Math.max(1, Math.min(readTimeoutMillis, remainingMillis));
            int attemptConnectTimeout = (int) Math.max(1, Math.min(READY_CONNECT_TIMEOUT_MILLIS, remainingMillis));
            if (pingOnce(host, port, attemptConnectTimeout, attemptReadTimeout)) {
                return true;
            }
            remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remainingMillis > 0) {
                // Retry until the bounded readiness deadline expires.
                Thread.sleep(Math.min(100, remainingMillis));
            }
        }
        return false;
    }

    private static boolean pingOnce(String host, int port, int connectTimeoutMillis, int readTimeoutMillis) {
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            socket.setSoTimeout(readTimeoutMillis);
            RespClientCodec.writeCommand(socket.getOutputStream(), List.of(PING));
            RespClientCodec.RespReply reply = RespClientCodec.readReply(socket.getInputStream(),
                    RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            return reply.isSimpleString("PONG");
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    private static BenchWorkloadResult runThroughput(
            BenchWorkloadRequest request,
            YierdisBench.Workload workload
    ) throws InterruptedException {
        byte[] value = new byte[request.dataSize()];
        Arrays.fill(value, (byte) 'x');

        int perClient = request.requests() / request.clients();
        int remainder = request.requests() % request.clients();
        ExecutorService pool = Executors.newFixedThreadPool(request.clients());
        List<Future<YierdisBench.WorkerCounter>> futures = new ArrayList<>(request.clients());

        long startNs = System.nanoTime();
        try {
            for (int i = 0; i < request.clients(); i++) {
                int n = perClient + (i < remainder ? 1 : 0);
                futures.add(pool.submit(new YierdisBench.ThroughputWorker(
                        request.host(),
                        request.port(),
                        workload,
                        n,
                        request.pipeline(),
                        request.keyspace(),
                        value,
                        0,
                        request.strictReplies()
                )));
            }
            waitForPool(pool);
            long ops = 0;
            long errors = 0;
            for (Future<YierdisBench.WorkerCounter> future : futures) {
                try {
                    YierdisBench.WorkerCounter counter = future.get();
                    ops += counter.ops;
                    errors += counter.errors;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                } catch (ExecutionException e) {
                    throw new IllegalStateException("suite throughput worker failed", e.getCause());
                }
            }

            double seconds = elapsedSeconds(startNs);
            double qps = seconds == 0.0 ? 0.0 : ops / seconds;
            return new BenchWorkloadResult(ops, errors, seconds, qps, Double.NaN, Double.NaN, Double.NaN);
        } finally {
            pool.shutdownNow();
        }
    }

    private static BenchWorkloadResult runLatency(
            BenchWorkloadRequest request,
            YierdisBench.Workload workload
    ) throws InterruptedException {
        byte[] value = new byte[request.dataSize()];
        Arrays.fill(value, (byte) 'x');

        int perClient = request.requests() / request.clients();
        int remainder = request.requests() % request.clients();
        ExecutorService pool = Executors.newFixedThreadPool(request.clients());
        List<Future<YierdisBench.LatencySamples>> futures = new ArrayList<>(request.clients());

        long startNs = System.nanoTime();
        try {
            for (int i = 0; i < request.clients(); i++) {
                int n = perClient + (i < remainder ? 1 : 0);
                futures.add(pool.submit(new YierdisBench.LatencyWorker(
                        request.host(),
                        request.port(),
                        workload,
                        n,
                        request.keyspace(),
                        value,
                        request.strictReplies()
                )));
            }
            waitForPool(pool);

            int total = 0;
            long errors = 0;
            List<YierdisBench.LatencySamples> samples = new ArrayList<>(futures.size());
            for (Future<YierdisBench.LatencySamples> future : futures) {
                try {
                    YierdisBench.LatencySamples sample = future.get();
                    samples.add(sample);
                    total += sample.samples.length;
                    errors += sample.errors;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                } catch (ExecutionException e) {
                    throw new IllegalStateException("suite latency worker failed", e.getCause());
                }
            }

            long[] all = new long[total];
            int offset = 0;
            for (YierdisBench.LatencySamples sample : samples) {
                System.arraycopy(sample.samples, 0, all, offset, sample.samples.length);
                offset += sample.samples.length;
            }
            Arrays.sort(all);
            YierdisBench.LatencyStats stats = YierdisBench.LatencyStats.ofSortedNanos(all);
            double seconds = elapsedSeconds(startNs);
            double qps = seconds == 0.0 ? 0.0 : all.length / seconds;
            return new BenchWorkloadResult(
                    all.length,
                    errors,
                    seconds,
                    qps,
                    stats.p50Millis(),
                    stats.p95Millis(),
                    stats.p99Millis()
            );
        } finally {
            pool.shutdownNow();
        }
    }

    private static void waitForPool(ExecutorService pool) throws InterruptedException {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(1, TimeUnit.HOURS)) {
                throw new IllegalStateException("suite workload did not finish within 1 hour");
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private static double elapsedSeconds(long startNs) {
        return Math.max(Duration.ofNanos(System.nanoTime() - startNs).toNanos() / 1_000_000_000.0, 0.0);
    }

    private static YierdisBench.Workload mapWorkload(BenchWorkloadKind workload) {
        return switch (workload) {
            case PING -> YierdisBench.Workload.PING;
            case SET_GET -> YierdisBench.Workload.SET_RANDOM;
            case APPEND, NATIVE_DEFRAG_APPEND -> YierdisBench.Workload.APPEND;
            case HLL_SPARSE -> YierdisBench.Workload.PFADD_SPARSE;
            case HLL_DENSE -> YierdisBench.Workload.PFADD_DENSE;
            case HLL_PFCOUNT -> YierdisBench.Workload.PFCOUNT;
            case MAXMEMORY_EVICTION, TTL_EXPIRATION, LIST_LPUSH, HASH_HSET, SET_SADD, ZSET_ZADD, SCAN, MIXED_READ_WRITE ->
                    throw new IllegalArgumentException("unsupported extended suite workload: " + workload);
        };
    }

    private static boolean requiresDenseHllPrefill(BenchWorkloadKind workload) {
        return workload == BenchWorkloadKind.HLL_DENSE || workload == BenchWorkloadKind.HLL_PFCOUNT;
    }

    interface DenseHllPreparer {
        void prefill(String host, int port, int keyspace, int pipeline);
    }

    private record PreparedPass(String artifactLabel, String scenarioId, int port, Path logFile) {
        private static PreparedPass from(SuiteHarness.RunningServer server) {
            return new PreparedPass(server.artifactLabel(), server.scenarioId(), server.port(), server.logFile());
        }

        private boolean matches(SuiteHarness.RunningServer server) {
            return artifactLabel.equals(server.artifactLabel())
                    && scenarioId.equals(server.scenarioId())
                    && port == server.port()
                    && logFile.equals(server.logFile());
        }
    }
}
