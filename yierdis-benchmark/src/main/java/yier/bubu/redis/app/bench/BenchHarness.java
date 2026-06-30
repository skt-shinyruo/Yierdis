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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
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
    private static final int WORKLOAD_READ_TIMEOUT_MILLIS = 5_000;
    private static final int WORKLOAD_CONNECT_TIMEOUT_MILLIS = 1_000;
    private static final byte[] CMD_SET = "SET".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_GET = "GET".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_EXPIRE = "EXPIRE".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_LPUSH = "LPUSH".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_HSET = "HSET".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_SADD = "SADD".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_ZADD = "ZADD".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_SCAN = "SCAN".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_COUNT = "COUNT".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_0 = "0".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_60 = "60".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CMD_100 = "100".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PING = "PING".getBytes(StandardCharsets.US_ASCII);
    private final ObservationClient observationClient = new ObservationClient();
    private final DenseHllPreparer denseHllPreparer;
    private final int workloadReadTimeoutMillis;
    private final Set<PreparedPass> preparedPasses = ConcurrentHashMap.newKeySet();
    private final Set<ExternalRedisEndpoint> externalRedisEndpoints = ConcurrentHashMap.newKeySet();

    public BenchHarness() {
        this(YierdisBench::prefillDenseHll, WORKLOAD_READ_TIMEOUT_MILLIS);
    }

    BenchHarness(DenseHllPreparer denseHllPreparer) {
        this(denseHllPreparer, WORKLOAD_READ_TIMEOUT_MILLIS);
    }

    BenchHarness(DenseHllPreparer denseHllPreparer, int workloadReadTimeoutMillis) {
        this.denseHllPreparer = Objects.requireNonNull(denseHllPreparer, "denseHllPreparer");
        if (workloadReadTimeoutMillis <= 0) {
            throw new IllegalArgumentException("workloadReadTimeoutMillis must be > 0");
        }
        this.workloadReadTimeoutMillis = workloadReadTimeoutMillis;
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

        if (artifact.kind() == SuiteArtifact.Kind.EXTERNAL_REDIS) {
            if (!waitReady(artifact, READY_TIMEOUT_MILLIS, READY_READ_TIMEOUT_MILLIS)) {
                throw new IllegalStateException("suite server not ready within "
                        + READY_TIMEOUT_MILLIS + " ms: " + artifact.host() + ":" + artifact.port());
            }
            externalRedisEndpoints.add(ExternalRedisEndpoint.from(artifact));
            prepareExternalRedisPass(artifact);
            return new SuiteHarness.RunningServer(artifact.label(), scenario.id(), artifact.port(), logFile, null);
        }

        YierdisBenchServerArgs serverArgs = config.baseServerArgs();
        scenario.applyServerOverrides(serverArgs);
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
        for (ExternalRedisEndpoint endpoint : externalRedisEndpoints) {
            if (endpoint.host().equals(host) && endpoint.port() == port) {
                return observationClient.capture(endpoint.toArtifact());
            }
        }
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
                workloadHost(server, config),
                server.port(),
                scenario.requests(),
                scenario.clients(),
                scenario.pipeline(),
                scenario.keyspace(),
                scenario.dataSize(),
                scenario.latency(),
                config.strictReplies(),
                workloadIsExternalRedis(server, config),
                workloadRedisUser(server, config),
                workloadRedisAuth(server, config),
                workloadRedisDb(server, config)
        );
        BenchWorkloadResult result = runWorkload(request);
        return new IterationResult(kind, index, result.toMetrics());
    }

    @Override
    public void stopServer(SuiteHarness.RunningServer server) {
        Objects.requireNonNull(server, "server");
        Object handle = server.handle();
        if (handle == null) {
            externalRedisEndpoints.removeIf(endpoint -> endpoint.port() == server.port());
            preparedPasses.removeIf(pass -> pass.matches(server));
            return;
        }
        if (!(handle instanceof YierdisBench.ServerProcess process)) {
            throw new IllegalArgumentException("unsupported suite server handle: " + handle.getClass().getName());
        }
        process.stop();
        preparedPasses.removeIf(pass -> pass.matches(server));
    }

    private static void prepareExternalRedisPass(SuiteArtifact artifact) {
        runAdminCommand(artifact, List.of(bytes("FLUSHDB")));
    }

    private static void runAdminCommand(SuiteArtifact artifact, List<byte[]> command) {
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(artifact.host(), artifact.port()), READY_CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(READY_READ_TIMEOUT_MILLIS);
            bootstrapRedisSession(socket.getOutputStream(), socket.getInputStream(),
                    artifact.authUser(), artifact.authPassword(), artifact.db());
            RespClientCodec.writeCommand(socket.getOutputStream(), command);
            RespClientCodec.readReply(socket.getInputStream(), RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
        } catch (IOException e) {
            throw new IllegalStateException("failed admin command for " + artifact.label()
                    + " at " + artifact.host() + ":" + artifact.port(), e);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    BenchWorkloadResult runWorkload(BenchWorkloadRequest request) throws InterruptedException {
        Objects.requireNonNull(request, "request");
        if (isExtendedWorkload(request.workload())) {
            return request.latency()
                    ? runExtendedLatency(request, workloadReadTimeoutMillis)
                    : runExtendedThroughput(request, workloadReadTimeoutMillis);
        }
        YierdisBench.Workload workload = mapWorkload(request.workload());
        if (request.latency()) {
            return runLatency(request, workload, workloadReadTimeoutMillis);
        }
        return runThroughput(request, workload, workloadReadTimeoutMillis);
    }

    static boolean isExtendedWorkload(BenchWorkloadKind workload) {
        return switch (Objects.requireNonNull(workload, "workload")) {
            case SET_GET, MAXMEMORY_EVICTION, TTL_EXPIRATION, LIST_LPUSH, HASH_HSET, SET_SADD, ZSET_ZADD, SCAN, MIXED_READ_WRITE ->
                    true;
            default -> false;
        };
    }

    static byte[] encodeExtendedCommandForTest(BenchWorkloadKind workload, int keyIndex, int opIndex, byte[] value) {
        return RespClientCodec.encodeCommand(extendedCommand(workload, keyIndex, opIndex, value));
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
            denseHllPreparer.prefill(workloadHost(server, config), server.port(), scenario.keyspace(), scenario.pipeline());
        }
    }

    static String workloadHost(SuiteHarness.RunningServer server, SuiteConfig config) {
        for (SuiteArtifact artifact : config.artifactsInRunOrder()) {
            if (artifact.label().equals(server.artifactLabel())) {
                return artifact.kind() == SuiteArtifact.Kind.EXTERNAL_REDIS ? artifact.host() : config.host();
            }
        }
        return config.host();
    }

    private static String workloadRedisUser(SuiteHarness.RunningServer server, SuiteConfig config) {
        SuiteArtifact artifact = workloadArtifact(server, config);
        return artifact == null ? "" : artifact.authUser();
    }

    private static String workloadRedisAuth(SuiteHarness.RunningServer server, SuiteConfig config) {
        SuiteArtifact artifact = workloadArtifact(server, config);
        return artifact == null ? "" : artifact.authPassword();
    }

    private static int workloadRedisDb(SuiteHarness.RunningServer server, SuiteConfig config) {
        SuiteArtifact artifact = workloadArtifact(server, config);
        return artifact == null ? 0 : artifact.db();
    }

    private static boolean workloadIsExternalRedis(SuiteHarness.RunningServer server, SuiteConfig config) {
        SuiteArtifact artifact = workloadArtifact(server, config);
        return artifact != null && artifact.kind() == SuiteArtifact.Kind.EXTERNAL_REDIS;
    }

    private static SuiteArtifact workloadArtifact(SuiteHarness.RunningServer server, SuiteConfig config) {
        for (SuiteArtifact artifact : config.artifactsInRunOrder()) {
            if (artifact.label().equals(server.artifactLabel())) {
                return artifact;
            }
        }
        return null;
    }

    static boolean waitReady(String host, int port, int timeoutMillis, int readTimeoutMillis) throws InterruptedException {
        return waitReady(SuiteArtifact.externalRedis("redis", host, port, "", "", 0), timeoutMillis, readTimeoutMillis);
    }

    static boolean waitReady(SuiteArtifact artifact, int timeoutMillis, int readTimeoutMillis) throws InterruptedException {
        Objects.requireNonNull(artifact, "artifact");
        if (artifact.kind() != SuiteArtifact.Kind.EXTERNAL_REDIS) {
            throw new IllegalArgumentException("waitReady artifact must be EXTERNAL_REDIS");
        }
        String host = artifact.host();
        int port = artifact.port();
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
            if (pingOnce(artifact, attemptConnectTimeout, attemptReadTimeout)) {
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

    private static boolean pingOnce(SuiteArtifact artifact, int connectTimeoutMillis, int readTimeoutMillis) {
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(artifact.host(), artifact.port()), connectTimeoutMillis);
            socket.setSoTimeout(readTimeoutMillis);
            bootstrapRedisSession(socket.getOutputStream(), socket.getInputStream(),
                    artifact.authUser(), artifact.authPassword(), artifact.db());
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
            YierdisBench.Workload workload,
            int readTimeoutMillis
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
                        request.strictReplies(),
                        request.externalRedis(),
                        readTimeoutMillis,
                        request.redisUser(),
                        request.redisAuth(),
                        request.redisDb()
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
            YierdisBench.Workload workload,
            int readTimeoutMillis
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
                        request.strictReplies(),
                        request.externalRedis(),
                        readTimeoutMillis,
                        request.redisUser(),
                        request.redisAuth(),
                        request.redisDb()
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

    private static BenchWorkloadResult runExtendedThroughput(BenchWorkloadRequest request, int readTimeoutMillis) throws InterruptedException {
        ExtendedOutcome outcome = runExtended(request, readTimeoutMillis, false);
        double seconds = elapsedSeconds(outcome.startNs());
        double qps = seconds == 0.0 ? 0.0 : outcome.ops() / seconds;
        return new BenchWorkloadResult(outcome.ops(), outcome.errors(), seconds, qps, Double.NaN, Double.NaN, Double.NaN);
    }

    private static BenchWorkloadResult runExtendedLatency(BenchWorkloadRequest request, int readTimeoutMillis) throws InterruptedException {
        ExtendedOutcome outcome = runExtended(request, readTimeoutMillis, true);
        long[] all = outcome.samples();
        Arrays.sort(all);
        YierdisBench.LatencyStats stats = YierdisBench.LatencyStats.ofSortedNanos(all);
        double seconds = elapsedSeconds(outcome.startNs());
        double qps = seconds == 0.0 ? 0.0 : all.length / seconds;
        return new BenchWorkloadResult(outcome.ops(), outcome.errors(), seconds, qps, stats.p50Millis(), stats.p95Millis(), stats.p99Millis());
    }

    private static ExtendedOutcome runExtended(BenchWorkloadRequest request, int readTimeoutMillis, boolean latency) throws InterruptedException {
        byte[] value = new byte[request.dataSize()];
        Arrays.fill(value, (byte) 'x');

        if (request.workload() == BenchWorkloadKind.SET_GET
                || request.workload() == BenchWorkloadKind.TTL_EXPIRATION
                || request.workload() == BenchWorkloadKind.MIXED_READ_WRITE) {
            if (!prefillExtendedKeys(request, readTimeoutMillis, value)) {
                return new ExtendedOutcome(0, 1, new long[0], System.nanoTime());
            }
        }

        int perClient = request.requests() / request.clients();
        int remainder = request.requests() % request.clients();
        ExecutorService pool = Executors.newFixedThreadPool(request.clients());
        List<Future<ExtendedClientResult>> futures = new ArrayList<>(request.clients());
        int startOp = 0;

        long startNs = System.nanoTime();
        try {
            for (int i = 0; i < request.clients(); i++) {
                int n = perClient + (i < remainder ? 1 : 0);
                int clientStartOp = startOp;
                startOp += n;
                futures.add(pool.submit(() -> runExtendedClient(
                        request.host(),
                        request.port(),
                        request.workload(),
                        n,
                        clientStartOp,
                        request.pipeline(),
                        request.keyspace(),
                        value,
                        request.strictReplies(),
                        request.dataSize(),
                        readTimeoutMillis,
                        latency,
                        request.externalRedis(),
                        request.redisUser(),
                        request.redisAuth(),
                        request.redisDb()
                )));
            }
            waitForPool(pool);

            long ops = 0;
            long errors = 0;
            List<long[]> samples = latency ? new ArrayList<>(futures.size()) : List.of();
            int totalSamples = 0;
            for (Future<ExtendedClientResult> future : futures) {
                try {
                    ExtendedClientResult result = future.get();
                    ops += result.ops();
                    errors += result.errors();
                    if (latency) {
                        samples.add(result.samples());
                        totalSamples += result.samples().length;
                    }
                } catch (ExecutionException e) {
                    throw new IllegalStateException("suite extended workload worker failed", e.getCause());
                }
            }

            long[] all = new long[totalSamples];
            if (latency) {
                int offset = 0;
                for (long[] sample : samples) {
                    System.arraycopy(sample, 0, all, offset, sample.length);
                    offset += sample.length;
                }
            }
            return new ExtendedOutcome(ops, errors, latency ? all : new long[0], startNs);
        } finally {
            pool.shutdownNow();
        }
    }

    private static ExtendedClientResult runExtendedClient(
            String host,
            int port,
            BenchWorkloadKind workload,
            int requests,
            int startOpIndex,
            int pipeline,
            int keyspace,
            byte[] value,
            boolean strictReplies,
            int expectedDataSize,
            int readTimeoutMillis,
            boolean latency,
            boolean externalRedis,
            String redisUser,
            String redisAuth,
            int redisDb
    ) {
        if (requests <= 0) {
            return new ExtendedClientResult(0, 0, new long[0]);
        }

        long ops = 0;
        long errors = 0;
        long[] samples = latency ? new long[requests] : new long[0];
        int recorded = 0;

        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), WORKLOAD_CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(readTimeoutMillis);
            try (BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
                 BufferedInputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024)) {
                if (externalRedis) {
                    bootstrapRedisSession(out, in, redisUser, redisAuth, redisDb);
                }
                int remaining = requests;
                while (remaining > 0) {
                    int batch = Math.min(pipeline, remaining);
                    long[] batchStart = latency ? new long[batch] : null;
                    for (int i = 0; i < batch; i++) {
                        int opIndex = startOpIndex + (int) ops + i;
                        int keyIndex = Math.floorMod(opIndex, keyspace);
                        if (latency) {
                            batchStart[i] = System.nanoTime();
                        }
                        RespClientCodec.writeCommand(out, extendedCommand(workload, keyIndex, opIndex, value));
                    }
                    out.flush();
                    for (int i = 0; i < batch; i++) {
                        int opIndex = startOpIndex + (int) ops + i;
                        RespClientCodec.RespReply reply = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
                        if (reply.kind() == RespClientCodec.RespReply.Kind.ERROR) {
                            errors++;
                        } else if (strictReplies && !validateExtendedReply(workload, opIndex, expectedDataSize, reply)) {
                            errors++;
                        }
                        if (latency) {
                            samples[recorded++] = System.nanoTime() - batchStart[i];
                        }
                    }
                    ops += batch;
                    remaining -= batch;
                }
            }
        } catch (Exception e) {
            errors++;
            if (latency && recorded < samples.length) {
                samples = Arrays.copyOf(samples, recorded);
            }
            return new ExtendedClientResult(ops, errors, samples);
        }

        if (latency && recorded < samples.length) {
            samples = Arrays.copyOf(samples, recorded);
        }
        return new ExtendedClientResult(ops, errors, samples);
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
            case APPEND, NATIVE_DEFRAG_APPEND -> YierdisBench.Workload.APPEND;
            case HLL_SPARSE -> YierdisBench.Workload.PFADD_SPARSE;
            case HLL_DENSE -> YierdisBench.Workload.PFADD_DENSE;
            case HLL_PFCOUNT -> YierdisBench.Workload.PFCOUNT;
            case SET_GET, MAXMEMORY_EVICTION, TTL_EXPIRATION, LIST_LPUSH, HASH_HSET, SET_SADD, ZSET_ZADD, SCAN, MIXED_READ_WRITE ->
                    throw new IllegalStateException("extended workload was not routed before core mapping: " + workload);
        };
    }

    private static boolean prefillExtendedKeys(BenchWorkloadRequest request, int readTimeoutMillis, byte[] value) {
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(request.host(), request.port()), WORKLOAD_CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(readTimeoutMillis);
            try (BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
                 BufferedInputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024)) {
                if (request.externalRedis()) {
                    bootstrapRedisSession(out, in, request.redisUser(), request.redisAuth(), request.redisDb());
                }
                int remaining = request.keyspace();
                int keyIndex = 0;
                while (remaining > 0) {
                    int batch = Math.min(request.pipeline(), remaining);
                    for (int i = 0; i < batch; i++) {
                        RespClientCodec.writeCommand(out, List.of(
                                CMD_SET,
                                extendedKey(request.workload(), keyIndex++),
                                value
                        ));
                    }
                    out.flush();
                    for (int i = 0; i < batch; i++) {
                        RespClientCodec.RespReply reply = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
                        if (!reply.isSimpleString("OK")) {
                            return false;
                        }
                    }
                    remaining -= batch;
                }
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

    private static List<byte[]> extendedCommand(BenchWorkloadKind workload, int keyIndex, int opIndex, byte[] value) {
        byte[] key = extendedKey(workload, keyIndex);
        return switch (Objects.requireNonNull(workload, "workload")) {
            case SET_GET -> opIndex % 2 == 0
                    ? List.of(CMD_SET, key, value)
                    : List.of(CMD_GET, key);
            case MAXMEMORY_EVICTION -> List.of(CMD_SET, key, value);
            case TTL_EXPIRATION -> List.of(CMD_EXPIRE, key, CMD_60);
            case LIST_LPUSH -> List.of(CMD_LPUSH, key, value);
            case HASH_HSET -> List.of(CMD_HSET, key, ascii("field:" + keyIndex + ':' + opIndex), value);
            case SET_SADD -> List.of(CMD_SADD, key, ascii("member:" + keyIndex + ':' + opIndex));
            case ZSET_ZADD -> List.of(CMD_ZADD, key, ascii(Integer.toString(opIndex)), ascii("member:" + keyIndex + ':' + opIndex));
            case SCAN -> List.of(CMD_SCAN, CMD_0, CMD_COUNT, CMD_100);
            case MIXED_READ_WRITE -> opIndex % 5 == 0
                    ? List.of(CMD_SET, key, value)
                    : List.of(CMD_GET, key);
            default -> throw new IllegalArgumentException("not an extended suite workload: " + workload);
        };
    }

    private static boolean validateExtendedReply(BenchWorkloadKind workload, int opIndex, int expectedDataSize, RespClientCodec.RespReply reply) {
        return switch (workload) {
            case SET_GET -> {
                if (opIndex % 2 == 0) {
                    yield reply.isSimpleString("OK");
                }
                yield reply.kind() == RespClientCodec.RespReply.Kind.BULK_STRING
                        && reply.bulkLength() == expectedDataSize;
            }
            case MAXMEMORY_EVICTION -> reply.isSimpleString("OK");
            case LIST_LPUSH ->
                    reply.kind() == RespClientCodec.RespReply.Kind.INTEGER
                            && reply.integer() != null
                            && reply.integer() >= 1L;
            case HASH_HSET, SET_SADD, ZSET_ZADD ->
                    reply.kind() == RespClientCodec.RespReply.Kind.INTEGER
                            && reply.integer() != null
                            && reply.integer() == 1L;
            case TTL_EXPIRATION ->
                    reply.kind() == RespClientCodec.RespReply.Kind.INTEGER
                            && reply.integer() != null
                            && reply.integer() == 1L;
            case SCAN -> isValidScanReply(reply);
            case MIXED_READ_WRITE -> {
                if (opIndex % 5 == 0) {
                    yield reply.isSimpleString("OK");
                }
                yield reply.kind() == RespClientCodec.RespReply.Kind.BULK_STRING
                        && reply.bulkLength() == expectedDataSize;
            }
            default -> true;
        };
    }

    private static boolean isValidScanReply(RespClientCodec.RespReply reply) {
        if (reply.kind() != RespClientCodec.RespReply.Kind.ARRAY
                || reply.values() == null
                || reply.values().size() != 2) {
            return false;
        }

        RespClientCodec.RespReply cursor = reply.values().get(0);
        RespClientCodec.RespReply keys = reply.values().get(1);
        boolean cursorString = cursor.kind() == RespClientCodec.RespReply.Kind.BULK_STRING
                || cursor.kind() == RespClientCodec.RespReply.Kind.SIMPLE_STRING;
        return cursorString && keys.kind() == RespClientCodec.RespReply.Kind.ARRAY;
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static void bootstrapRedisSession(
            java.io.OutputStream out,
            java.io.InputStream in,
            String redisUser,
            String redisAuth,
            int redisDb
    ) throws IOException {
        if (redisAuth != null && !redisAuth.isBlank()) {
            if (redisUser == null || redisUser.isBlank()) {
                RespClientCodec.writeCommand(out, List.of(bytes("AUTH"), bytes(redisAuth)));
            } else {
                RespClientCodec.writeCommand(out, List.of(bytes("AUTH"), bytes(redisUser), bytes(redisAuth)));
            }
            out.flush();
            RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
        }
        RespClientCodec.writeCommand(out, List.of(bytes("SELECT"), bytes(Integer.toString(redisDb))));
        out.flush();
        RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
    }

    private static byte[] extendedKey(BenchWorkloadKind workload, int keyIndex) {
        return ascii(workload.name() + ":key:" + keyIndex);
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

    private record ExternalRedisEndpoint(String host, int port, String authUser, String authPassword, int db) {
        private ExternalRedisEndpoint {
            Objects.requireNonNull(host, "host");
        }

        private static ExternalRedisEndpoint from(SuiteArtifact artifact) {
            return new ExternalRedisEndpoint(
                    artifact.host(),
                    artifact.port(),
                    artifact.authUser(),
                    artifact.authPassword(),
                    artifact.db()
            );
        }

        private SuiteArtifact toArtifact() {
            return SuiteArtifact.externalRedis("redis", host, port, authUser, authPassword, db);
        }
    }

    private record ExtendedOutcome(long ops, long errors, long[] samples, long startNs) {
    }

    private record ExtendedClientResult(long ops, long errors, long[] samples) {
    }
}
