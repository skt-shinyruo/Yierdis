package yier.bubu.redis.app.bench;

import picocli.CommandLine;
import picocli.CommandLine.ParameterException;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.foreign.YierdisNativeObjectTable;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 纯 Java 压测工具：用于测量 Yierdis 在默认 FFM native memory 模式下的吞吐与延迟。
 * <p>
 * 设计原则：
 * - 不依赖 redis-benchmark 等系统工具
 * - 使用本地 TCP + Redis RESP，避免“进程内直连”偏离真实网络路径
 * - 以固定请求数为主，输出 QPS 与简单的延迟分位数
 */
public final class YierdisBench {
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT_BASE = 16378;
    private static final List<String> DEFAULT_BACKENDS = List.of("foreign");

    // 共享容器 + memory limit=16G + server 固定 -Xms4g -Xmx4g 的保守默认预算
    private static final String DEFAULT_XMS = "4g";
    private static final String DEFAULT_XMX = "4g";
    private static final String DEFAULT_MAX_DIRECT_MEMORY = "6g";

    private static final int DEFAULT_KEYSPACE = 1_000_000;
    private static final int DEFAULT_DATA_SIZE = 256;

    private static final int DEFAULT_REQUESTS = 1_000_000;
    private static final int DEFAULT_CLIENTS = 200;
    private static final int DEFAULT_PIPELINE = 16;

    private static final int DEFAULT_LATENCY_REQUESTS = 200_000;
    private static final int DEFAULT_LATENCY_CLIENTS = 50;
    private static final byte[] HLL_SPARSE_KEY_PREFIX = "hll:sparse:".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HLL_DENSE_KEY_PREFIX = "hll:dense:".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HLL_ELEMENT_PREFIX = "hll:elem:".getBytes(StandardCharsets.US_ASCII);
    private static final int HLL_FIXED_DIGITS = 8;

    private static final int CONNECT_TIMEOUT_MILLIS = 1000;
    private static final int READY_TIMEOUT_MILLIS = 15_000;
    private static final int DB_DEFRAG_COMPARE_KEYSPACE = 4096;
    private static final int DB_DEFRAG_COMPARE_REQUESTS = 8192;
    private static final int DB_DEFRAG_COMPARE_CLIENTS = 8;

    private static final DecimalFormat DF = new DecimalFormat("0.000");
    private static final int NATIVE_ALLOCATOR_MAX_SLOTS = 262_144;

    public static void main(String[] args) throws Exception {
        YierdisBenchArgs benchArgs = new YierdisBenchArgs();
        CommandLine cmd = new CommandLine(benchArgs);
        cmd.setUnmatchedArgumentsAllowed(true);
        try {
            cmd.parseArgs(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            cmd.usage(System.err);
            return;
        }
        if (benchArgs.help) {
            cmd.usage(System.out);
            return;
        }

        YierdisBenchServerArgs baseServerArgs = new YierdisBenchServerArgs();
        CommandLine serverCmd = new CommandLine(baseServerArgs);
        try {
            serverCmd.parseArgs(benchArgs.serverArgs.toArray(new String[0]));
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            serverCmd.usage(System.err);
            return;
        }
        if (baseServerArgs.help) {
            serverCmd.usage(System.out);
            return;
        }
        baseServerArgs.normalizeAndValidate();

        BenchConfig config;
        try {
            config = BenchConfig.from(benchArgs, baseServerArgs);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            cmd.usage(System.err);
            return;
        }
        if (config.nativeEval) {
            println("YierdisBench（native allocator eval）");
            println("模式: in-process（不启动 server）");
            int effectiveIterations = effectiveNativeEvalIterations(config.nativeEvalIterations);
            println("iterations: " + effectiveIterations);
            println("");
            printNativeEvalReport(runNativeEval(effectiveIterations));
            println("");
            println("完成。");
            return;
        }

        Path serverJar = null;
        if (!config.noStartServer) {
            serverJar = config.serverJar != null ? config.serverJar : findServerJar();
        }
        Path runDir = Files.createTempDirectory(Path.of(".").toAbsolutePath().normalize(), ".bench-java.");

        println("YierdisBench（纯 Java）");
        println("运行目录: " + runDir);
        if (serverJar != null) {
            println("serverJar: " + serverJar);
            println("");
            printBudgetHint(config);
            println("");
        } else {
            println("模式: connect-only（不启动内置 server）");
            println("target: " + config.host + ":" + config.portBase);
            println("");
        }

        List<BackendResult> results = new ArrayList<>();
        for (int i = 0; i < config.backends.size(); i++) {
            String backend = config.backends.get(i);
            int port = config.portBase + i;
            Path logFile = runDir.resolve("server-" + backend + ".log");

            println("============================================================");
            println("后端: " + backend + "  port=" + port);
            if (!config.noStartServer) {
                println("日志: " + logFile);
            }

            ServerProcess server = null;
            if (!config.noStartServer) {
                YierdisBenchServerArgs serverArgsForRun = config.baseServerArgs.copy();
                serverArgsForRun.port = port;
                serverArgsForRun.normalizeAndValidate();

                server = new ServerProcess(
                        config.javaCmd,
                        serverJar,
                        config.serverXms,
                        config.serverXmx,
                        config.serverMaxDirectMemory,
                        serverArgsForRun,
                        logFile
                );
            }

            BackendResult backendResult = new BackendResult(backend, port);
            Instant startedAt = Instant.now();
            try {
                if (server != null) {
                    server.start();
                    if (!waitReady(config.host, port, READY_TIMEOUT_MILLIS)) {
                        throw new IllegalStateException("服务未就绪，请检查日志: " + logFile);
                    }
                    println("服务就绪，启动耗时: " + Duration.between(startedAt, Instant.now()).toMillis() + " ms");
                }

                if (!config.skipPrefill) {
                    println("");
                    println("[1/3] 预置数据（SET keyspace=" + config.keyspace + "，dataSize=" + config.dataSize + "，pipeline=" + config.pipeline + "）");
                    ThroughputResult prefill = runThroughput(
                            config.host,
                            port,
                            Workload.SET_SEQUENTIAL,
                            config.keyspace,
                            config.clients,
                            config.pipeline,
                            config.keyspace,
                            config.dataSize,
                            config.strictReplies
                    );
                    println("预置完成: " + prefill);
                }

                println("");
                println("[2/3] 吞吐压测（requests=" + config.requests + "，clients=" + config.clients + "，pipeline=" + config.pipeline + "）");
                ThroughputResult setQps = runThroughput(
                        config.host,
                        port,
                        Workload.SET_RANDOM,
                        config.requests,
                        config.clients,
                        config.pipeline,
                        config.keyspace,
                        config.dataSize,
                        config.strictReplies
                );
                ThroughputResult getQps = runThroughput(
                        config.host,
                        port,
                        Workload.GET_RANDOM,
                        config.requests,
                        config.clients,
                        config.pipeline,
                        config.keyspace,
                        config.dataSize,
                        config.strictReplies
                );
                backendResult.setThroughput = setQps;
                backendResult.getThroughput = getQps;
                println("SET: " + setQps);
                println("GET: " + getQps);

                if (!config.skipLatency) {
                    println("");
                    println("[3/3] 延迟压测（pipeline=1，requests=" + config.latencyRequests + "，clients=" + config.latencyClients + "）");
                    LatencyResult pingLat = runLatency(
                            config.host,
                            port,
                            Workload.PING,
                            config.latencyRequests,
                            config.latencyClients,
                            config.keyspace,
                            config.dataSize,
                            config.strictReplies
                    );
                    LatencyResult setLat = runLatency(
                            config.host,
                            port,
                            Workload.SET_RANDOM,
                            config.latencyRequests,
                            config.latencyClients,
                            config.keyspace,
                            config.dataSize,
                            config.strictReplies
                    );
                    LatencyResult getLat = runLatency(
                            config.host,
                            port,
                            Workload.GET_RANDOM,
                            config.latencyRequests,
                            config.latencyClients,
                            config.keyspace,
                            config.dataSize,
                            config.strictReplies
                    );
                    backendResult.pingLatency = pingLat;
                    backendResult.setLatency = setLat;
                    backendResult.getLatency = getLat;
                    println("PING: " + pingLat);
                    println("SET : " + setLat);
                    println("GET : " + getLat);
                }

                println("");
                println("[APPEND] 追加写入压测");
                if (!config.skipLatency) {
                    LatencyResult appendLat = runLatency(
                            config.host,
                            port,
                            Workload.APPEND,
                            config.latencyRequests,
                            config.latencyClients,
                            config.keyspace,
                            config.dataSize,
                            config.strictReplies
                    );
                    backendResult.appendLatency = appendLat;
                    println("APPEND: " + appendLat);
                }
                ThroughputResult appendQps = runThroughput(
                        config.host,
                        port,
                        Workload.APPEND,
                        config.requests,
                        config.clients,
                        config.pipeline,
                        config.keyspace,
                        config.dataSize,
                        config.strictReplies
                );
                backendResult.appendThroughput = appendQps;
                println("APPEND throughput: " + appendQps);

                println("");
                println("[HLL] PFADD/PFCOUNT sparse/dense 压测");
                int hllDenseKeyspace = Math.max(1, Math.min(config.keyspace, Math.min(config.requests, 4096)));
                ThroughputResult pfaddSparseQps = runThroughput(
                        config.host,
                        port,
                        Workload.PFADD_SPARSE,
                        config.requests,
                        config.clients,
                        config.pipeline,
                        config.keyspace,
                        config.dataSize,
                        config.strictReplies
                );
                prefillDenseHll(
                        config.host,
                        port,
                        hllDenseKeyspace,
                        config.pipeline
                );
                ThroughputResult pfaddDenseQps = runThroughput(
                        config.host,
                        port,
                        Workload.PFADD_DENSE,
                        config.requests,
                        config.clients,
                        config.pipeline,
                        hllDenseKeyspace,
                        config.dataSize,
                        config.strictReplies
                );
                ThroughputResult pfcountQps = runThroughput(
                        config.host,
                        port,
                        Workload.PFCOUNT,
                        config.requests,
                        config.clients,
                        config.pipeline,
                        hllDenseKeyspace,
                        config.dataSize,
                        config.strictReplies
                );
                backendResult.pfaddSparseThroughput = pfaddSparseQps;
                backendResult.pfaddDenseThroughput = pfaddDenseQps;
                backendResult.pfcountThroughput = pfcountQps;
                println("PFADD sparse throughput: " + pfaddSparseQps);
                println("PFADD dense throughput : " + pfaddDenseQps);
                println("PFCOUNT throughput     : " + pfcountQps);
                if (!config.skipLatency) {
                    LatencyResult pfaddSparseLat = runLatency(
                            config.host,
                            port,
                            Workload.PFADD_SPARSE,
                            config.latencyRequests,
                            config.latencyClients,
                            config.keyspace,
                            config.dataSize,
                            config.strictReplies
                    );
                    LatencyResult pfaddDenseLat = runLatency(
                            config.host,
                            port,
                            Workload.PFADD_DENSE,
                            config.latencyRequests,
                            config.latencyClients,
                            hllDenseKeyspace,
                            config.dataSize,
                            config.strictReplies
                    );
                    backendResult.pfaddSparseLatency = pfaddSparseLat;
                    backendResult.pfaddDenseLatency = pfaddDenseLat;
                    println("PFADD sparse latency   : " + pfaddSparseLat);
                    println("PFADD dense latency    : " + pfaddDenseLat);
                }
            } finally {
                if (server != null) {
                    server.stop();
                }
            }

            results.add(backendResult);
        }

        DbDefragComparisonResult dbDefragComparison = null;
        if (!config.noStartServer && !config.skipNativeDefragCompare) {
            println("");
            println("[DB native defrag] disabled/enabled p99 对比");
            dbDefragComparison = runDbNativeDefragComparison(config, serverJar, runDir);
            printDbDefragComparison(dbDefragComparison);
        }

        println("");
        println("============================================================");
        println("汇总（吞吐越大越好；延迟越小越好）");
        printSummary(results, config.skipLatency);
        if (dbDefragComparison != null) {
            println("");
            printDbDefragComparison(dbDefragComparison);
        }
        println("");
        println("完成。");
    }

    private static void printBudgetHint(BenchConfig config) {
        println("预算提示（共享容器 + memory limit=16G 的保守默认值，可通过参数覆盖）");
        println("  server JVM : -Xms" + config.serverXms + " -Xmx" + config.serverXmx
                + " -XX:MaxDirectMemorySize=" + config.serverMaxDirectMemory);
        println("  maxmemory  : --maxmemoryBytes " + config.baseServerArgs.maxmemoryBytes
                + " --maxmemoryPolicy " + config.baseServerArgs.maxmemoryPolicy);
        println("  native mem : JDK 25 FFM（固定启用）；容器内请配合 -XX:MaxDirectMemorySize 做预算。");
        println("  提醒：容器 OOMKill 优先下调 maxDirectMemory / maxmemory，而不是只看 maxmemory。");
    }

    private static Path findServerJar() {
        Path target = Path.of("yierdis-server", "yierdis-server-main", "target");
        if (!Files.isDirectory(target)) {
            throw new IllegalStateException("未找到目录: " + target.toAbsolutePath());
        }
        try (var stream = Files.list(target)) {
            return stream
                    .filter(p -> p.getFileName().toString().startsWith("yierdis-") && p.getFileName().toString().endsWith(".jar"))
                    .filter(p -> !p.getFileName().toString().contains("original-"))
                    .max(Comparator.comparingLong(p -> p.toFile().lastModified()))
                    .orElseThrow(() -> new IllegalStateException("未找到 server jar，请先运行 mvn package"));
        } catch (IOException e) {
            throw new IllegalStateException("扫描 server jar 失败: " + target.toAbsolutePath(), e);
        }
    }

    private static Path requireRegularFile(Path path, String optionName) {
        Objects.requireNonNull(path, optionName);
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException(optionName + " 不存在: " + path.toAbsolutePath());
        }
        return path;
    }

    private static boolean waitReady(String host, int port, int timeoutMillis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            try (Socket s = new Socket()) {
                s.setTcpNoDelay(true);
                s.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
                try (BufferedOutputStream out = new BufferedOutputStream(s.getOutputStream());
                     BufferedInputStream in = new BufferedInputStream(s.getInputStream())) {
                    try (RespCommandWriter w = new RespCommandWriter(out)) {
                        w.writePing();
                        out.flush();
                        return RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES)
                                .isSimpleString("PONG");
                    }
                }
            } catch (Exception ignored) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    private static ThroughputResult runThroughput(
            String host,
            int port,
            Workload workload,
            int totalRequests,
            int clients,
            int pipeline,
            int keyspace,
            int dataSize,
            boolean strictReplies
    ) throws InterruptedException {
        if (totalRequests <= 0) {
            throw new IllegalArgumentException("totalRequests must be > 0");
        }
        if (clients <= 0) {
            throw new IllegalArgumentException("clients must be > 0");
        }
        if (pipeline <= 0) {
            throw new IllegalArgumentException("pipeline must be > 0");
        }
        if (keyspace <= 0) {
            throw new IllegalArgumentException("keyspace must be > 0");
        }
        if (dataSize < 0) {
            throw new IllegalArgumentException("dataSize must be >= 0");
        }

        byte[] value = new byte[dataSize];
        Arrays.fill(value, (byte) 'x');

        int perClient = totalRequests / clients;
        int remainder = totalRequests % clients;

        ExecutorService pool = Executors.newFixedThreadPool(clients);
        List<Future<WorkerCounter>> futures = new ArrayList<>(clients);

        Instant start = Instant.now();
        long startNs = System.nanoTime();
        for (int i = 0; i < clients; i++) {
            int n = perClient + (i < remainder ? 1 : 0);
            int startIndex = workload == Workload.SET_SEQUENTIAL ? (totalRequests * i) / clients : 0;
            futures.add(pool.submit(new ThroughputWorker(
                    host, port, workload, n, pipeline, keyspace, value, startIndex, strictReplies
            )));
        }
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);

        long ops = 0;
        long errors = 0;
        for (Future<WorkerCounter> f : futures) {
            try {
                WorkerCounter c = f.get();
                ops += c.ops;
                errors += c.errors;
            } catch (ExecutionException e) {
                throw new IllegalStateException("压测线程失败", e.getCause());
            }
        }

        long elapsedNs = System.nanoTime() - startNs;
        double seconds = elapsedNs / 1_000_000_000.0;
        double qps = ops / seconds;
        return new ThroughputResult(workload, ops, errors, seconds, qps, start);
    }

    private static LatencyResult runLatency(
            String host,
            int port,
            Workload workload,
            int totalRequests,
            int clients,
            int keyspace,
            int dataSize,
            boolean strictReplies
    ) throws InterruptedException {
        if (totalRequests <= 0) {
            throw new IllegalArgumentException("totalRequests must be > 0");
        }
        if (clients <= 0) {
            throw new IllegalArgumentException("clients must be > 0");
        }
        if (keyspace <= 0) {
            throw new IllegalArgumentException("keyspace must be > 0");
        }
        if (dataSize < 0) {
            throw new IllegalArgumentException("dataSize must be >= 0");
        }

        byte[] value = new byte[dataSize];
        Arrays.fill(value, (byte) 'x');

        int perClient = totalRequests / clients;
        int remainder = totalRequests % clients;

        ExecutorService pool = Executors.newFixedThreadPool(clients);
        List<Future<LatencySamples>> futures = new ArrayList<>(clients);

        long startNs = System.nanoTime();
        for (int i = 0; i < clients; i++) {
            int n = perClient + (i < remainder ? 1 : 0);
            futures.add(pool.submit(new LatencyWorker(
                    host, port, workload, n, keyspace, value, strictReplies
            )));
        }
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);

        int total = 0;
        long errors = 0;
        for (Future<LatencySamples> f : futures) {
            try {
                LatencySamples s = f.get();
                total += s.samples.length;
                errors += s.errors;
            } catch (ExecutionException e) {
                throw new IllegalStateException("延迟压测线程失败", e.getCause());
            }
        }

        long[] all = new long[total];
        int off = 0;
        for (Future<LatencySamples> f : futures) {
            try {
                LatencySamples s = f.get();
                System.arraycopy(s.samples, 0, all, off, s.samples.length);
                off += s.samples.length;
            } catch (ExecutionException e) {
                throw new IllegalStateException("延迟压测线程失败", e.getCause());
            }
        }

        long elapsedNs = System.nanoTime() - startNs;
        double seconds = elapsedNs / 1_000_000_000.0;
        Arrays.sort(all);

        LatencyStats stats = LatencyStats.ofSortedNanos(all);
        double qps = all.length / seconds;
        return new LatencyResult(workload, all.length, errors, seconds, qps, stats);
    }

    private static void prefillDenseHll(String host, int port, int keyspace, int pipeline) {
        if (keyspace <= 0) {
            throw new IllegalArgumentException("keyspace must be > 0");
        }
        if (pipeline <= 0) {
            throw new IllegalArgumentException("pipeline must be > 0");
        }
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
            try (BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
                 BufferedInputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024);
                 RespCommandWriter writer = new RespCommandWriter(out)) {
                byte[] sourceKey = hllKey("src", 0);
                byte[] denseKey = new byte[HLL_DENSE_KEY_PREFIX.length + HLL_FIXED_DIGITS];
                writer.writePfadd(sourceKey, hllElement("seed", 0, 0));
                out.flush();
                RespClientCodec.RespReply sourceReply = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
                if (isErrorReply(sourceReply)) {
                    throw new IllegalStateException("PFADD dense source prefill failed");
                }

                int remaining = keyspace;
                int index = 0;
                while (remaining > 0) {
                    int batch = Math.min(pipeline, remaining);
                    for (int i = 0; i < batch; i++) {
                        writeDenseHllKey(denseKey, index++);
                        writer.writePfmerge(denseKey, sourceKey);
                    }
                    out.flush();
                    for (int i = 0; i < batch; i++) {
                        RespClientCodec.RespReply reply = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
                        if (!reply.isSimpleString("OK")) {
                            throw new IllegalStateException("PFMERGE dense prefill failed");
                        }
                    }
                    remaining -= batch;
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("dense HLL prefill failed", e);
        }
    }

    private static DbDefragComparisonResult runDbNativeDefragComparison(BenchConfig config, Path serverJar, Path runDir) throws Exception {
        int keyspace = Math.max(1, Math.min(config.keyspace, DB_DEFRAG_COMPARE_KEYSPACE));
        int requests = Math.max(keyspace, Math.min(config.latencyRequests, DB_DEFRAG_COMPARE_REQUESTS));
        int clients = Math.max(1, Math.min(config.latencyClients, DB_DEFRAG_COMPARE_CLIENTS));
        int dataSize = Math.max(1, config.dataSize);
        int disabledPort = config.portBase + config.backends.size();
        int enabledPort = disabledPort + 1;

        LatencyResult disabled = runDbDefragPass(
                config,
                serverJar,
                runDir,
                false,
                disabledPort,
                keyspace,
                requests,
                clients,
                dataSize
        );
        LatencyResult enabled = runDbDefragPass(
                config,
                serverJar,
                runDir,
                true,
                enabledPort,
                keyspace,
                requests,
                clients,
                dataSize
        );

        double disabledP99 = disabled.stats.p99Millis();
        double enabledP99 = enabled.stats.p99Millis();
        double impact = disabledP99 <= 0.0 ? 0.0 : ((enabledP99 - disabledP99) * 100.0) / disabledP99;
        return new DbDefragComparisonResult(disabled, enabled, impact);
    }

    private static LatencyResult runDbDefragPass(
            BenchConfig config,
            Path serverJar,
            Path runDir,
            boolean nativeDefragEnabled,
            int port,
            int keyspace,
            int requests,
            int clients,
            int dataSize
    ) throws Exception {
        String label = nativeDefragEnabled ? "enabled" : "disabled";
        YierdisBenchServerArgs serverArgs = config.baseServerArgs.copy();
        serverArgs.port = port;
        serverArgs.cleanupIntervalMillis = 1L;
        serverArgs.noCleanup = false;
        serverArgs.nativeDefragEnabled = nativeDefragEnabled;
        serverArgs.normalizeAndValidate();

        Path logFile = runDir.resolve("server-native-defrag-" + label + ".log");
        ServerProcess server = new ServerProcess(
                config.javaCmd,
                serverJar,
                config.serverXms,
                config.serverXmx,
                config.serverMaxDirectMemory,
                serverArgs,
                logFile
        );
        try {
            server.start();
            if (!waitReady(config.host, port, READY_TIMEOUT_MILLIS)) {
                throw new IllegalStateException("native defrag compare server not ready: " + logFile);
            }
            ThroughputResult prefill = runThroughput(
                    config.host, port, Workload.SET_SEQUENTIAL, keyspace, clients, config.pipeline, keyspace, dataSize, config.strictReplies
            );
            ThroughputResult fragmented = runThroughput(
                    config.host, port, Workload.APPEND, keyspace, clients, config.pipeline, keyspace, dataSize, config.strictReplies
            );
            if (prefill.errors > 0 || fragmented.errors > 0) {
                throw new IllegalStateException("native defrag compare preparation failed for " + label);
            }
            return runLatency(config.host, port, Workload.APPEND, requests, clients, keyspace, dataSize, config.strictReplies);
        } finally {
            server.stop();
        }
    }

    static String renderDbDefragComparison(DbDefragComparisonResult result) {
        Objects.requireNonNull(result, "result");
        StringBuilder sb = new StringBuilder();
        String header = String.format("%16s | %16s | %10s | %13s",
                "disabled_p99_ms", "enabled_p99_ms", "impact_pct", "enabled_err");
        appendTableHeader(sb, header);
        sb.append(String.format("%16s | %16s | %10s | %13d",
                DF.format(result.disabled.stats.p99Millis()),
                DF.format(result.enabled.stats.p99Millis()),
                DF.format(result.impactPercent),
                result.enabled.errors)).append('\n');
        return sb.toString();
    }

    private static void printDbDefragComparison(DbDefragComparisonResult result) {
        println("[db-native-defrag] APPEND p99 impact");
        for (String line : renderDbDefragComparison(result).split("\n", -1)) {
            if (!line.isEmpty()) {
                println(line);
            }
        }
    }

    static String renderComparison(ComparisonResult result) {
        Objects.requireNonNull(result, "result");
        StringBuilder sb = new StringBuilder();

        sb.append("[comparison]\n");
        sb.append("status: ").append(result.comparable() ? "comparable" : "non-comparable").append('\n');
        sb.append("environment: ").append(result.environmentCaveat).append('\n');
        appendComparisonProvenance(sb, "baseline", result.baseline);
        appendComparisonProvenance(sb, "current", result.current);
        sb.append('\n');

        String header = result.skipLatency
                ? String.format(
                "%-8s | %-14s | %12s | %13s | %12s | %13s | %12s | %13s | %16s | %17s | %16s | %17s | %12s | %13s",
                "side", "status", "SET_QPS", "SET_delta_pct", "GET_QPS", "GET_delta_pct",
                "APPEND_QPS", "APPEND_delta", "PFADD_S_QPS", "PFADD_S_delta",
                "PFADD_D_QPS", "PFADD_D_delta", "PFCOUNT_QPS", "PFCOUNT_delta"
        )
                : String.format(
                "%-8s | %-14s | %12s | %13s | %12s | %13s | %14s | %14s | %14s | %14s | %14s | %14s | %12s | %13s | %14s | %14s | %16s | %17s | %16s | %17s | %12s | %13s | %18s | %18s | %18s | %18s",
                "side", "status", "SET_QPS", "SET_delta_pct", "GET_QPS", "GET_delta_pct",
                "PING_p95(ms)", "PING_delta_pct", "SET_p95(ms)", "SET_lat_delta", "GET_p95(ms)", "GET_lat_delta",
                "APPEND_QPS", "APPEND_delta", "APPEND_p95(ms)", "APPEND_lat_delta",
                "PFADD_S_QPS", "PFADD_S_delta", "PFADD_D_QPS", "PFADD_D_delta", "PFCOUNT_QPS", "PFCOUNT_delta",
                "PFADD_S_p95(ms)", "PFADD_S_lat_delta", "PFADD_D_p95(ms)", "PFADD_D_lat_delta"
        );
        appendTableHeader(sb, header);
        appendComparisonRow(sb, result.baseline, result.baseline, result.comparable(), result.skipLatency);
        appendComparisonRow(sb, result.current, result.baseline, result.comparable(), result.skipLatency);

        if (!result.comparable()) {
            sb.append("baseline status: ").append(result.baseline.statusLabel()).append('\n');
            sb.append("current status: ").append(result.current.statusLabel()).append('\n');
        }
        if (!result.baseline.comparable()) {
            sb.append("baseline failure: ").append(result.baseline.failureMessage).append('\n');
        }
        if (!result.current.comparable()) {
            sb.append("current failure: ").append(result.current.failureMessage).append('\n');
        }
        return sb.toString();
    }

    private static void appendComparisonProvenance(StringBuilder sb, String prefix, ComparisonSideResult side) {
        sb.append(prefix).append(" jar: ").append(side.jarPath.toAbsolutePath()).append('\n');
        sb.append(prefix).append(" command: ").append(String.join(" ", side.commandLine)).append('\n');
        sb.append(prefix).append(" commit: ").append(side.commitLabel).append('\n');
    }

    private static void appendComparisonRow(
            StringBuilder sb,
            ComparisonSideResult side,
            ComparisonSideResult baseline,
            boolean comparable,
            boolean skipLatency
    ) {
        BackendResult r = side.result;
        BackendResult b = baseline.result;
        String setQps = throughputQps(r.setThroughput);
        String getQps = throughputQps(r.getThroughput);
        String appendQps = throughputQps(r.appendThroughput);
        String pfaddSparseQps = throughputQps(r.pfaddSparseThroughput);
        String pfaddDenseQps = throughputQps(r.pfaddDenseThroughput);
        String pfcountQps = throughputQps(r.pfcountThroughput);

        boolean showDelta = comparable && side != baseline;
        String setDelta = showDelta ? deltaPct(b.setThroughput == null ? null : b.setThroughput.qps, r.setThroughput == null ? null : r.setThroughput.qps) : "-";
        String getDelta = showDelta ? deltaPct(b.getThroughput == null ? null : b.getThroughput.qps, r.getThroughput == null ? null : r.getThroughput.qps) : "-";
        String appendDelta = showDelta ? deltaPct(b.appendThroughput == null ? null : b.appendThroughput.qps, r.appendThroughput == null ? null : r.appendThroughput.qps) : "-";
        String pfaddSparseDelta = showDelta ? deltaPct(b.pfaddSparseThroughput == null ? null : b.pfaddSparseThroughput.qps, r.pfaddSparseThroughput == null ? null : r.pfaddSparseThroughput.qps) : "-";
        String pfaddDenseDelta = showDelta ? deltaPct(b.pfaddDenseThroughput == null ? null : b.pfaddDenseThroughput.qps, r.pfaddDenseThroughput == null ? null : r.pfaddDenseThroughput.qps) : "-";
        String pfcountDelta = showDelta ? deltaPct(b.pfcountThroughput == null ? null : b.pfcountThroughput.qps, r.pfcountThroughput == null ? null : r.pfcountThroughput.qps) : "-";

        if (skipLatency) {
            sb.append(String.format(
                    "%-8s | %-14s | %12s | %13s | %12s | %13s | %12s | %13s | %16s | %17s | %16s | %17s | %12s | %13s",
                    side.label, side.statusLabel(), setQps, comparisonDelta(setDelta, comparable, showDelta), getQps, comparisonDelta(getDelta, comparable, showDelta),
                    appendQps, comparisonDelta(appendDelta, comparable, showDelta), pfaddSparseQps, comparisonDelta(pfaddSparseDelta, comparable, showDelta),
                    pfaddDenseQps, comparisonDelta(pfaddDenseDelta, comparable, showDelta), pfcountQps, comparisonDelta(pfcountDelta, comparable, showDelta)
            )).append('\n');
            return;
        }

        String pingP95 = latencyP95(r.pingLatency);
        String setP95 = latencyP95(r.setLatency);
        String getP95 = latencyP95(r.getLatency);
        String appendP95 = latencyP95(r.appendLatency);
        String pfaddSparseP95 = latencyP95(r.pfaddSparseLatency);
        String pfaddDenseP95 = latencyP95(r.pfaddDenseLatency);
        String pingDelta = showDelta ? deltaPct(b.pingLatency == null ? null : b.pingLatency.stats.p95Millis(), r.pingLatency == null ? null : r.pingLatency.stats.p95Millis()) : "-";
        String setLatDelta = showDelta ? deltaPct(b.setLatency == null ? null : b.setLatency.stats.p95Millis(), r.setLatency == null ? null : r.setLatency.stats.p95Millis()) : "-";
        String getLatDelta = showDelta ? deltaPct(b.getLatency == null ? null : b.getLatency.stats.p95Millis(), r.getLatency == null ? null : r.getLatency.stats.p95Millis()) : "-";
        String appendLatDelta = showDelta ? deltaPct(b.appendLatency == null ? null : b.appendLatency.stats.p95Millis(), r.appendLatency == null ? null : r.appendLatency.stats.p95Millis()) : "-";
        String pfaddSparseLatDelta = showDelta ? deltaPct(b.pfaddSparseLatency == null ? null : b.pfaddSparseLatency.stats.p95Millis(), r.pfaddSparseLatency == null ? null : r.pfaddSparseLatency.stats.p95Millis()) : "-";
        String pfaddDenseLatDelta = showDelta ? deltaPct(b.pfaddDenseLatency == null ? null : b.pfaddDenseLatency.stats.p95Millis(), r.pfaddDenseLatency == null ? null : r.pfaddDenseLatency.stats.p95Millis()) : "-";

        sb.append(String.format(
                "%-8s | %-14s | %12s | %13s | %12s | %13s | %14s | %14s | %14s | %14s | %14s | %14s | %12s | %13s | %14s | %14s | %16s | %17s | %16s | %17s | %12s | %13s | %18s | %18s | %18s | %18s",
                side.label, side.statusLabel(), setQps, comparisonDelta(setDelta, comparable, showDelta), getQps, comparisonDelta(getDelta, comparable, showDelta),
                pingP95, comparisonDelta(pingDelta, comparable, showDelta), setP95, comparisonDelta(setLatDelta, comparable, showDelta), getP95, comparisonDelta(getLatDelta, comparable, showDelta),
                appendQps, comparisonDelta(appendDelta, comparable, showDelta), appendP95, comparisonDelta(appendLatDelta, comparable, showDelta),
                pfaddSparseQps, comparisonDelta(pfaddSparseDelta, comparable, showDelta), pfaddDenseQps, comparisonDelta(pfaddDenseDelta, comparable, showDelta), pfcountQps, comparisonDelta(pfcountDelta, comparable, showDelta),
                pfaddSparseP95, comparisonDelta(pfaddSparseLatDelta, comparable, showDelta), pfaddDenseP95, comparisonDelta(pfaddDenseLatDelta, comparable, showDelta)
        )).append('\n');
    }

    private static String throughputQps(ThroughputResult result) {
        return result == null ? "-" : DF.format(result.qps);
    }

    private static String latencyP95(LatencyResult result) {
        return result == null ? "-" : DF.format(result.stats.p95Millis());
    }

    private static String comparisonDelta(String delta, boolean comparable, boolean showDelta) {
        if (!comparable) {
            return "n/a";
        }
        return showDelta ? delta : "-";
    }

    private static String deltaPct(Double baseline, Double current) {
        if (baseline == null || current == null || baseline == 0.0) {
            return "n/a";
        }
        double pct = ((current - baseline) * 100.0) / baseline;
        return (pct >= 0.0 ? "+" : "") + DF.format(pct) + "%";
    }

    static String renderSummary(List<BackendResult> results, boolean skipLatency) {
        StringBuilder sb = new StringBuilder();
        String header = skipLatency
                ? String.format(
                "%-8s | %12s | %8s | %12s | %8s | %12s | %8s | %16s | %8s | %16s | %8s | %12s | %8s",
                "backend", "SET_QPS", "SET_ERR", "GET_QPS", "GET_ERR", "APPEND_QPS", "APPEND_ERR",
                "PFADD_S_QPS", "PFADD_S_E", "PFADD_D_QPS", "PFADD_D_E", "PFCOUNT_QPS", "PFCOUNT_E"
        )
                : String.format(
                "%-8s | %12s | %8s | %12s | %8s | %14s | %8s | %14s | %8s | %14s | %8s | %12s | %8s | %14s | %8s | %16s | %8s | %16s | %8s | %12s | %8s | %18s | %10s | %18s | %10s",
                "backend", "SET_QPS", "SET_ERR", "GET_QPS", "GET_ERR",
                "PING_p95(ms)", "PING_E", "SET_p95(ms)", "SET_E", "GET_p95(ms)", "GET_E",
                "APPEND_QPS", "APPEND_ERR", "APPEND_p95(ms)", "APPEND_E",
                "PFADD_S_QPS", "PFADD_S_E", "PFADD_D_QPS", "PFADD_D_E", "PFCOUNT_QPS", "PFCOUNT_E",
                "PFADD_S_p95(ms)", "PFADD_S_E2", "PFADD_D_p95(ms)", "PFADD_D_E2"
        );
        sb.append(header).append('\n');
        sb.append(repeat('-', header.length())).append('\n');

        for (BackendResult r : results) {
            String setQps = r.setThroughput == null ? "-" : DF.format(r.setThroughput.qps);
            String appendQps = r.appendThroughput == null ? "-" : DF.format(r.appendThroughput.qps);
            String getQps = r.getThroughput == null ? "-" : DF.format(r.getThroughput.qps);
            String pfaddSparseQps = r.pfaddSparseThroughput == null ? "-" : DF.format(r.pfaddSparseThroughput.qps);
            String pfaddDenseQps = r.pfaddDenseThroughput == null ? "-" : DF.format(r.pfaddDenseThroughput.qps);
            String pfcountQps = r.pfcountThroughput == null ? "-" : DF.format(r.pfcountThroughput.qps);
            String setErr = r.setThroughput == null ? "-" : Long.toString(r.setThroughput.errors);
            String appendErr = r.appendThroughput == null ? "-" : Long.toString(r.appendThroughput.errors);
            String getErr = r.getThroughput == null ? "-" : Long.toString(r.getThroughput.errors);
            String pfaddSparseErr = r.pfaddSparseThroughput == null ? "-" : Long.toString(r.pfaddSparseThroughput.errors);
            String pfaddDenseErr = r.pfaddDenseThroughput == null ? "-" : Long.toString(r.pfaddDenseThroughput.errors);
            String pfcountErr = r.pfcountThroughput == null ? "-" : Long.toString(r.pfcountThroughput.errors);
            if (skipLatency) {
                sb.append(String.format(
                                "%-8s | %12s | %8s | %12s | %8s | %12s | %8s | %16s | %8s | %16s | %8s | %12s | %8s",
                                r.backend, setQps, setErr, getQps, getErr, appendQps, appendErr,
                                pfaddSparseQps, pfaddSparseErr, pfaddDenseQps, pfaddDenseErr, pfcountQps, pfcountErr
                        ))
                        .append('\n');
                continue;
            }
            String pingP95 = r.pingLatency == null ? "-" : DF.format(r.pingLatency.stats.p95Millis());
            String setP95 = r.setLatency == null ? "-" : DF.format(r.setLatency.stats.p95Millis());
            String appendP95 = r.appendLatency == null ? "-" : DF.format(r.appendLatency.stats.p95Millis());
            String getP95 = r.getLatency == null ? "-" : DF.format(r.getLatency.stats.p95Millis());
            String pingErr = r.pingLatency == null ? "-" : Long.toString(r.pingLatency.errors);
            String setLatErr = r.setLatency == null ? "-" : Long.toString(r.setLatency.errors);
            String appendLatErr = r.appendLatency == null ? "-" : Long.toString(r.appendLatency.errors);
            String getLatErr = r.getLatency == null ? "-" : Long.toString(r.getLatency.errors);
            String pfaddSparseP95 = r.pfaddSparseLatency == null ? "-" : DF.format(r.pfaddSparseLatency.stats.p95Millis());
            String pfaddDenseP95 = r.pfaddDenseLatency == null ? "-" : DF.format(r.pfaddDenseLatency.stats.p95Millis());
            String pfaddSparseLatErr = r.pfaddSparseLatency == null ? "-" : Long.toString(r.pfaddSparseLatency.errors);
            String pfaddDenseLatErr = r.pfaddDenseLatency == null ? "-" : Long.toString(r.pfaddDenseLatency.errors);
            sb.append(String.format(
                    "%-8s | %12s | %8s | %12s | %8s | %14s | %8s | %14s | %8s | %14s | %8s | %12s | %8s | %14s | %8s | %16s | %8s | %16s | %8s | %12s | %8s | %18s | %10s | %18s | %10s",
                    r.backend, setQps, setErr, getQps, getErr,
                    pingP95, pingErr, setP95, setLatErr, getP95, getLatErr,
                    appendQps, appendErr, appendP95, appendLatErr,
                    pfaddSparseQps, pfaddSparseErr, pfaddDenseQps, pfaddDenseErr, pfcountQps, pfcountErr,
                    pfaddSparseP95, pfaddSparseLatErr, pfaddDenseP95, pfaddDenseLatErr
            )).append('\n');
        }
        return sb.toString();
    }

    private static void printSummary(List<BackendResult> results, boolean skipLatency) {
        for (String line : renderSummary(results, skipLatency).split("\n", -1)) {
            if (!line.isEmpty()) {
                println(line);
            }
        }
    }

    static NativeEvalReport runNativeEval(int iterations) {
        int n = effectiveNativeEvalIterations(iterations);
        return new NativeEvalReport(
                runNativeAllocateFree(n),
                runNativeResolve(n),
                runNativeRealloc(n),
                runNativePin(n),
                runNativeMetadata(),
                runNativeQuarantine(n),
                runNativeSmallObjectChurn(n),
                runNativeDefragImpact(n)
        );
    }

    static String renderNativeEvalReport(NativeEvalReport report) {
        Objects.requireNonNull(report, "report");
        StringBuilder sb = new StringBuilder();

        sb.append("[native-allocator] allocate/free\n");
        appendTableHeader(sb, String.format("%-9s | %8s | %10s | %12s | %11s",
                "class", "bytes", "ops", "alloc_us", "free_us"));
        for (NativeSizeClassResult r : report.sizeClasses) {
            sb.append(String.format("%-9s | %8d | %10d | %12s | %11s",
                    r.label, r.bytes, r.ops, DF.format(r.allocMicros), DF.format(r.freeMicros))).append('\n');
        }

        sb.append("\n[native-allocator] resolve/close\n");
        appendTableHeader(sb, String.format("%10s | %16s", "ops", "resolve_close_us"));
        sb.append(String.format("%10d | %16s", report.resolve.ops, DF.format(report.resolve.resolveCloseMicros))).append('\n');

        sb.append("\n[native-allocator] realloc\n");
        appendTableHeader(sb, String.format("%10s | %8s | %8s | %10s", "ops", "in_place", "moved", "avg_us"));
        sb.append(String.format("%10d | %8d | %8d | %10s",
                report.realloc.ops, report.realloc.inPlace, report.realloc.moved, DF.format(report.realloc.avgMicros))).append('\n');

        sb.append("\n[native-allocator] pin/unpin\n");
        appendTableHeader(sb, String.format("%10s | %13s", "ops", "pin_unpin_us"));
        sb.append(String.format("%10d | %13s", report.pin.ops, DF.format(report.pin.pinUnpinMicros))).append('\n');

        sb.append("\n[native-allocator] object-table metadata\n");
        appendTableHeader(sb, String.format("%-7s | %11s | %21s | %12s",
                "value", "value_bytes", "metadata_bytes_per_obj", "metadata_pct"));
        for (NativeMetadataResult r : report.metadata) {
            sb.append(String.format("%-7s | %11d | %21s | %12s",
                    r.label, r.valueBytes, DF.format(r.metadataBytesPerObject), DF.format(r.metadataPercent))).append('\n');
        }

        sb.append("\n[native-allocator] quarantine/epoch churn\n");
        appendTableHeader(sb, String.format("%14s | %19s", "retained_bytes", "retained_pct_reserved"));
        sb.append(String.format("%14d | %16s",
                report.quarantine.retainedBytes, DF.format(report.quarantine.retainedPercentOfReserved))).append('\n');

        sb.append("\n[native-allocator] small-object churn\n");
        appendTableHeader(sb, String.format("%10s | %14s | %10s", "ops", "avg_us_per_op", "p99_us"));
        sb.append(String.format("%10d | %14s | %10s",
                report.churn.ops, DF.format(report.churn.avgMicros), DF.format(report.churn.p99Micros))).append('\n');

        sb.append("\n[native-allocator] defrag p99 impact\n");
        appendTableHeader(sb, String.format("%15s | %14s | %10s | %13s",
                "disabled_p99_us", "enabled_p99_us", "impact_pct", "moved_objects"));
        sb.append(String.format("%15s | %14s | %10s | %13d",
                DF.format(report.defragImpact.disabledP99Micros),
                DF.format(report.defragImpact.enabledP99Micros),
                DF.format(report.defragImpact.impactPercent),
                report.defragImpact.movedObjects)).append('\n');
        return sb.toString();
    }

    private static void printNativeEvalReport(NativeEvalReport report) {
        for (String line : renderNativeEvalReport(report).split("\n", -1)) {
            if (!line.isEmpty()) {
                println(line);
            }
        }
    }

    private static void appendTableHeader(StringBuilder sb, String header) {
        sb.append(header).append('\n');
        sb.append(repeat('-', header.length())).append('\n');
    }

    private static List<NativeSizeClassResult> runNativeAllocateFree(int iterations) {
        int[] sizes = {64, 256, 4096, 65_536};
        String[] labels = {"small-64", "small-256", "medium-4k", "large-64k"};
        List<NativeSizeClassResult> results = new ArrayList<>(sizes.length);
        for (int i = 0; i < sizes.length; i++) {
            try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("bench-alloc-" + labels[i]);
                 NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, NATIVE_ALLOCATOR_MAX_SLOTS)) {
                long allocNanos = 0L;
                long freeNanos = 0L;
                for (int j = 0; j < iterations; j++) {
                    long t0 = System.nanoTime();
                    NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, sizes[i]);
                    allocNanos += System.nanoTime() - t0;
                    long t1 = System.nanoTime();
                    allocator.free(handle);
                    freeNanos += System.nanoTime() - t1;
                }
                results.add(new NativeSizeClassResult(
                        labels[i],
                        sizes[i],
                        iterations,
                        microsPerOp(allocNanos, iterations),
                        microsPerOp(freeNanos, iterations)
                ));
            }
        }
        return results;
    }

    private static NativeResolveResult runNativeResolve(int iterations) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("bench-resolve");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, NATIVE_ALLOCATOR_MAX_SLOTS)) {
            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 256);
            long nanos = 0L;
            for (int i = 0; i < iterations; i++) {
                long t0 = System.nanoTime();
                try (NativeObjectView ignored = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                    // close cost is part of the stable handle access path.
                }
                nanos += System.nanoTime() - t0;
            }
            allocator.free(handle);
            return new NativeResolveResult(iterations, microsPerOp(nanos, iterations));
        }
    }

    private static NativeReallocResult runNativeRealloc(int iterations) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("bench-realloc");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, NATIVE_ALLOCATOR_MAX_SLOTS)) {
            int inPlace = 0;
            int moved = 0;
            long nanos = 0L;
            for (int i = 0; i < iterations; i++) {
                NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 64);
                int targetSize = (i & 1) == 0 ? 32 : 4096;
                long beforeInPlace = allocator.stats().reallocInPlaceCount();
                long beforeMoved = allocator.stats().reallocMovedCount();
                long t0 = System.nanoTime();
                handle = allocator.realloc(handle, targetSize, NativeReallocPolicy.PRESERVE_PREFIX);
                nanos += System.nanoTime() - t0;
                NativeAllocatorStats stats = allocator.stats();
                inPlace += (int) (stats.reallocInPlaceCount() - beforeInPlace);
                moved += (int) (stats.reallocMovedCount() - beforeMoved);
                allocator.free(handle);
            }
            return new NativeReallocResult(iterations, inPlace, moved, microsPerOp(nanos, iterations));
        }
    }

    private static NativePinResult runNativePin(int iterations) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("bench-pin");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, NATIVE_ALLOCATOR_MAX_SLOTS)) {
            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 256);
            long nanos = 0L;
            for (int i = 0; i < iterations; i++) {
                long t0 = System.nanoTime();
                allocator.pin(handle);
                allocator.unpin(handle);
                nanos += System.nanoTime() - t0;
            }
            allocator.free(handle);
            return new NativePinResult(iterations, microsPerOp(nanos, iterations));
        }
    }

    private static List<NativeMetadataResult> runNativeMetadata() {
        return List.of(
                metadataResult("small", 64),
                metadataResult("medium", 4096)
        );
    }

    private static NativeMetadataResult metadataResult(String label, int valueBytes) {
        double metadataBytes = YierdisNativeObjectTable.META_BYTES;
        double pct = valueBytes <= 0 ? 0.0 : (metadataBytes * 100.0) / (metadataBytes + valueBytes);
        return new NativeMetadataResult(label, valueBytes, metadataBytes, pct);
    }

    private static NativeQuarantineResult runNativeQuarantine(int iterations) {
        int n = Math.max(1, Math.min(iterations, 4096));
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("bench-quarantine");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, NATIVE_ALLOCATOR_MAX_SLOTS)) {
            try (NativeEpochScope ignored = allocator.beginEpoch(NativeEpochKind.SCAN)) {
                for (int i = 0; i < n; i++) {
                    NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 64);
                    allocator.free(handle);
                }
                NativeAllocatorStats stats = allocator.stats();
                long reserved = Math.max(1L, stats.reservedBytes());
                return new NativeQuarantineResult(stats.quarantineBytes(), stats.quarantineBytes() * 100.0 / reserved);
            }
        }
    }

    private static NativeChurnResult runNativeSmallObjectChurn(int iterations) {
        int n = Math.max(1, iterations);
        long[] samples = new long[n];
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("bench-churn");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, NATIVE_ALLOCATOR_MAX_SLOTS)) {
            for (int i = 0; i < n; i++) {
                long t0 = System.nanoTime();
                NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 64);
                allocator.free(handle);
                samples[i] = System.nanoTime() - t0;
            }
        }
        long sum = 0L;
        for (long sample : samples) {
            sum += sample;
        }
        Arrays.sort(samples);
        return new NativeChurnResult(n, microsPerOp(sum, n), samples[Math.max(0, (int) Math.round(0.99 * (n - 1)))] / 1_000.0);
    }

    private static NativeDefragImpactResult runNativeDefragImpact(int iterations) {
        int n = Math.max(8, Math.min(iterations, 2048));
        long[] disabled = new long[n];
        long[] enabled = new long[n];
        long movedObjects = 0L;
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("bench-defrag");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, NATIVE_ALLOCATOR_MAX_SLOTS)) {
            NativeHandle[] handles = new NativeHandle[n];
            for (int i = 0; i < n; i++) {
                handles[i] = allocator.allocate(NativeObjectKind.STRING_BYTES, 128);
            }
            for (int i = 0; i < n; i++) {
                long t0 = System.nanoTime();
                try (NativeObjectView ignored = allocator.resolve(handles[i], NativeAccessMode.READ_ONLY)) {
                    // bounded synthetic baseline
                }
                disabled[i] = System.nanoTime() - t0;
            }
            for (int i = 0; i < n; i++) {
                long t0 = System.nanoTime();
                if ((i & 15) == 0) {
                    movedObjects += allocator.defragCycle(new NativeDefragOptions(4096, 4, TimeUnit.MILLISECONDS.toNanos(1))).movedObjects();
                }
                try (NativeObjectView ignored = allocator.resolve(handles[i], NativeAccessMode.READ_ONLY)) {
                    // includes intermittent defragCycle cost.
                }
                enabled[i] = System.nanoTime() - t0;
            }
        }
        Arrays.sort(disabled);
        Arrays.sort(enabled);
        double disabledP99 = percentileMicros(disabled, 0.99);
        double enabledP99 = percentileMicros(enabled, 0.99);
        double impact = disabledP99 <= 0.0 ? 0.0 : ((enabledP99 - disabledP99) * 100.0) / disabledP99;
        return new NativeDefragImpactResult(disabledP99, enabledP99, impact, movedObjects);
    }

    private static double percentileMicros(long[] sortedNanos, double percentile) {
        if (sortedNanos.length == 0) {
            return 0.0;
        }
        int index = Math.max(0, Math.min(sortedNanos.length - 1, (int) Math.round(percentile * (sortedNanos.length - 1))));
        return sortedNanos[index] / 1_000.0;
    }

    private static double microsPerOp(long nanos, long ops) {
        if (ops <= 0) {
            return 0.0;
        }
        return (nanos / 1_000.0) / ops;
    }

    private static String repeat(char c, int n) {
        if (n <= 0) {
            return "";
        }
        char[] out = new char[n];
        Arrays.fill(out, c);
        return new String(out);
    }

    private static void println(String s) {
        System.out.println(s);
    }

    static boolean validateStrictReply(Workload workload, InputStream in, int expectedDataSize) throws IOException {
        if (workload == null || in == null) {
            return false;
        }
        RespClientCodec.RespReply reply = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
        return validateStrictReply(workload, reply, expectedDataSize);
    }

    private static boolean validateStrictReply(Workload workload, RespClientCodec.RespReply reply, int expectedDataSize) {
        if (workload == null || reply == null) {
            return false;
        }
        switch (workload) {
            case PING:
                return reply.isSimpleString("PONG");
            case SET_RANDOM:
            case SET_SEQUENTIAL:
                return reply.isSimpleString("OK");
            case APPEND:
                return reply.kind() == RespClientCodec.RespReply.Kind.INTEGER
                        && reply.integer() != null
                        && reply.integer() >= 0L
                        && (expectedDataSize <= 0 || reply.integer() >= expectedDataSize);
            case PFADD_SPARSE:
            case PFADD_DENSE:
                return reply.kind() == RespClientCodec.RespReply.Kind.INTEGER
                        && reply.integer() != null
                        && reply.integer() >= 0L
                        && reply.integer() <= 1L;
            case PFCOUNT:
                return reply.kind() == RespClientCodec.RespReply.Kind.INTEGER
                        && reply.integer() != null
                        && reply.integer() >= 0L;
            case GET_RANDOM:
                if (reply.isNull()) {
                    return true;
                }
                if (reply.kind() != RespClientCodec.RespReply.Kind.BULK_STRING) {
                    return false;
                }
                return expectedDataSize < 0 || reply.bulkLength() == expectedDataSize;
            default:
                return true;
        }
    }

    private static boolean isErrorReply(RespClientCodec.RespReply reply) {
        if (reply == null) {
            return false;
        }
        return reply.kind() == RespClientCodec.RespReply.Kind.ERROR;
    }

    enum Workload {
        PING,
        SET_RANDOM,
        SET_SEQUENTIAL,
        APPEND,
        GET_RANDOM,
        PFADD_SPARSE,
        PFADD_DENSE,
        PFCOUNT
    }

    static final class BenchConfig {
        final boolean noStartServer;
        final Path serverJar;
        final boolean comparisonMode;
        final Path baselineServerJar;
        final Path currentServerJar;
        final List<String> backends;
        final String host;
        final int portBase;

        final String javaCmd;
        final String serverXms;
        final String serverXmx;
        final String serverMaxDirectMemory;
        final YierdisBenchServerArgs baseServerArgs;

        final int keyspace;
        final int dataSize;
        final int requests;
        final int clients;
        final int pipeline;
        final int latencyRequests;
        final int latencyClients;

        final boolean skipPrefill;
        final boolean skipLatency;
        final boolean strictReplies;
        final boolean skipNativeDefragCompare;
        final boolean nativeEval;
        final int nativeEvalIterations;

        private BenchConfig(
                boolean noStartServer,
                Path serverJar,
                boolean comparisonMode,
                Path baselineServerJar,
                Path currentServerJar,
                List<String> backends,
                String host,
                int portBase,
                String javaCmd,
                String serverXms,
                String serverXmx,
                String serverMaxDirectMemory,
                YierdisBenchServerArgs baseServerArgs,
                int keyspace,
                int dataSize,
                int requests,
                int clients,
                int pipeline,
                int latencyRequests,
                int latencyClients,
                boolean skipPrefill,
                boolean skipLatency,
                boolean strictReplies,
                boolean skipNativeDefragCompare,
                boolean nativeEval,
                int nativeEvalIterations
        ) {
            this.noStartServer = noStartServer;
            this.serverJar = serverJar;
            this.comparisonMode = comparisonMode;
            this.baselineServerJar = baselineServerJar;
            this.currentServerJar = currentServerJar;
            this.backends = backends;
            this.host = host;
            this.portBase = portBase;
            this.javaCmd = javaCmd;
            this.serverXms = serverXms;
            this.serverXmx = serverXmx;
            this.serverMaxDirectMemory = serverMaxDirectMemory;
            this.baseServerArgs = baseServerArgs;
            this.keyspace = keyspace;
            this.dataSize = dataSize;
            this.requests = requests;
            this.clients = clients;
            this.pipeline = pipeline;
            this.latencyRequests = latencyRequests;
            this.latencyClients = latencyClients;
            this.skipPrefill = skipPrefill;
            this.skipLatency = skipLatency;
            this.strictReplies = strictReplies;
            this.skipNativeDefragCompare = skipNativeDefragCompare;
            this.nativeEval = nativeEval;
            this.nativeEvalIterations = nativeEvalIterations;
        }

        static BenchConfig from(YierdisBenchArgs args, YierdisBenchServerArgs baseServerArgs) {
            Objects.requireNonNull(args, "args");
            Objects.requireNonNull(baseServerArgs, "baseServerArgs");

            Path serverJar = args.serverJar;
            Path baselineServerJar = args.baselineServerJar;
            Path currentServerJar = args.currentServerJar;

            if (args.comparisonMode) {
                if (serverJar != null) {
                    throw new IllegalArgumentException("comparisonMode 不支持 serverJar");
                }
                if (args.noStartServer) {
                    throw new IllegalArgumentException("comparisonMode 不支持 noStartServer");
                }
                if (baselineServerJar == null) {
                    throw new IllegalArgumentException("comparisonMode 需要 baselineServerJar");
                }
                if (currentServerJar == null) {
                    throw new IllegalArgumentException("comparisonMode 需要 currentServerJar");
                }
                baselineServerJar = requireRegularFile(baselineServerJar, "baselineServerJar");
                currentServerJar = requireRegularFile(currentServerJar, "currentServerJar");
            } else if (serverJar != null) {
                serverJar = requireRegularFile(serverJar, "serverJar");
            }
            if (args.nativeEval) {
                effectiveNativeEvalIterations(args.nativeEvalIterations);
            }

            List<String> backends = args.comparisonMode
                    ? List.of("baseline", "current")
                    : (args.noStartServer ? List.of("external") : DEFAULT_BACKENDS);

            return new BenchConfig(
                    args.noStartServer,
                    serverJar,
                    args.comparisonMode,
                    baselineServerJar,
                    currentServerJar,
                    backends,
                    args.host,
                    args.portBase,
                    args.javaCmd,
                    args.xms,
                    args.xmx,
                    args.maxDirectMemory,
                    baseServerArgs,
                    args.keyspace,
                    args.dataSize,
                    args.requests,
                    args.clients,
                    args.pipeline,
                    args.latencyRequests,
                    args.latencyClients,
                    args.skipPrefill,
                    args.skipLatency,
                    args.strictReplies,
                    args.comparisonMode || args.skipNativeDefragCompare,
                    args.nativeEval,
                    args.nativeEvalIterations
            );
        }
    }

    static final class ComparisonResult {
        final ComparisonSideResult baseline;
        final ComparisonSideResult current;
        final boolean skipLatency;
        final String environmentCaveat;

        ComparisonResult(
                ComparisonSideResult baseline,
                ComparisonSideResult current,
                boolean skipLatency,
                String environmentCaveat
        ) {
            this.baseline = Objects.requireNonNull(baseline, "baseline");
            this.current = Objects.requireNonNull(current, "current");
            this.skipLatency = skipLatency;
            this.environmentCaveat = Objects.requireNonNull(environmentCaveat, "environmentCaveat");
        }

        boolean comparable() {
            return baseline.comparable() && current.comparable();
        }
    }

    static final class ComparisonSideResult {
        final String label;
        final Path jarPath;
        final List<String> commandLine;
        final String commitLabel;
        final BackendResult result;
        final boolean failed;
        final boolean partial;
        final String failureMessage;

        private ComparisonSideResult(
                String label,
                Path jarPath,
                List<String> commandLine,
                String commitLabel,
                BackendResult result,
                boolean failed,
                boolean partial,
                String failureMessage
        ) {
            this.label = Objects.requireNonNull(label, "label");
            this.jarPath = Objects.requireNonNull(jarPath, "jarPath");
            this.commandLine = List.copyOf(commandLine);
            this.commitLabel = Objects.requireNonNull(commitLabel, "commitLabel");
            this.result = Objects.requireNonNull(result, "result");
            this.failed = failed;
            this.partial = partial;
            this.failureMessage = failureMessage == null ? "" : failureMessage;
        }

        static ComparisonSideResult success(
                String label,
                Path jarPath,
                List<String> commandLine,
                String commitLabel,
                BackendResult result
        ) {
            return new ComparisonSideResult(label, jarPath, commandLine, commitLabel, result, false, false, "");
        }

        static ComparisonSideResult failure(
                String label,
                Path jarPath,
                List<String> commandLine,
                String commitLabel,
                BackendResult result,
                boolean partial,
                String failureMessage
        ) {
            return new ComparisonSideResult(label, jarPath, commandLine, commitLabel, result, true, partial, failureMessage);
        }

        boolean comparable() {
            return !failed && !partial;
        }

        String statusLabel() {
            if (failed && partial) {
                return "failed-partial";
            }
            if (failed) {
                return "failed";
            }
            if (partial) {
                return "partial";
            }
            return "ok";
        }
    }

    static final class ServerProcess {
        private final String javaCmd;
        private final Path serverJar;
        private final String xms;
        private final String xmx;
        private final String maxDirectMemory;
        private final YierdisBenchServerArgs serverArgs;
        private final Path logFile;

        private Process process;

        ServerProcess(
                String javaCmd,
                Path serverJar,
                String xms,
                String xmx,
                String maxDirectMemory,
                YierdisBenchServerArgs serverArgs,
                Path logFile
        ) {
            this.javaCmd = Objects.requireNonNull(javaCmd, "javaCmd");
            this.serverJar = Objects.requireNonNull(serverJar, "serverJar");
            this.xms = Objects.requireNonNull(xms, "xms");
            this.xmx = Objects.requireNonNull(xmx, "xmx");
            this.maxDirectMemory = Objects.requireNonNull(maxDirectMemory, "maxDirectMemory");
            this.serverArgs = Objects.requireNonNull(serverArgs, "serverArgs");
            this.logFile = Objects.requireNonNull(logFile, "logFile");
        }

        void start() throws IOException {
            if (process != null) {
                throw new IllegalStateException("server process already started");
            }
            Files.createDirectories(logFile.getParent());

            ProcessBuilder pb = new ProcessBuilder(commandLine());
            pb.redirectErrorStream(true);
            pb.redirectOutput(logFile.toFile());
            process = pb.start();
        }

        List<String> commandLine() {
            List<String> cmd = new ArrayList<>();
            cmd.add(javaCmd);
            cmd.add("-Xms" + xms);
            cmd.add("-Xmx" + xmx);
            cmd.add("-XX:MaxDirectMemorySize=" + maxDirectMemory);
            cmd.add("-jar");
            cmd.add(serverJar.toAbsolutePath().toString());
            cmd.addAll(serverArgs.toArgv());
            return cmd;
        }

        void stop() {
            Process p = process;
            process = null;
            if (p == null) {
                return;
            }

            if (!p.isAlive()) {
                return;
            }

            p.destroy();
            try {
                if (!p.waitFor(1500, TimeUnit.MILLISECONDS)) {
                    p.destroyForcibly();
                    p.waitFor(1500, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                p.destroyForcibly();
            }
        }
    }

    static final class ThroughputWorker implements Callable<WorkerCounter> {
        private final String host;
        private final int port;
        private final Workload workload;
        private final int requests;
        private final int pipeline;
        private final int keyspace;
        private final byte[] value;
        private final int seqStartIndex;
        private final boolean strictReplies;

        ThroughputWorker(
                String host,
                int port,
                Workload workload,
                int requests,
                int pipeline,
                int keyspace,
                byte[] value,
                int seqStartIndex,
                boolean strictReplies
        ) {
            this.host = host;
            this.port = port;
            this.workload = workload;
            this.requests = requests;
            this.pipeline = pipeline;
            this.keyspace = keyspace;
            this.value = value;
            this.seqStartIndex = seqStartIndex;
            this.strictReplies = strictReplies;
        }

        @Override
        public WorkerCounter call() {
            if (requests <= 0) {
                return new WorkerCounter(0, 0);
            }

            long ops = 0;
            long errors = 0;
            SplittableRandom rnd = new SplittableRandom(System.nanoTime() ^ Thread.currentThread().getId());

            try (Socket socket = new Socket()) {
                socket.setTcpNoDelay(true);
                socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
                try (BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
                     BufferedInputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024)) {
                    try (RespCommandWriter writer = new RespCommandWriter(out)) {
                        byte[] keyBuf = new byte[9]; // "k" + 8 digits
                        byte[] hllSparseKeyBuf = new byte[HLL_SPARSE_KEY_PREFIX.length + HLL_FIXED_DIGITS];
                        byte[] hllDenseKeyBuf = new byte[HLL_DENSE_KEY_PREFIX.length + HLL_FIXED_DIGITS];
                        byte[] hllElementBuf = new byte[HLL_ELEMENT_PREFIX.length + HLL_FIXED_DIGITS + 1 + HLL_FIXED_DIGITS];

                        int remaining = requests;
                        int seq = seqStartIndex;
                        while (remaining > 0) {
                            int batch = Math.min(pipeline, remaining);

                            for (int i = 0; i < batch; i++) {
                                int opIndex = requests - remaining + i;
                                int keyIndex = workload == Workload.SET_SEQUENTIAL
                                        ? seq++ % keyspace
                                        : rnd.nextInt(keyspace);
                                switch (workload) {
                                    case SET_RANDOM:
                                        writeKey(keyBuf, keyIndex);
                                        writer.writeSet(keyBuf, value);
                                        break;
                                    case SET_SEQUENTIAL:
                                        writeKey(keyBuf, keyIndex);
                                        writer.writeSet(keyBuf, value);
                                        break;
                                    case APPEND:
                                        writeKey(keyBuf, keyIndex);
                                        writer.writeAppend(keyBuf, value);
                                        break;
                                    case GET_RANDOM:
                                        writeKey(keyBuf, keyIndex);
                                        writer.writeGet(keyBuf);
                                        break;
                                    case PFADD_SPARSE:
                                        writeHllKey(hllSparseKeyBuf, HLL_SPARSE_KEY_PREFIX, keyIndex);
                                        writeHllElement(hllElementBuf, keyIndex, opIndex);
                                        writer.writePfadd(hllSparseKeyBuf, hllElementBuf);
                                        break;
                                    case PFADD_DENSE:
                                        writeDenseHllKey(hllDenseKeyBuf, keyIndex);
                                        writeHllElement(hllElementBuf, keyIndex, opIndex);
                                        writer.writePfadd(hllDenseKeyBuf, hllElementBuf);
                                        break;
                                    case PFCOUNT:
                                        writeDenseHllKey(hllDenseKeyBuf, keyIndex);
                                        writer.writePfcount(hllDenseKeyBuf);
                                        break;
                                    case PING:
                                        writer.writePing();
                                        break;
                                    default:
                                        throw new IllegalStateException("unexpected workload: " + workload);
                                }
                            }
                            out.flush();

                            for (int i = 0; i < batch; i++) {
                                RespClientCodec.RespReply reply = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
                                if (isErrorReply(reply)) {
                                    errors++;
                                } else if (strictReplies && !validateStrictReply(workload, reply, value.length)) {
                                    errors++;
                                }
                            }

                            ops += batch;
                            remaining -= batch;
                        }
                    }
                }
            } catch (Exception e) {
                // 压测线程内出现异常：计入错误并尽量返回当前统计，便于主线程输出日志提示。
                errors++;
            }

            return new WorkerCounter(ops, errors);
        }
    }

    static final class LatencyWorker implements Callable<LatencySamples> {
        private final String host;
        private final int port;
        private final Workload workload;
        private final int requests;
        private final int keyspace;
        private final byte[] value;
        private final boolean strictReplies;

        LatencyWorker(String host, int port, Workload workload, int requests, int keyspace, byte[] value, boolean strictReplies) {
            this.host = host;
            this.port = port;
            this.workload = workload;
            this.requests = requests;
            this.keyspace = keyspace;
            this.value = value;
            this.strictReplies = strictReplies;
        }

        @Override
        public LatencySamples call() {
            if (requests <= 0) {
                return new LatencySamples(new long[0], 0);
            }

            long errors = 0;
            long[] samples = new long[requests];
            int recorded = 0;
            SplittableRandom rnd = new SplittableRandom(System.nanoTime() ^ Thread.currentThread().getId());
            byte[] keyBuf = new byte[9];
            byte[] hllSparseKeyBuf = new byte[HLL_SPARSE_KEY_PREFIX.length + HLL_FIXED_DIGITS];
            byte[] hllDenseKeyBuf = new byte[HLL_DENSE_KEY_PREFIX.length + HLL_FIXED_DIGITS];
            byte[] hllElementBuf = new byte[HLL_ELEMENT_PREFIX.length + HLL_FIXED_DIGITS + 1 + HLL_FIXED_DIGITS];

            try (Socket socket = new Socket()) {
                socket.setTcpNoDelay(true);
                socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
                try (BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
                     BufferedInputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024)) {
                    try (RespCommandWriter writer = new RespCommandWriter(out)) {
                        for (int i = 0; i < requests; i++) {
                            int keyIndex = rnd.nextInt(keyspace);

                            long t0 = System.nanoTime();
                            switch (workload) {
                                case PING:
                                    writer.writePing();
                                    break;
                                case SET_RANDOM:
                                    writeKey(keyBuf, keyIndex);
                                    writer.writeSet(keyBuf, value);
                                    break;
                                case APPEND:
                                    writeKey(keyBuf, keyIndex);
                                    writer.writeAppend(keyBuf, value);
                                    break;
                                case GET_RANDOM:
                                    writeKey(keyBuf, keyIndex);
                                    writer.writeGet(keyBuf);
                                    break;
                                case PFADD_SPARSE:
                                    writeHllKey(hllSparseKeyBuf, HLL_SPARSE_KEY_PREFIX, keyIndex);
                                    writeHllElement(hllElementBuf, keyIndex, i);
                                    writer.writePfadd(hllSparseKeyBuf, hllElementBuf);
                                    break;
                                case PFADD_DENSE:
                                    writeDenseHllKey(hllDenseKeyBuf, keyIndex);
                                    writeHllElement(hllElementBuf, keyIndex, i);
                                    writer.writePfadd(hllDenseKeyBuf, hllElementBuf);
                                    break;
                                case PFCOUNT:
                                    writeDenseHllKey(hllDenseKeyBuf, keyIndex);
                                    writer.writePfcount(hllDenseKeyBuf);
                                    break;
                                default:
                                    throw new IllegalStateException("unexpected workload: " + workload);
                            }
                            out.flush();
                            RespClientCodec.RespReply reply = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
                            if (isErrorReply(reply)) {
                                errors++;
                            } else if (strictReplies && !validateStrictReply(workload, reply, value.length)) {
                                errors++;
                            }
                            long t1 = System.nanoTime();
                            samples[i] = t1 - t0;
                            recorded++;
                        }
                    }
                }
            } catch (Exception e) {
                errors++;
                // 截断：只返回已采样部分，避免主线程继续等待无意义数据。
                samples = Arrays.copyOf(samples, recorded);
            }
            return new LatencySamples(samples, errors);
        }
    }

    static final class RespCommandWriter implements AutoCloseable {
        private static final byte[] CMD_PING = "PING".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] CMD_GET = "GET".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] CMD_SET = "SET".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] CMD_APPEND = "APPEND".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] CMD_PFADD = "PFADD".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] CMD_PFCOUNT = "PFCOUNT".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] CMD_PFMERGE = "PFMERGE".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] FRAME_PING = RespClientCodec.encodeCommand(List.of(CMD_PING));

        private final OutputStream out;
        private final MutableRequestArgs requestArgs = new MutableRequestArgs();

        RespCommandWriter(OutputStream out) {
            this.out = Objects.requireNonNull(out, "out");
        }

        void writePing() throws IOException {
            out.write(FRAME_PING);
        }

        void writeGet(byte[] key) throws IOException {
            writeFrame(requestArgs.with(CMD_GET, key));
        }

        void writeSet(byte[] key, byte[] value) throws IOException {
            writeFrame(requestArgs.with(CMD_SET, key, value));
        }

        void writeAppend(byte[] key, byte[] value) throws IOException {
            writeFrame(requestArgs.with(CMD_APPEND, key, value));
        }

        void writePfadd(byte[] key, byte[] element) throws IOException {
            writeFrame(requestArgs.with(CMD_PFADD, key, element));
        }

        void writePfcount(byte[] key) throws IOException {
            writeFrame(requestArgs.with(CMD_PFCOUNT, key));
        }

        void writePfmerge(byte[] destKey, byte[] sourceKey) throws IOException {
            writeFrame(requestArgs.with(CMD_PFMERGE, destKey, sourceKey));
        }

        private void writeFrame(List<byte[]> args) throws IOException {
            RespClientCodec.writeCommand(out, args);
        }

        @Override
        public void close() {
            // no-op
        }

        private static final class MutableRequestArgs extends AbstractList<byte[]> {
            private byte[] cmd;
            private byte[] arg1;
            private byte[] arg2;
            private int size;

            MutableRequestArgs with(byte[] cmd, byte[] arg1) {
                this.cmd = cmd;
                this.arg1 = arg1;
                this.arg2 = null;
                this.size = 2;
                return this;
            }

            MutableRequestArgs with(byte[] cmd, byte[] arg1, byte[] arg2) {
                this.cmd = cmd;
                this.arg1 = arg1;
                this.arg2 = arg2;
                this.size = 3;
                return this;
            }

            @Override
            public byte[] get(int index) {
                switch (index) {
                    case 0:
                        return cmd;
                    case 1:
                        return arg1;
                    case 2:
                        if (size >= 3) {
                            return arg2;
                        }
                        break;
                    default:
                        break;
                }
                throw new IndexOutOfBoundsException("index=" + index + ", size=" + size);
            }

            @Override
            public int size() {
                return size;
            }
        }
    }

    static final class WorkerCounter {
        final long ops;
        final long errors;

        WorkerCounter(long ops, long errors) {
            this.ops = ops;
            this.errors = errors;
        }
    }

    static final class ThroughputResult {
        final Workload workload;
        final long ops;
        final long errors;
        final double seconds;
        final double qps;
        final Instant startedAt;

        ThroughputResult(Workload workload, long ops, long errors, double seconds, double qps, Instant startedAt) {
            this.workload = workload;
            this.ops = ops;
            this.errors = errors;
            this.seconds = seconds;
            this.qps = qps;
            this.startedAt = startedAt;
        }

        @Override
        public String toString() {
            return "ops=" + ops
                    + ", errors=" + errors
                    + ", time=" + DF.format(seconds) + "s"
                    + ", qps=" + DF.format(qps);
        }
    }

    static final class LatencySamples {
        final long[] samples;
        final long errors;

        LatencySamples(long[] samples, long errors) {
            this.samples = samples;
            this.errors = errors;
        }
    }

    static final class LatencyResult {
        final Workload workload;
        final int ops;
        final long errors;
        final double seconds;
        final double qps;
        final LatencyStats stats;

        LatencyResult(Workload workload, int ops, long errors, double seconds, double qps, LatencyStats stats) {
            this.workload = workload;
            this.ops = ops;
            this.errors = errors;
            this.seconds = seconds;
            this.qps = qps;
            this.stats = stats;
        }

        @Override
        public String toString() {
            return "ops=" + ops
                    + ", errors=" + errors
                    + ", time=" + DF.format(seconds) + "s"
                    + ", qps=" + DF.format(qps)
                    + ", p50=" + DF.format(stats.p50Millis()) + "ms"
                    + ", p95=" + DF.format(stats.p95Millis()) + "ms"
                    + ", p99=" + DF.format(stats.p99Millis()) + "ms";
        }
    }

    static final class LatencyStats {
        final int count;
        final double avgNanos;
        final long p50Nanos;
        final long p95Nanos;
        final long p99Nanos;
        final long maxNanos;

        private LatencyStats(int count, double avgNanos, long p50Nanos, long p95Nanos, long p99Nanos, long maxNanos) {
            this.count = count;
            this.avgNanos = avgNanos;
            this.p50Nanos = p50Nanos;
            this.p95Nanos = p95Nanos;
            this.p99Nanos = p99Nanos;
            this.maxNanos = maxNanos;
        }

        static LatencyStats ofSortedNanos(long[] sorted) {
            if (sorted == null || sorted.length == 0) {
                return new LatencyStats(0, Double.NaN, 0, 0, 0, 0);
            }
            long sum = 0;
            for (long v : sorted) {
                sum += v;
            }
            int n = sorted.length;
            long p50 = percentile(sorted, 0.50);
            long p95 = percentile(sorted, 0.95);
            long p99 = percentile(sorted, 0.99);
            long max = sorted[n - 1];
            return new LatencyStats(n, (double) sum / n, p50, p95, p99, max);
        }

        private static long percentile(long[] sorted, double p) {
            if (sorted.length == 0) {
                return 0;
            }
            double idx = p * (sorted.length - 1);
            int i = (int) Math.round(idx);
            i = Math.max(0, Math.min(sorted.length - 1, i));
            return sorted[i];
        }

        double p50Millis() {
            return p50Nanos / 1_000_000.0;
        }

        double p95Millis() {
            return p95Nanos / 1_000_000.0;
        }

        double p99Millis() {
            return p99Nanos / 1_000_000.0;
        }
    }

    static final class BackendResult {
        final String backend;
        final int port;
        ThroughputResult setThroughput;
        ThroughputResult appendThroughput;
        ThroughputResult getThroughput;
        ThroughputResult pfaddSparseThroughput;
        ThroughputResult pfaddDenseThroughput;
        ThroughputResult pfcountThroughput;
        LatencyResult pingLatency;
        LatencyResult setLatency;
        LatencyResult appendLatency;
        LatencyResult getLatency;
        LatencyResult pfaddSparseLatency;
        LatencyResult pfaddDenseLatency;

        BackendResult(String backend, int port) {
            this.backend = backend;
            this.port = port;
        }
    }

    static final class DbDefragComparisonResult {
        final LatencyResult disabled;
        final LatencyResult enabled;
        final double impactPercent;

        DbDefragComparisonResult(LatencyResult disabled, LatencyResult enabled, double impactPercent) {
            this.disabled = Objects.requireNonNull(disabled, "disabled");
            this.enabled = Objects.requireNonNull(enabled, "enabled");
            this.impactPercent = impactPercent;
        }
    }

    static final class NativeEvalReport {
        final List<NativeSizeClassResult> sizeClasses;
        final NativeResolveResult resolve;
        final NativeReallocResult realloc;
        final NativePinResult pin;
        final List<NativeMetadataResult> metadata;
        final NativeQuarantineResult quarantine;
        final NativeChurnResult churn;
        final NativeDefragImpactResult defragImpact;

        NativeEvalReport(
                List<NativeSizeClassResult> sizeClasses,
                NativeResolveResult resolve,
                NativeReallocResult realloc,
                NativePinResult pin,
                List<NativeMetadataResult> metadata,
                NativeQuarantineResult quarantine,
                NativeChurnResult churn,
                NativeDefragImpactResult defragImpact
        ) {
            this.sizeClasses = Objects.requireNonNull(sizeClasses, "sizeClasses");
            this.resolve = Objects.requireNonNull(resolve, "resolve");
            this.realloc = Objects.requireNonNull(realloc, "realloc");
            this.pin = Objects.requireNonNull(pin, "pin");
            this.metadata = Objects.requireNonNull(metadata, "metadata");
            this.quarantine = Objects.requireNonNull(quarantine, "quarantine");
            this.churn = Objects.requireNonNull(churn, "churn");
            this.defragImpact = Objects.requireNonNull(defragImpact, "defragImpact");
        }
    }

    static final class NativeSizeClassResult {
        final String label;
        final int bytes;
        final int ops;
        final double allocMicros;
        final double freeMicros;

        NativeSizeClassResult(String label, int bytes, int ops, double allocMicros, double freeMicros) {
            this.label = label;
            this.bytes = bytes;
            this.ops = ops;
            this.allocMicros = allocMicros;
            this.freeMicros = freeMicros;
        }
    }

    static final class NativeResolveResult {
        final int ops;
        final double resolveCloseMicros;

        NativeResolveResult(int ops, double resolveCloseMicros) {
            this.ops = ops;
            this.resolveCloseMicros = resolveCloseMicros;
        }
    }

    static final class NativeReallocResult {
        final int ops;
        final int inPlace;
        final int moved;
        final double avgMicros;

        NativeReallocResult(int ops, int inPlace, int moved, double avgMicros) {
            this.ops = ops;
            this.inPlace = inPlace;
            this.moved = moved;
            this.avgMicros = avgMicros;
        }
    }

    static final class NativePinResult {
        final int ops;
        final double pinUnpinMicros;

        NativePinResult(int ops, double pinUnpinMicros) {
            this.ops = ops;
            this.pinUnpinMicros = pinUnpinMicros;
        }
    }

    static final class NativeMetadataResult {
        final String label;
        final int valueBytes;
        final double metadataBytesPerObject;
        final double metadataPercent;

        NativeMetadataResult(String label, int valueBytes, double metadataBytesPerObject, double metadataPercent) {
            this.label = label;
            this.valueBytes = valueBytes;
            this.metadataBytesPerObject = metadataBytesPerObject;
            this.metadataPercent = metadataPercent;
        }
    }

    static final class NativeQuarantineResult {
        final long retainedBytes;
        final double retainedPercentOfReserved;

        NativeQuarantineResult(long retainedBytes, double retainedPercentOfReserved) {
            this.retainedBytes = retainedBytes;
            this.retainedPercentOfReserved = retainedPercentOfReserved;
        }
    }

    static final class NativeChurnResult {
        final int ops;
        final double avgMicros;
        final double p99Micros;

        NativeChurnResult(int ops, double avgMicros, double p99Micros) {
            this.ops = ops;
            this.avgMicros = avgMicros;
            this.p99Micros = p99Micros;
        }
    }

    static final class NativeDefragImpactResult {
        final double disabledP99Micros;
        final double enabledP99Micros;
        final double impactPercent;
        final long movedObjects;

        NativeDefragImpactResult(double disabledP99Micros, double enabledP99Micros, double impactPercent, long movedObjects) {
            this.disabledP99Micros = disabledP99Micros;
            this.enabledP99Micros = enabledP99Micros;
            this.impactPercent = impactPercent;
            this.movedObjects = movedObjects;
        }
    }

    private static void writeKey(byte[] out, int keyIndex) {
        // 固定长度 key：k + 8 位十进制补零，便于保持请求大小稳定。
        out[0] = 'k';
        int x = keyIndex;
        for (int i = 8; i >= 1; i--) {
            int digit = x % 10;
            out[i] = (byte) ('0' + digit);
            x /= 10;
        }
    }

    private static byte[] hllElement(String prefix, int keyIndex, int opIndex) {
        return (prefix + ':' + keyIndex + ':' + opIndex).getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] hllKey(String prefix, int keyIndex) {
        return ("hll:" + prefix + ':' + keyIndex).getBytes(StandardCharsets.US_ASCII);
    }

    private static int effectiveNativeEvalIterations(int requestedIterations) {
        if (requestedIterations <= 0) {
            throw new IllegalArgumentException("nativeEvalIterations must be > 0");
        }
        return Math.max(8, requestedIterations);
    }

    private static void writeHllKey(byte[] out, byte[] prefix, int keyIndex) {
        System.arraycopy(prefix, 0, out, 0, prefix.length);
        writeFixedDigits(out, prefix.length, keyIndex);
    }

    static void writeDenseHllKey(byte[] out, int keyIndex) {
        writeHllKey(out, HLL_DENSE_KEY_PREFIX, keyIndex);
    }

    private static void writeHllElement(byte[] out, int keyIndex, int opIndex) {
        System.arraycopy(HLL_ELEMENT_PREFIX, 0, out, 0, HLL_ELEMENT_PREFIX.length);
        int offset = HLL_ELEMENT_PREFIX.length;
        writeFixedDigits(out, offset, keyIndex);
        out[offset + HLL_FIXED_DIGITS] = ':';
        writeFixedDigits(out, offset + HLL_FIXED_DIGITS + 1, opIndex);
    }

    private static void writeFixedDigits(byte[] out, int offset, int value) {
        int x = Math.floorMod(value, 100_000_000);
        for (int i = offset + HLL_FIXED_DIGITS - 1; i >= offset; i--) {
            out[i] = (byte) ('0' + (x % 10));
            x /= 10;
        }
    }

    private YierdisBench() {
    }
}
