package yier.bubu.redis.bench;

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
 * 纯 Java 压测工具：用于对比 Yierdis 的 off-heap 后端（none/netty/unsafe）的吞吐与延迟。
 * <p>
 * 设计原则：
 * - 不依赖 redis-benchmark 等系统工具
 * - 使用本地 TCP + RESP2，避免“进程内直连”偏离真实网络路径
 * - 以固定请求数为主，输出 QPS 与简单的延迟分位数
 */
public final class YierdisBench {
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT_BASE = 16378;
    private static final List<String> DEFAULT_BACKENDS = List.of("none", "netty", "unsafe");

    // 共享容器 + memory limit=16G + server 固定 -Xms4g -Xmx4g 的保守默认预算
    private static final String DEFAULT_XMS = "4g";
    private static final String DEFAULT_XMX = "4g";
    private static final String DEFAULT_MAX_DIRECT_MEMORY = "6g";
    private static final long DEFAULT_MAXMEMORY_BYTES = 7L * 1024 * 1024 * 1024; // 7GiB
    private static final long DEFAULT_OFFHEAP_MAX_BYTES = 4L * 1024 * 1024 * 1024; // 4GiB
    private static final String DEFAULT_MAXMEMORY_POLICY = "allkeys-lru";
    private static final int DEFAULT_MAXMEMORY_SAMPLES = 5;

    private static final int DEFAULT_KEYSPACE = 1_000_000;
    private static final int DEFAULT_DATA_SIZE = 256;

    private static final int DEFAULT_REQUESTS = 1_000_000;
    private static final int DEFAULT_CLIENTS = 200;
    private static final int DEFAULT_PIPELINE = 16;

    private static final int DEFAULT_LATENCY_REQUESTS = 200_000;
    private static final int DEFAULT_LATENCY_CLIENTS = 50;

    private static final int CONNECT_TIMEOUT_MILLIS = 1000;
    private static final int READY_TIMEOUT_MILLIS = 15_000;

    private static final DecimalFormat DF = new DecimalFormat("0.000");

    public static void main(String[] args) throws Exception {
        BenchConfig config = BenchConfig.parse(args);
        if (config.showHelp) {
            printHelp();
            return;
        }

        Path serverJar = config.serverJar != null ? config.serverJar : findServerJar();
        Path runDir = Files.createTempDirectory(Path.of(".").toAbsolutePath().normalize(), ".bench-java.");

        println("YierdisBench（纯 Java）");
        println("运行目录: " + runDir);
        println("serverJar: " + serverJar);
        println("");
        printBudgetHint(config);
        println("");

        List<BackendResult> results = new ArrayList<>();
        for (int i = 0; i < config.backends.size(); i++) {
            String backend = config.backends.get(i);
            int port = config.portBase + i;
            Path logFile = runDir.resolve("server-" + backend + ".log");

            println("============================================================");
            println("后端: " + backend + "  port=" + port);
            println("日志: " + logFile);

            ServerProcess server = new ServerProcess(
                    config.javaCmd,
                    serverJar,
                    config.serverXms,
                    config.serverXmx,
                    config.serverMaxDirectMemory,
                    port,
                    backend,
                    config.offheapMaxBytes,
                    config.maxmemoryBytes,
                    config.maxmemoryPolicy,
                    config.maxmemorySamples,
                    logFile,
                    config.serverExtraArgs
            );

            BackendResult backendResult = new BackendResult(backend, port);
            Instant startedAt = Instant.now();
            try {
                server.start();
                if (!waitReady(config.host, port, READY_TIMEOUT_MILLIS)) {
                    throw new IllegalStateException("服务未就绪，请检查日志: " + logFile);
                }
                println("服务就绪，启动耗时: " + Duration.between(startedAt, Instant.now()).toMillis() + " ms");

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
                            config.dataSize
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
                        config.dataSize
                );
                ThroughputResult getQps = runThroughput(
                        config.host,
                        port,
                        Workload.GET_RANDOM,
                        config.requests,
                        config.clients,
                        config.pipeline,
                        config.keyspace,
                        config.dataSize
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
                            config.dataSize
                    );
                    LatencyResult setLat = runLatency(
                            config.host,
                            port,
                            Workload.SET_RANDOM,
                            config.latencyRequests,
                            config.latencyClients,
                            config.keyspace,
                            config.dataSize
                    );
                    LatencyResult getLat = runLatency(
                            config.host,
                            port,
                            Workload.GET_RANDOM,
                            config.latencyRequests,
                            config.latencyClients,
                            config.keyspace,
                            config.dataSize
                    );
                    backendResult.pingLatency = pingLat;
                    backendResult.setLatency = setLat;
                    backendResult.getLatency = getLat;
                    println("PING: " + pingLat);
                    println("SET : " + setLat);
                    println("GET : " + getLat);
                }
            } finally {
                server.stop();
            }

            results.add(backendResult);
        }

        println("");
        println("============================================================");
        println("汇总（吞吐越大越好；延迟越小越好）");
        printSummary(results, config.skipLatency);
        println("");
        println("完成。");
    }

    private static void printHelp() {
        println("用法：java -jar yierdis-bench-<version>.jar [options]");
        println("");
        println("常用参数：");
        println("  --serverJar <path>                 指定 yierdis-server jar；默认自动从 yierdis-server/target/ 查找");
        println("  --backends none,netty,unsafe       默认: none,netty,unsafe");
        println("  --host 127.0.0.1                   默认: 127.0.0.1");
        println("  --portBase 16378                   默认: 16378（每个后端 +1）");
        println("");
        println("压测规模：");
        println("  --keyspace 1000000                 默认: 1000000");
        println("  --dataSize 256                     默认: 256 bytes");
        println("  --requests 1000000                 默认: 1000000（吞吐压测每种命令的总请求数）");
        println("  --clients 200                      默认: 200（吞吐压测连接数）");
        println("  --pipeline 16                      默认: 16（吞吐压测 pipeline 深度）");
        println("");
        println("延迟压测：");
        println("  --latencyRequests 200000           默认: 200000（总请求数）");
        println("  --latencyClients 50                默认: 50（连接数）");
        println("  --skipLatency                      跳过延迟压测");
        println("");
        println("服务端（子进程）预算：");
        println("  --xms 4g --xmx 4g                  默认: -Xms4g -Xmx4g");
        println("  --maxDirectMemory 6g               默认: -XX:MaxDirectMemorySize=6g");
        println("  --maxmemoryBytes 7516192768        默认: 7GiB（容器 16G 保守起步）");
        println("  --offheapMaxBytes 4294967296       默认: 4GiB（仅对 netty/unsafe 生效）");
        println("  --maxmemoryPolicy allkeys-lru      默认: allkeys-lru");
        println("  --maxmemorySamples 5               默认: 5");
        println("");
        println("其它：");
        println("  --skipPrefill                      跳过预置数据（GET 可能大量 miss，影响可比性）");
        println("  --javaCmd <java>                   指定用于启动 server 子进程的 java 命令（默认: java）");
        println("  --serverArg \"--ioThreads 1\"        追加 server 参数（可重复多次）");
        println("");
    }

    private static void printBudgetHint(BenchConfig config) {
        println("预算提示（共享容器 + memory limit=16G 的保守默认值，可通过参数覆盖）");
        println("  server JVM : -Xms" + config.serverXms + " -Xmx" + config.serverXmx
                + " -XX:MaxDirectMemorySize=" + config.serverMaxDirectMemory);
        println("  maxmemory  : --maxmemoryBytes " + config.maxmemoryBytes + " --maxmemoryPolicy " + config.maxmemoryPolicy);
        println("  off-heap   : --offheapMaxBytes " + config.offheapMaxBytes + "（仅 netty/unsafe）");
        println("  提醒：容器 OOMKill 优先下调 offheapMaxBytes / maxDirectMemory，而不是只看 maxmemory。");
    }

    private static Path findServerJar() {
        Path target = Path.of("yierdis-server", "target");
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

    private static boolean waitReady(String host, int port, int timeoutMillis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            try (Socket s = new Socket()) {
                s.setTcpNoDelay(true);
                s.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
                try (BufferedOutputStream out = new BufferedOutputStream(s.getOutputStream());
                     BufferedInputStream in = new BufferedInputStream(s.getInputStream())) {
                    RespCommandWriter w = new RespCommandWriter(out);
                    w.writePing();
                    out.flush();
                    RespResponseSkipper.skipOne(in);
                    return true;
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
            int dataSize
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
                    host, port, workload, n, pipeline, keyspace, value, startIndex
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
            int dataSize
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
                    host, port, workload, n, keyspace, value
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

    private static void printSummary(List<BackendResult> results, boolean skipLatency) {
        String header = skipLatency
                ? String.format("%-8s | %12s | %12s", "backend", "SET_QPS", "GET_QPS")
                : String.format("%-8s | %12s | %12s | %16s | %16s | %16s", "backend", "SET_QPS", "GET_QPS", "PING_p95(ms)", "SET_p95(ms)", "GET_p95(ms)");
        println(header);
        println(repeat('-', header.length()));

        for (BackendResult r : results) {
            String setQps = r.setThroughput == null ? "-" : formatQps(r.setThroughput.qps);
            String getQps = r.getThroughput == null ? "-" : formatQps(r.getThroughput.qps);
            if (skipLatency) {
                println(String.format("%-8s | %12s | %12s", r.backend, setQps, getQps));
                continue;
            }
            String pingP95 = r.pingLatency == null ? "-" : DF.format(r.pingLatency.stats.p95Millis());
            String setP95 = r.setLatency == null ? "-" : DF.format(r.setLatency.stats.p95Millis());
            String getP95 = r.getLatency == null ? "-" : DF.format(r.getLatency.stats.p95Millis());
            println(String.format("%-8s | %12s | %12s | %16s | %16s | %16s", r.backend, setQps, getQps, pingP95, setP95, getP95));
        }
    }

    private static String formatQps(double qps) {
        if (Double.isNaN(qps) || Double.isInfinite(qps)) {
            return "NaN";
        }
        if (qps >= 1_000_000) {
            return DF.format(qps / 1_000_000.0) + "M";
        }
        if (qps >= 1_000) {
            return DF.format(qps / 1_000.0) + "K";
        }
        return DF.format(qps);
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

    enum Workload {
        PING,
        SET_RANDOM,
        SET_SEQUENTIAL,
        GET_RANDOM
    }

    static final class BenchConfig {
        final boolean showHelp;
        final Path serverJar;
        final List<String> backends;
        final String host;
        final int portBase;

        final String javaCmd;
        final String serverXms;
        final String serverXmx;
        final String serverMaxDirectMemory;
        final long maxmemoryBytes;
        final String maxmemoryPolicy;
        final int maxmemorySamples;
        final long offheapMaxBytes;
        final List<String> serverExtraArgs;

        final int keyspace;
        final int dataSize;
        final int requests;
        final int clients;
        final int pipeline;
        final int latencyRequests;
        final int latencyClients;

        final boolean skipPrefill;
        final boolean skipLatency;

        private BenchConfig(
                boolean showHelp,
                Path serverJar,
                List<String> backends,
                String host,
                int portBase,
                String javaCmd,
                String serverXms,
                String serverXmx,
                String serverMaxDirectMemory,
                long maxmemoryBytes,
                String maxmemoryPolicy,
                int maxmemorySamples,
                long offheapMaxBytes,
                List<String> serverExtraArgs,
                int keyspace,
                int dataSize,
                int requests,
                int clients,
                int pipeline,
                int latencyRequests,
                int latencyClients,
                boolean skipPrefill,
                boolean skipLatency
        ) {
            this.showHelp = showHelp;
            this.serverJar = serverJar;
            this.backends = backends;
            this.host = host;
            this.portBase = portBase;
            this.javaCmd = javaCmd;
            this.serverXms = serverXms;
            this.serverXmx = serverXmx;
            this.serverMaxDirectMemory = serverMaxDirectMemory;
            this.maxmemoryBytes = maxmemoryBytes;
            this.maxmemoryPolicy = maxmemoryPolicy;
            this.maxmemorySamples = maxmemorySamples;
            this.offheapMaxBytes = offheapMaxBytes;
            this.serverExtraArgs = serverExtraArgs;
            this.keyspace = keyspace;
            this.dataSize = dataSize;
            this.requests = requests;
            this.clients = clients;
            this.pipeline = pipeline;
            this.latencyRequests = latencyRequests;
            this.latencyClients = latencyClients;
            this.skipPrefill = skipPrefill;
            this.skipLatency = skipLatency;
        }

        static BenchConfig parse(String[] args) {
            Objects.requireNonNull(args, "args");

            boolean help = false;
            Path serverJar = null;
            List<String> backends = new ArrayList<>(DEFAULT_BACKENDS);
            String host = DEFAULT_HOST;
            int portBase = DEFAULT_PORT_BASE;

            String javaCmd = "java";
            String xms = DEFAULT_XMS;
            String xmx = DEFAULT_XMX;
            String maxDirect = DEFAULT_MAX_DIRECT_MEMORY;
            long maxmemoryBytes = DEFAULT_MAXMEMORY_BYTES;
            String maxmemoryPolicy = DEFAULT_MAXMEMORY_POLICY;
            int maxmemorySamples = DEFAULT_MAXMEMORY_SAMPLES;
            long offheapMaxBytes = DEFAULT_OFFHEAP_MAX_BYTES;
            List<String> serverArgs = new ArrayList<>();

            int keyspace = DEFAULT_KEYSPACE;
            int dataSize = DEFAULT_DATA_SIZE;
            int requests = DEFAULT_REQUESTS;
            int clients = DEFAULT_CLIENTS;
            int pipeline = DEFAULT_PIPELINE;
            int latencyRequests = DEFAULT_LATENCY_REQUESTS;
            int latencyClients = DEFAULT_LATENCY_CLIENTS;
            boolean skipPrefill = false;
            boolean skipLatency = false;

            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                switch (a) {
                    case "--help":
                    case "-h":
                        help = true;
                        break;
                    case "--serverJar":
                        serverJar = Path.of(requireArg(args, ++i, a));
                        break;
                    case "--backends":
                        backends = splitCsv(requireArg(args, ++i, a));
                        break;
                    case "--host":
                        host = requireArg(args, ++i, a);
                        break;
                    case "--portBase":
                        portBase = Integer.parseInt(requireArg(args, ++i, a));
                        break;
                    case "--javaCmd":
                        javaCmd = requireArg(args, ++i, a);
                        break;
                    case "--xms":
                        xms = requireArg(args, ++i, a);
                        break;
                    case "--xmx":
                        xmx = requireArg(args, ++i, a);
                        break;
                    case "--maxDirectMemory":
                        maxDirect = requireArg(args, ++i, a);
                        break;
                    case "--maxmemoryBytes":
                        maxmemoryBytes = Long.parseLong(requireArg(args, ++i, a));
                        break;
                    case "--maxmemoryPolicy":
                        maxmemoryPolicy = requireArg(args, ++i, a);
                        break;
                    case "--maxmemorySamples":
                        maxmemorySamples = Integer.parseInt(requireArg(args, ++i, a));
                        break;
                    case "--offheapMaxBytes":
                        offheapMaxBytes = Long.parseLong(requireArg(args, ++i, a));
                        break;
                    case "--serverArg":
                        serverArgs.add(requireArg(args, ++i, a));
                        break;
                    case "--keyspace":
                        keyspace = Integer.parseInt(requireArg(args, ++i, a));
                        break;
                    case "--dataSize":
                        dataSize = Integer.parseInt(requireArg(args, ++i, a));
                        break;
                    case "--requests":
                        requests = Integer.parseInt(requireArg(args, ++i, a));
                        break;
                    case "--clients":
                        clients = Integer.parseInt(requireArg(args, ++i, a));
                        break;
                    case "--pipeline":
                        pipeline = Integer.parseInt(requireArg(args, ++i, a));
                        break;
                    case "--latencyRequests":
                        latencyRequests = Integer.parseInt(requireArg(args, ++i, a));
                        break;
                    case "--latencyClients":
                        latencyClients = Integer.parseInt(requireArg(args, ++i, a));
                        break;
                    case "--skipPrefill":
                        skipPrefill = true;
                        break;
                    case "--skipLatency":
                        skipLatency = true;
                        break;
                    default:
                        throw new IllegalArgumentException("未知参数: " + a + "（使用 --help 查看用法）");
                }
            }

            if (serverJar != null && !Files.isRegularFile(serverJar)) {
                throw new IllegalArgumentException("serverJar 不存在: " + serverJar.toAbsolutePath());
            }

            if (backends.isEmpty()) {
                throw new IllegalArgumentException("backends 不能为空");
            }
            for (String b : backends) {
                String normalized = b.trim().toLowerCase(Locale.ROOT);
                if (normalized.isEmpty()) {
                    continue;
                }
                if (!normalized.equals("none") && !normalized.equals("netty") && !normalized.equals("unsafe") && !normalized.equals("foreign")) {
                    throw new IllegalArgumentException("不支持的 backend: " + b);
                }
            }

            return new BenchConfig(
                    help,
                    serverJar,
                    backends,
                    host,
                    portBase,
                    javaCmd,
                    xms,
                    xmx,
                    maxDirect,
                    maxmemoryBytes,
                    maxmemoryPolicy,
                    maxmemorySamples,
                    offheapMaxBytes,
                    serverArgs,
                    keyspace,
                    dataSize,
                    requests,
                    clients,
                    pipeline,
                    latencyRequests,
                    latencyClients,
                    skipPrefill,
                    skipLatency
            );
        }

        private static String requireArg(String[] args, int index, String optName) {
            if (index < 0 || index >= args.length) {
                throw new IllegalArgumentException("参数缺失: " + optName);
            }
            return args[index];
        }

        private static List<String> splitCsv(String csv) {
            if (csv == null || csv.isBlank()) {
                return List.of();
            }
            String[] parts = csv.split(",");
            List<String> out = new ArrayList<>(parts.length);
            for (String p : parts) {
                String s = p.trim();
                if (!s.isEmpty()) {
                    out.add(s);
                }
            }
            return out;
        }
    }

    static final class ServerProcess {
        private final String javaCmd;
        private final Path serverJar;
        private final String xms;
        private final String xmx;
        private final String maxDirectMemory;
        private final int port;
        private final String backend;
        private final long offheapMaxBytes;
        private final long maxmemoryBytes;
        private final String maxmemoryPolicy;
        private final int maxmemorySamples;
        private final Path logFile;
        private final List<String> extraServerArgs;

        private Process process;

        ServerProcess(
                String javaCmd,
                Path serverJar,
                String xms,
                String xmx,
                String maxDirectMemory,
                int port,
                String backend,
                long offheapMaxBytes,
                long maxmemoryBytes,
                String maxmemoryPolicy,
                int maxmemorySamples,
                Path logFile,
                List<String> extraServerArgs
        ) {
            this.javaCmd = Objects.requireNonNull(javaCmd, "javaCmd");
            this.serverJar = Objects.requireNonNull(serverJar, "serverJar");
            this.xms = Objects.requireNonNull(xms, "xms");
            this.xmx = Objects.requireNonNull(xmx, "xmx");
            this.maxDirectMemory = Objects.requireNonNull(maxDirectMemory, "maxDirectMemory");
            this.port = port;
            this.backend = Objects.requireNonNull(backend, "backend");
            this.offheapMaxBytes = offheapMaxBytes;
            this.maxmemoryBytes = maxmemoryBytes;
            this.maxmemoryPolicy = maxmemoryPolicy;
            this.maxmemorySamples = maxmemorySamples;
            this.logFile = Objects.requireNonNull(logFile, "logFile");
            this.extraServerArgs = extraServerArgs == null ? List.of() : extraServerArgs;
        }

        void start() throws IOException {
            if (process != null) {
                throw new IllegalStateException("server process already started");
            }
            Files.createDirectories(logFile.getParent());

            List<String> cmd = new ArrayList<>();
            cmd.add(javaCmd);
            cmd.add("-Xms" + xms);
            cmd.add("-Xmx" + xmx);
            cmd.add("-XX:MaxDirectMemorySize=" + maxDirectMemory);
            cmd.add("-jar");
            cmd.add(serverJar.toAbsolutePath().toString());
            cmd.add("--port");
            cmd.add(Integer.toString(port));
            cmd.add("--maxmemoryBytes");
            cmd.add(Long.toString(maxmemoryBytes));
            cmd.add("--maxmemoryPolicy");
            cmd.add(maxmemoryPolicy);
            cmd.add("--maxmemorySamples");
            cmd.add(Integer.toString(maxmemorySamples));

            if ("none".equalsIgnoreCase(backend)) {
                cmd.add("--offheapBackend");
                cmd.add("none");
            } else {
                cmd.add("--offheapBackend");
                cmd.add(backend);
                cmd.add("--offheapMaxBytes");
                cmd.add(Long.toString(offheapMaxBytes));
            }

            for (String extra : extraServerArgs) {
                if (extra == null || extra.isBlank()) {
                    continue;
                }
                cmd.addAll(splitArgs(extra));
            }

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            pb.redirectOutput(logFile.toFile());
            process = pb.start();
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

        private static List<String> splitArgs(String s) {
            // 简化：按空白切分，不支持复杂引用；用于追加简单参数（例如 --ioThreads 1）。
            String[] parts = s.trim().split("\\s+");
            return Arrays.asList(parts);
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

        ThroughputWorker(
                String host,
                int port,
                Workload workload,
                int requests,
                int pipeline,
                int keyspace,
                byte[] value,
                int seqStartIndex
        ) {
            this.host = host;
            this.port = port;
            this.workload = workload;
            this.requests = requests;
            this.pipeline = pipeline;
            this.keyspace = keyspace;
            this.value = value;
            this.seqStartIndex = seqStartIndex;
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
                    RespCommandWriter writer = new RespCommandWriter(out);
                    byte[] keyBuf = new byte[9]; // "k" + 8 digits

                    int remaining = requests;
                    int seq = seqStartIndex;
                    while (remaining > 0) {
                        int batch = Math.min(pipeline, remaining);

                        for (int i = 0; i < batch; i++) {
                            int keyIndex = workload == Workload.SET_SEQUENTIAL
                                    ? seq++ % keyspace
                                    : rnd.nextInt(keyspace);
                            writeKey(keyBuf, keyIndex);
                            switch (workload) {
                                case SET_RANDOM:
                                case SET_SEQUENTIAL:
                                    writer.writeSet(keyBuf, value);
                                    break;
                                case GET_RANDOM:
                                    writer.writeGet(keyBuf);
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
                            RespResponseSkipper.skipOne(in);
                        }

                        ops += batch;
                        remaining -= batch;
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

        LatencyWorker(String host, int port, Workload workload, int requests, int keyspace, byte[] value) {
            this.host = host;
            this.port = port;
            this.workload = workload;
            this.requests = requests;
            this.keyspace = keyspace;
            this.value = value;
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

            try (Socket socket = new Socket()) {
                socket.setTcpNoDelay(true);
                socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
                try (BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
                     BufferedInputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024)) {
                    RespCommandWriter writer = new RespCommandWriter(out);

                    for (int i = 0; i < requests; i++) {
                        int keyIndex = rnd.nextInt(keyspace);
                        writeKey(keyBuf, keyIndex);

                        long t0 = System.nanoTime();
                        switch (workload) {
                            case PING:
                                writer.writePing();
                                break;
                            case SET_RANDOM:
                                writer.writeSet(keyBuf, value);
                                break;
                            case GET_RANDOM:
                                writer.writeGet(keyBuf);
                                break;
                            default:
                                throw new IllegalStateException("unexpected workload: " + workload);
                        }
                        out.flush();
                        RespResponseSkipper.skipOne(in);
                        long t1 = System.nanoTime();
                        samples[i] = t1 - t0;
                        recorded++;
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

    static final class RespCommandWriter {
        private static final byte[] CRLF = new byte[]{'\r', '\n'};
        private static final byte[] CMD_PING = "PING".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] CMD_GET = "GET".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] CMD_SET = "SET".getBytes(StandardCharsets.US_ASCII);

        private final OutputStream out;
        private final byte[] numBuf = new byte[32];

        RespCommandWriter(OutputStream out) {
            this.out = Objects.requireNonNull(out, "out");
        }

        void writePing() throws IOException {
            // *1\r\n$4\r\nPING\r\n
            out.write('*');
            writeIntAscii(1);
            out.write(CRLF);

            out.write('$');
            writeIntAscii(CMD_PING.length);
            out.write(CRLF);
            out.write(CMD_PING);
            out.write(CRLF);
        }

        void writeGet(byte[] key) throws IOException {
            // *2\r\n$3\r\nGET\r\n$<len>\r\n<key>\r\n
            out.write('*');
            writeIntAscii(2);
            out.write(CRLF);

            out.write('$');
            writeIntAscii(CMD_GET.length);
            out.write(CRLF);
            out.write(CMD_GET);
            out.write(CRLF);

            out.write('$');
            writeIntAscii(key.length);
            out.write(CRLF);
            out.write(key);
            out.write(CRLF);
        }

        void writeSet(byte[] key, byte[] value) throws IOException {
            // *3\r\n$3\r\nSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<val>\r\n
            out.write('*');
            writeIntAscii(3);
            out.write(CRLF);

            out.write('$');
            writeIntAscii(CMD_SET.length);
            out.write(CRLF);
            out.write(CMD_SET);
            out.write(CRLF);

            out.write('$');
            writeIntAscii(key.length);
            out.write(CRLF);
            out.write(key);
            out.write(CRLF);

            out.write('$');
            writeIntAscii(value.length);
            out.write(CRLF);
            out.write(value);
            out.write(CRLF);
        }

        private void writeIntAscii(int v) throws IOException {
            if (v < 0) {
                throw new IllegalArgumentException("v must be >= 0");
            }
            int pos = numBuf.length;
            int x = v;
            if (x == 0) {
                numBuf[--pos] = '0';
            } else {
                while (x > 0) {
                    int digit = x % 10;
                    numBuf[--pos] = (byte) ('0' + digit);
                    x /= 10;
                }
            }
            out.write(numBuf, pos, numBuf.length - pos);
        }
    }

    static final class RespResponseSkipper {
        private static final ThreadLocal<byte[]> TL_SKIP_BUF =
                ThreadLocal.withInitial(() -> new byte[8 * 1024]);

        private RespResponseSkipper() {
        }

        static void skipOne(InputStream in) throws IOException {
            int type = in.read();
            if (type < 0) {
                throw new IOException("EOF");
            }
            switch (type) {
                case '+': // simple string
                case '-': // error
                case ':': // integer
                    skipLine(in);
                    return;
                case '$': { // bulk string
                    long len = readLongLine(in);
                    if (len == -1) {
                        return;
                    }
                    if (len < 0) {
                        throw new IOException("invalid bulk length: " + len);
                    }
                    skipFully(in, len + 2); // data + CRLF
                    return;
                }
                case '*': { // array
                    long count = readLongLine(in);
                    if (count == -1) {
                        return;
                    }
                    if (count < 0) {
                        throw new IOException("invalid array length: " + count);
                    }
                    for (long i = 0; i < count; i++) {
                        skipOne(in);
                    }
                    return;
                }
                default:
                    throw new IOException("unknown RESP type: " + (char) type);
            }
        }

        private static void skipLine(InputStream in) throws IOException {
            int prev = -1;
            for (; ; ) {
                int b = in.read();
                if (b < 0) {
                    throw new IOException("EOF while reading line");
                }
                if (prev == '\r' && b == '\n') {
                    return;
                }
                prev = b;
            }
        }

        private static long readLongLine(InputStream in) throws IOException {
            long sign = 1;
            long value = 0;
            boolean started = false;
            int prev = -1;
            for (; ; ) {
                int b = in.read();
                if (b < 0) {
                    throw new IOException("EOF while reading integer line");
                }
                if (!started) {
                    started = true;
                    if (b == '-') {
                        sign = -1;
                        prev = b;
                        continue;
                    }
                }
                if (prev == '\r' && b == '\n') {
                    return value * sign;
                }
                if (b == '\r') {
                    prev = b;
                    continue;
                }
                if (b < '0' || b > '9') {
                    throw new IOException("invalid digit in integer line: " + (char) b);
                }
                value = value * 10 + (b - '0');
                prev = b;
            }
        }

        private static void skipFully(InputStream in, long len) throws IOException {
            long remaining = len;
            byte[] buf = TL_SKIP_BUF.get();
            while (remaining > 0) {
                int n = in.read(buf, 0, (int) Math.min(buf.length, remaining));
                if (n < 0) {
                    throw new IOException("EOF while skipping payload");
                }
                remaining -= n;
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
        ThroughputResult getThroughput;
        LatencyResult pingLatency;
        LatencyResult setLatency;
        LatencyResult getLatency;

        BackendResult(String backend, int port) {
            this.backend = backend;
            this.port = port;
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

    private YierdisBench() {
    }
}
