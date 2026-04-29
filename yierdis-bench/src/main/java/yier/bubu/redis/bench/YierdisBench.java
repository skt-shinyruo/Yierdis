package yier.bubu.redis.bench;

import picocli.CommandLine;
import picocli.CommandLine.ParameterException;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.args.YierdisServerArgs;
import yier.bubu.redis.protocol.v1.CustomProtocolV1ReplyInspector;
import yier.bubu.redis.protocol.v1.CustomProtocolV1RequestEncoder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
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
 * - 使用本地 TCP + 自定义协议 v1，避免“进程内直连”偏离真实网络路径
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

    private static final int CONNECT_TIMEOUT_MILLIS = 1000;
    private static final int READY_TIMEOUT_MILLIS = 15_000;

    private static final DecimalFormat DF = new DecimalFormat("0.000");

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

        YierdisServerArgs baseServerArgs = new YierdisServerArgs();
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

        BenchConfig config = BenchConfig.from(benchArgs, baseServerArgs);

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
                YierdisServerArgs serverArgsForRun = config.baseServerArgs.copy();
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
            } finally {
                if (server != null) {
                    server.stop();
                }
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
        Path target = Path.of("yierdis-app", "yierdis-server-app", "target");
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
                    try (CustomCommandWriter w = new CustomCommandWriter(out);
                         JsonReplyReader reader = new JsonReplyReader(in)) {
                        w.writePing();
                        out.flush();
                        JsonReplyReader.Line line = reader.readLine();
                        return line != null && line.len() > 0 && startsWith(line.bytes(), line.len(), OK_PREFIX);
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

    static String renderSummary(List<BackendResult> results, boolean skipLatency) {
        StringBuilder sb = new StringBuilder();
        String header = skipLatency
                ? String.format("%-8s | %12s | %8s | %12s | %8s", "backend", "SET_QPS", "SET_ERR", "GET_QPS", "GET_ERR")
                : String.format(
                "%-8s | %12s | %8s | %12s | %8s | %14s | %8s | %14s | %8s | %14s | %8s",
                "backend", "SET_QPS", "SET_ERR", "GET_QPS", "GET_ERR",
                "PING_p95(ms)", "PING_E", "SET_p95(ms)", "SET_E", "GET_p95(ms)", "GET_E"
        );
        sb.append(header).append('\n');
        sb.append(repeat('-', header.length())).append('\n');

        for (BackendResult r : results) {
            String setQps = r.setThroughput == null ? "-" : DF.format(r.setThroughput.qps);
            String getQps = r.getThroughput == null ? "-" : DF.format(r.getThroughput.qps);
            String setErr = r.setThroughput == null ? "-" : Long.toString(r.setThroughput.errors);
            String getErr = r.getThroughput == null ? "-" : Long.toString(r.getThroughput.errors);
            if (skipLatency) {
                sb.append(String.format("%-8s | %12s | %8s | %12s | %8s", r.backend, setQps, setErr, getQps, getErr))
                        .append('\n');
                continue;
            }
            String pingP95 = r.pingLatency == null ? "-" : DF.format(r.pingLatency.stats.p95Millis());
            String setP95 = r.setLatency == null ? "-" : DF.format(r.setLatency.stats.p95Millis());
            String getP95 = r.getLatency == null ? "-" : DF.format(r.getLatency.stats.p95Millis());
            String pingErr = r.pingLatency == null ? "-" : Long.toString(r.pingLatency.errors);
            String setLatErr = r.setLatency == null ? "-" : Long.toString(r.setLatency.errors);
            String getLatErr = r.getLatency == null ? "-" : Long.toString(r.getLatency.errors);
            sb.append(String.format(
                    "%-8s | %12s | %8s | %12s | %8s | %14s | %8s | %14s | %8s | %14s | %8s",
                    r.backend, setQps, setErr, getQps, getErr,
                    pingP95, pingErr, setP95, setLatErr, getP95, getLatErr
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

    private static final byte[] OK_PREFIX = "{\"ok\":true,\"result\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ERR_PREFIX = "{\"ok\":false,\"error\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PONG = "PONG".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] OK = "OK".getBytes(StandardCharsets.US_ASCII);

    static boolean validateStrictReply(Workload workload, byte[] line, int lineLen, int expectedDataSize) {
        if (workload == null || line == null || lineLen <= 0) {
            return false;
        }
        switch (workload) {
            case PING:
                return CustomProtocolV1ReplyInspector.matchesOkAsciiStringResult(line, 0, lineLen, PONG);
            case SET_RANDOM:
            case SET_SEQUENTIAL:
                return CustomProtocolV1ReplyInspector.matchesOkAsciiStringResult(line, 0, lineLen, OK);
            case GET_RANDOM: {
                int decodedLen = CustomProtocolV1ReplyInspector.decodedOkResultByteLength(line, 0, lineLen);
                if (decodedLen == CustomProtocolV1ReplyInspector.INVALID_RESULT) {
                    return false;
                }
                if (decodedLen == CustomProtocolV1ReplyInspector.NULL_RESULT) {
                    return true;
                }
                return expectedDataSize < 0 || decodedLen == expectedDataSize;
            }
            default:
                return true;
        }
    }

    private static boolean isErrorLine(byte[] line, int lineLen) {
        return startsWith(line, lineLen, ERR_PREFIX);
    }

    private static boolean startsWith(byte[] line, int lineLen, byte[] prefix) {
        if (line == null || prefix == null) {
            return false;
        }
        if (lineLen < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (line[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    enum Workload {
        PING,
        SET_RANDOM,
        SET_SEQUENTIAL,
        GET_RANDOM
    }

    static final class BenchConfig {
        final boolean noStartServer;
        final Path serverJar;
        final List<String> backends;
        final String host;
        final int portBase;

        final String javaCmd;
        final String serverXms;
        final String serverXmx;
        final String serverMaxDirectMemory;
        final YierdisServerArgs baseServerArgs;

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

        private BenchConfig(
                boolean noStartServer,
                Path serverJar,
                List<String> backends,
                String host,
                int portBase,
                String javaCmd,
                String serverXms,
                String serverXmx,
                String serverMaxDirectMemory,
                YierdisServerArgs baseServerArgs,
                int keyspace,
                int dataSize,
                int requests,
                int clients,
                int pipeline,
                int latencyRequests,
                int latencyClients,
                boolean skipPrefill,
                boolean skipLatency,
                boolean strictReplies
        ) {
            this.noStartServer = noStartServer;
            this.serverJar = serverJar;
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
        }

        static BenchConfig from(YierdisBenchArgs args, YierdisServerArgs baseServerArgs) {
            Objects.requireNonNull(args, "args");
            Objects.requireNonNull(baseServerArgs, "baseServerArgs");

            if (args.serverJar != null && !Files.isRegularFile(args.serverJar)) {
                throw new IllegalArgumentException("serverJar 不存在: " + args.serverJar.toAbsolutePath());
            }

            List<String> backends = args.noStartServer ? List.of("external") : DEFAULT_BACKENDS;

            return new BenchConfig(
                    args.noStartServer,
                    args.serverJar,
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
                    args.strictReplies
            );
        }
    }

    static final class ServerProcess {
        private final String javaCmd;
        private final Path serverJar;
        private final String xms;
        private final String xmx;
        private final String maxDirectMemory;
        private final YierdisServerArgs serverArgs;
        private final Path logFile;

        private Process process;

        ServerProcess(
                String javaCmd,
                Path serverJar,
                String xms,
                String xmx,
                String maxDirectMemory,
                YierdisServerArgs serverArgs,
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

            List<String> cmd = new ArrayList<>();
            cmd.add(javaCmd);
            cmd.add("-Xms" + xms);
            cmd.add("-Xmx" + xmx);
            cmd.add("-XX:MaxDirectMemorySize=" + maxDirectMemory);
            cmd.add("-jar");
            cmd.add(serverJar.toAbsolutePath().toString());
            cmd.addAll(serverArgs.toArgv());

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
                    try (CustomCommandWriter writer = new CustomCommandWriter(out);
                         JsonReplyReader reader = new JsonReplyReader(in)) {
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
                                JsonReplyReader.Line reply = reader.readLine();
                                if (isErrorLine(reply.bytes(), reply.len())) {
                                    errors++;
                                } else if (strictReplies && !validateStrictReply(workload, reply.bytes(), reply.len(), value.length)) {
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

            try (Socket socket = new Socket()) {
                socket.setTcpNoDelay(true);
                socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
                try (BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
                     BufferedInputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024)) {
                    try (CustomCommandWriter writer = new CustomCommandWriter(out);
                         JsonReplyReader reader = new JsonReplyReader(in)) {
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
                            JsonReplyReader.Line reply = reader.readLine();
                            if (isErrorLine(reply.bytes(), reply.len())) {
                                errors++;
                            } else if (strictReplies && !validateStrictReply(workload, reply.bytes(), reply.len(), value.length)) {
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

    static final class CustomCommandWriter implements AutoCloseable {
        private static final byte[] CMD_PING = "PING".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] CMD_GET = "GET".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] CMD_SET = "SET".getBytes(StandardCharsets.US_ASCII);
        private static final byte[] FRAME_PING = CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(CMD_PING));

        private final OutputStream out;
        private final BytesSink sink;
        private final byte[] intBuf = new byte[16];
        private final MutableRequestArgs requestArgs = new MutableRequestArgs();

        CustomCommandWriter(OutputStream out) {
            this.out = Objects.requireNonNull(out, "out");
            this.sink = (src, srcIndex, len) -> {
                try {
                    this.out.write(src, srcIndex, len);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
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

        private void writeFrame(List<byte[]> args) throws IOException {
            try {
                CustomProtocolV1RequestEncoder.writeRequestFrame(sink, args, intBuf);
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
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

    static final class JsonReplyReader implements AutoCloseable {
        private static final int READ_BUF_BYTES = 8 * 1024;
        private static final int DEFAULT_MAX_LINE_BYTES = 1024 * 1024; // 1 MiB

        private final InputStream in;
        private final byte[] readBuf = new byte[READ_BUF_BYTES];
        private int readPos;
        private int readLimit;

        private final int maxLineBytes;
        private final Line line = new Line();

        JsonReplyReader(InputStream in) {
            this(in, DEFAULT_MAX_LINE_BYTES);
        }

        JsonReplyReader(InputStream in, int maxLineBytes) {
            this.in = Objects.requireNonNull(in, "in");
            this.maxLineBytes = Math.max(0, maxLineBytes);
        }

        Line readLine() throws IOException {
            line.len = 0;
            for (; ; ) {
                if (readPos >= readLimit) {
                    int n = in.read(readBuf);
                    if (n < 0) {
                        throw new IOException("EOF");
                    }
                    if (n == 0) {
                        continue;
                    }
                    readPos = 0;
                    readLimit = n;
                }

                byte b = readBuf[readPos++];
                if (b == '\n') {
                    return line;
                }
                if (b == '\r') {
                    continue;
                }
                if (maxLineBytes > 0 && line.len + 1 > maxLineBytes) {
                    throw new IOException("line too long");
                }
                line.ensureCapacity(line.len + 1);
                line.bytes[line.len++] = b;
            }
        }

        @Override
        public void close() {
            // no-op
        }

        static final class Line {
            private byte[] bytes = new byte[256];
            private int len;

            byte[] bytes() {
                return bytes;
            }

            int len() {
                return len;
            }

            private void ensureCapacity(int desired) {
                if (bytes.length >= desired) {
                    return;
                }
                int next = bytes.length;
                while (next < desired) {
                    next = next <= 0 ? 256 : Math.min(Integer.MAX_VALUE / 2, next * 2);
                }
                bytes = Arrays.copyOf(bytes, next);
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
