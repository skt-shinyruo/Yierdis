package yier.bubu.redis.app.bench;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Unmatched;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Command(
        name = "yierdis-benchmark",
        description = "Pure Java benchmark tool for Yierdis (RESP over TCP).",
        sortOptions = false,
        usageHelpAutoWidth = true
)
public final class YierdisBenchArgs {
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    public boolean help;

    @Option(names = "--host", defaultValue = "127.0.0.1", description = "Target host to connect.")
    public String host = "127.0.0.1";

    @Option(
            names = "--portBase",
            defaultValue = "16378",
            description = "Port for the auto-started server. In connect-only mode, this is the target port."
    )
    public int portBase = 16378;

    @Option(names = "--noStartServer", description = "Do not start server process; benchmark an already running server.")
    public boolean noStartServer;

    @Option(names = "--serverJar", description = "Path to yierdis server jar (only used when starting server).")
    public Path serverJar;

    @Option(names = "--comparisonMode", description = "Run a jar-only baseline/current comparison.")
    public boolean comparisonMode;

    @Option(names = "--baselineServerJar", description = "Path to the baseline server jar used in comparison or suite mode.")
    public Path baselineServerJar;

    @Option(names = "--currentServerJar", description = "Path to the current server jar used in comparison or suite mode.")
    public Path currentServerJar;

    @Option(names = "--suite", description = "Run the release-grade benchmark suite.")
    public boolean suite;

    @Option(names = "--suiteProfile", defaultValue = "release", description = "Suite profile: release|full.")
    public String suiteProfile = "release";

    @Option(names = "--reportDir", description = "Directory for suite JSON, CSV, and Markdown report artifacts.")
    public Path reportDir;

    @Option(names = "--includeRedis", description = "Include an external Redis target in suite mode.")
    public boolean includeRedis;

    @Option(names = "--redisHost", defaultValue = "127.0.0.1", description = "External Redis host for suite comparison.")
    public String redisHost = "127.0.0.1";

    @Option(names = "--redisPort", defaultValue = "6379", description = "External Redis port for suite comparison.")
    public int redisPort = 6379;

    @Option(names = "--redisLabel", defaultValue = "redis", description = "Artifact label used for external Redis in reports.")
    public String redisLabel = "redis";

    @Option(names = "--redisUser", description = "Optional ACL username for external Redis in suite mode.")
    public String redisUser;

    @Option(names = "--redisAuth", description = "Optional password for external Redis in suite mode.")
    public String redisAuth;

    @Option(names = "--redisDb", defaultValue = "0", description = "Logical DB selected on external Redis before each suite pass.")
    public int redisDb = 0;

    @Option(names = "--javaCmd", defaultValue = "java", description = "Java command used to start server (only used when starting server).")
    public String javaCmd = "java";

    @Option(names = "--xms", defaultValue = "4g", description = "Server JVM -Xms (only used when starting server).")
    public String xms = "4g";

    @Option(names = "--xmx", defaultValue = "4g", description = "Server JVM -Xmx (only used when starting server).")
    public String xmx = "4g";

    @Option(names = "--maxDirectMemory", defaultValue = "6g", description = "Server JVM -XX:MaxDirectMemorySize (only used when starting server).")
    public String maxDirectMemory = "6g";

    @Option(names = "--keyspace", defaultValue = "1000000", description = "Keyspace size.")
    public int keyspace = 1_000_000;

    @Option(names = "--dataSize", defaultValue = "256", description = "Value bytes for SET workload.")
    public int dataSize = 256;

    @Option(names = "--requests", defaultValue = "1000000", description = "Total requests for throughput workload.")
    public int requests = 1_000_000;

    @Option(names = "--clients", defaultValue = "200", description = "Concurrent clients for throughput workload.")
    public int clients = 200;

    @Option(names = "--pipeline", defaultValue = "16", description = "Pipeline depth for throughput workload.")
    public int pipeline = 16;

    @Option(names = "--latencyRequests", defaultValue = "200000", description = "Requests for latency workload.")
    public int latencyRequests = 200_000;

    @Option(names = "--latencyClients", defaultValue = "50", description = "Concurrent clients for latency workload.")
    public int latencyClients = 50;

    @Option(names = "--skipPrefill", description = "Skip prefill stage.")
    public boolean skipPrefill;

    @Option(names = "--skipLatency", description = "Skip latency stage.")
    public boolean skipLatency;

    @Option(names = "--strictReplies", description = "Enable minimal reply semantic validation.")
    public boolean strictReplies;

    @Option(names = "--skipNativeDefragCompare", description = "Skip focused DB native defrag disabled/enabled p99 comparison.")
    public boolean skipNativeDefragCompare;

    @Option(names = "--nativeEval", description = "Run in-process native allocator evaluation and do not start or connect to a server.")
    public boolean nativeEval;

    @Option(names = "--nativeEvalIterations", defaultValue = "10000", description = "Iterations per native allocator evaluation scenario.")
    public int nativeEvalIterations = 10_000;

    @Unmatched
    public List<String> serverArgs = new ArrayList<>();
}
