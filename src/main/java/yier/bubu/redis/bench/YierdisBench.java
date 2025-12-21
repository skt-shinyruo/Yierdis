package yier.bubu.redis.bench;

import yier.bubu.redis.db.YierdisDb;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * A tiny, repeatable workload driver used to measure object counts and GC behavior before/after optimizations.
 * <p>
 * This runs in-process (no network) to focus measurements on the DB/value representation.
 */
public final class YierdisBench {
    private enum Scenario {
        STRINGS_1E6,
        HASHES_1E5_X10,
        LISTS_1E5_SMALL,
        ZSETS_1E5_SMALL
    }

    public static void main(String[] args) throws Exception {
        Config cfg = Config.parse(args);
        Scenario scenario = cfg.scenario;

        System.out.println("yierdis-bench");
        System.out.println("pid=" + ProcessHandle.current().pid());
        System.out.println("scenario=" + scenario.name().toLowerCase(Locale.ROOT));
        System.out.println("seed=" + cfg.seed);

        YierdisDb db = new YierdisDb();

        Instant start = Instant.now();
        switch (scenario) {
            case STRINGS_1E6 -> runStrings1e6(db, cfg);
            case HASHES_1E5_X10 -> runHashes1e5x10(db, cfg);
            case LISTS_1E5_SMALL -> runLists1e5Small(db, cfg);
            case ZSETS_1E5_SMALL -> runZSets1e5Small(db, cfg);
        }
        Instant end = Instant.now();

        System.out.println("elapsed=" + Duration.between(start, end).toMillis() + "ms");
        System.out.println("dbKeys=" + db.size());

        if (cfg.readyFile != null) {
            writeReadyFile(cfg.readyFile, scenario, cfg);
            System.out.println("readyFile=" + cfg.readyFile.toAbsolutePath());
        }

        System.out.println();
        System.out.println("Now you can run:");
        System.out.println("  jcmd " + ProcessHandle.current().pid() + " GC.class_histogram");
        System.out.println("  jcmd " + ProcessHandle.current().pid() + " GC.heap_info");
        System.out.println("  jcmd " + ProcessHandle.current().pid() + " VM.native_memory summary");
        System.out.println();

        hold(cfg.holdMillis);
    }

    private static void runStrings1e6(YierdisDb db, Config cfg) {
        int n = cfg.count;
        if (n <= 0) {
            n = 1_000_000;
        }

        for (int i = 0; i < n; i++) {
            byte[] key = asciiKey("k", i, 6);
            byte[] value = asciiLong(cfg.seed + i);
            db.setString(key, value, YierdisDb.SetMode.NORMAL, null);
        }
    }

    private static void runHashes1e5x10(YierdisDb db, Config cfg) {
        int n = cfg.count;
        if (n <= 0) {
            n = 100_000;
        }

        List<byte[]> fieldValuePairs = new ArrayList<>(20);
        for (int i = 0; i < 10; i++) {
            fieldValuePairs.add(asciiKey("f", i, 2));
            fieldValuePairs.add(asciiKey("v", i, 2));
        }

        for (int i = 0; i < n; i++) {
            byte[] key = asciiKey("h", i, 6);
            db.hset(key, fieldValuePairs);
        }
    }

    private static void runLists1e5Small(YierdisDb db, Config cfg) {
        int n = cfg.count;
        if (n <= 0) {
            n = 100_000;
        }

        List<byte[]> values = Arrays.asList(
                bytes("a"),
                bytes("b"),
                bytes("c"),
                bytes("d"),
                bytes("e"),
                bytes("f"),
                bytes("g"),
                bytes("h")
        );

        for (int i = 0; i < n; i++) {
            byte[] key = asciiKey("l", i, 6);
            db.rpush(key, values);
        }
    }

    private static void runZSets1e5Small(YierdisDb db, Config cfg) {
        int n = cfg.count;
        if (n <= 0) {
            n = 100_000;
        }

        List<byte[]> scoreMemberPairs = new ArrayList<>(16);
        for (int i = 0; i < 8; i++) {
            scoreMemberPairs.add(bytes(Integer.toString(i)));
            scoreMemberPairs.add(asciiKey("m", i, 2));
        }

        for (int i = 0; i < n; i++) {
            byte[] key = asciiKey("z", i, 6);
            db.zadd(key, scoreMemberPairs);
        }
    }

    private static void hold(long holdMillis) throws IOException {
        if (holdMillis <= 0) {
            System.out.println("hold=off (exiting)");
            return;
        }

        System.out.println("holdMillis=" + holdMillis);
        System.out.println("holding... (Ctrl+C to stop)");
        try {
            Thread.sleep(holdMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void writeReadyFile(Path file, Scenario scenario, Config cfg) throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        String content = "pid=" + ProcessHandle.current().pid() + "\n"
                + "scenario=" + scenario.name().toLowerCase(Locale.ROOT) + "\n"
                + "seed=" + cfg.seed + "\n";
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static byte[] asciiKey(String prefix, int i, int width) {
        String s = prefix + String.format(Locale.ROOT, "%0" + width + "d", i);
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] asciiLong(long v) {
        return Long.toString(v).getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class Config {
        final Scenario scenario;
        final int count;
        final long seed;
        final long holdMillis;
        final Path readyFile;

        private Config(Scenario scenario, int count, long seed, long holdMillis, Path readyFile) {
            this.scenario = scenario;
            this.count = count;
            this.seed = seed;
            this.holdMillis = holdMillis;
            this.readyFile = readyFile;
        }

        static Config parse(String[] args) {
            Scenario scenario = Scenario.STRINGS_1E6;
            int count = 0;
            long seed = 1L;
            long holdMillis = 600_000L;
            Path readyFile = null;

            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                switch (a) {
                    case "--scenario" -> {
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException("--scenario requires a value");
                        }
                        scenario = parseScenario(args[++i]);
                    }
                    case "--count" -> {
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException("--count requires a value");
                        }
                        count = Integer.parseInt(args[++i]);
                    }
                    case "--seed" -> {
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException("--seed requires a value");
                        }
                        seed = Long.parseLong(args[++i]);
                    }
                    case "--holdMillis" -> {
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException("--holdMillis requires a value");
                        }
                        holdMillis = Long.parseLong(args[++i]);
                    }
                    case "--readyFile" -> {
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException("--readyFile requires a value");
                        }
                        readyFile = Path.of(args[++i]);
                    }
                    case "--help" -> {
                        printHelpAndExit();
                    }
                    default -> throw new IllegalArgumentException("Unknown arg: " + a);
                }
            }
            return new Config(scenario, count, seed, holdMillis, readyFile);
        }

        private static Scenario parseScenario(String s) {
            String normalized = s.trim().toLowerCase(Locale.ROOT);
            return switch (normalized) {
                case "strings", "strings_1e6" -> Scenario.STRINGS_1E6;
                case "hashes", "hashes_1e5x10" -> Scenario.HASHES_1E5_X10;
                case "lists", "lists_1e5_small" -> Scenario.LISTS_1E5_SMALL;
                case "zsets", "zsets_1e5_small" -> Scenario.ZSETS_1E5_SMALL;
                default -> throw new IllegalArgumentException("Unknown scenario: " + s);
            };
        }

        private static void printHelpAndExit() {
            System.out.println("Usage: yier.bubu.redis.bench.YierdisBench [options]");
            System.out.println("  --scenario <strings|hashes|lists|zsets>  Default: strings");
            System.out.println("  --count <n>                              Override default sizes");
            System.out.println("  --seed <n>                               Seed for deterministic values");
            System.out.println("  --holdMillis <ms>                        Keep JVM alive for jcmd/JFR (default: 600000)");
            System.out.println("  --readyFile <path>                       Write a small file after load completes");
            System.exit(0);
        }
    }
}
