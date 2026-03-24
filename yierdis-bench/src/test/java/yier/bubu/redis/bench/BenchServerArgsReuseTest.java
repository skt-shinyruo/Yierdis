package yier.bubu.redis.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.args.YierdisServerArgs;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class BenchServerArgsReuseTest {
    @Test
    public void serverProcessUsesNormalizedArgvFromLaunchCopy() throws Exception {
        YierdisServerArgs baseServerArgs = parseServerArgs(
                "--port", "6381",
                "--noCleanup",
                "--executorSchedulingPolicy", "GLOBAL",
                "--offheapBackend", "UNSAFE",
                "--offheapMaxBytes", "2048",
                "--maxmemoryScope", "Per_Db",
                "--maxmemoryPolicy", "ALLKEYS-LRU",
                "--keysTimeBudgetMillis", "0",
                "--keysMaxResults", "0"
        );
        baseServerArgs.normalizeAndValidate();

        YierdisBenchArgs benchArgs = new YierdisBenchArgs();
        benchArgs.backends = "unsafe";
        benchArgs.portBase = 17380;

        YierdisBench.BenchConfig config = YierdisBench.BenchConfig.from(benchArgs, baseServerArgs);
        String backend = config.backends.get(0);

        YierdisServerArgs serverArgsForRun = config.baseServerArgs.copy();
        serverArgsForRun.port = config.portBase;
        serverArgsForRun.offheapBackend = backend;
        serverArgsForRun.normalizeAndValidate();

        Path tempDir = Files.createTempDirectory("bench-server-args-");
        Path script = tempDir.resolve("capture-argv.sh");
        Path fakeJar = tempDir.resolve("server.jar");
        Path logFile = tempDir.resolve("server.log");

        Files.writeString(script, "#!/bin/sh\nprintf '%s\\n' \"$@\"\n", StandardCharsets.US_ASCII);
        Files.setPosixFilePermissions(script, EnumSet.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE
        ));
        Files.writeString(fakeJar, "stub", StandardCharsets.US_ASCII);

        YierdisBench.ServerProcess process = new YierdisBench.ServerProcess(
                script.toAbsolutePath().toString(),
                fakeJar,
                "4g",
                "4g",
                "6g",
                serverArgsForRun,
                logFile
        );

        try {
            process.start();

            List<String> expected = new ArrayList<>();
            expected.add("-Xms4g");
            expected.add("-Xmx4g");
            expected.add("-XX:MaxDirectMemorySize=6g");
            expected.add("-jar");
            expected.add(fakeJar.toAbsolutePath().toString());
            expected.addAll(serverArgsForRun.toArgv());

            List<String> actual = waitForLines(logFile, expected.size());
            Assert.assertEquals(expected, actual);
            assertArgValue(actual, "--executorSchedulingPolicy", "global");
            assertArgValue(actual, "--offheapBackend", "unsafe");
            assertArgValue(actual, "--maxmemoryScope", "per-db");
            assertArgValue(actual, "--maxmemoryPolicy", "allkeys-lru");
            Assert.assertTrue(actual.contains("--noCleanup"));
        } finally {
            process.stop();
        }
    }

    private static YierdisServerArgs parseServerArgs(String... argv) {
        YierdisServerArgs args = new YierdisServerArgs();
        new CommandLine(args).parseArgs(argv);
        return args;
    }

    private static List<String> waitForLines(Path logFile, int expectedLines) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(5));
        while (Instant.now().isBefore(deadline)) {
            if (Files.isRegularFile(logFile)) {
                List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
                if (lines.size() >= expectedLines) {
                    return lines;
                }
            }
            Thread.sleep(25L);
        }
        Assert.fail("timed out waiting for log lines in " + logFile);
        return List.of();
    }

    private static void assertArgValue(List<String> argv, String flag, String expectedValue) {
        int index = argv.indexOf(flag);
        Assert.assertTrue("missing flag: " + flag, index >= 0);
        Assert.assertTrue("missing value after flag: " + flag, index + 1 < argv.size());
        Assert.assertEquals(expectedValue, argv.get(index + 1));
    }
}
