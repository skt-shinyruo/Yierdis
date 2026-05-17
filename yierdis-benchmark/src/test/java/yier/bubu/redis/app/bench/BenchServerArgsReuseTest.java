package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

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
    public void benchConfigDefaultsToSingleForeignRun() {
        YierdisBenchServerArgs baseServerArgs = parseServerArgs(
                "--port", "6381",
                "--maxmemoryScope", "Per_Db"
        );
        baseServerArgs.normalizeAndValidate();

        YierdisBench.BenchConfig config = YierdisBench.BenchConfig.from(new YierdisBenchArgs(), baseServerArgs);

        Assert.assertEquals(List.of("foreign"), config.backends);
    }

    @Test
    public void serverProcessUsesNormalizedArgvFromLaunchCopy() throws Exception {
        YierdisBenchServerArgs baseServerArgs = parseServerArgs(
                "--port", "6381",
                "--noCleanup",
                "--executorSchedulingPolicy", "GLOBAL",
                "--maxmemoryScope", "Per_Db",
                "--maxmemoryPolicy", "ALLKEYS-LRU",
                "--nativeDefragEnabled",
                "--nativeDefragMaxMoveBytes", "1024",
                "--nativeDefragMaxObjects", "7",
                "--nativeDefragTimeLimitMillis", "3",
                "--keysTimeBudgetMillis", "0",
                "--keysMaxResults", "0"
        );
        baseServerArgs.normalizeAndValidate();

        YierdisBenchArgs benchArgs = new YierdisBenchArgs();
        benchArgs.portBase = 17380;

        YierdisBench.BenchConfig config = YierdisBench.BenchConfig.from(benchArgs, baseServerArgs);
        Assert.assertEquals(List.of("foreign"), config.backends);

        YierdisBenchServerArgs serverArgsForRun = config.baseServerArgs.copy();
        serverArgsForRun.port = config.portBase;
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

        List<String> expected = new ArrayList<>();
        expected.add("-Xms4g");
        expected.add("-Xmx4g");
        expected.add("-XX:MaxDirectMemorySize=6g");
        expected.add("-jar");
        expected.add(fakeJar.toAbsolutePath().toString());
        expected.addAll(serverArgsForRun.toArgv());
        List<String> commandLine = new ArrayList<>();
        commandLine.add(script.toAbsolutePath().toString());
        commandLine.addAll(expected);
        Assert.assertEquals(commandLine, process.commandLine());

        try {
            process.start();

            List<String> actual = waitForLines(logFile, expected.size());
            Assert.assertEquals(expected, actual);
            assertArgValue(actual, "--executorSchedulingPolicy", "global");
            assertArgValue(actual, "--maxmemoryScope", "per-db");
            assertArgValue(actual, "--maxmemoryPolicy", "allkeys-lru");
            assertArgValue(actual, "--nativeDefragMaxMoveBytes", "1024");
            assertArgValue(actual, "--nativeDefragMaxObjects", "7");
            assertArgValue(actual, "--nativeDefragTimeLimitMillis", "3");
            Assert.assertTrue(actual.contains("--nativeDefragEnabled"));
            Assert.assertTrue(actual.contains("--noCleanup"));
            Assert.assertFalse(actual.contains("--offheapBackend"));
            Assert.assertFalse(actual.contains("--offheapMaxBytes"));
        } finally {
            process.stop();
        }
    }

    private static YierdisBenchServerArgs parseServerArgs(String... argv) {
        YierdisBenchServerArgs args = new YierdisBenchServerArgs();
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
