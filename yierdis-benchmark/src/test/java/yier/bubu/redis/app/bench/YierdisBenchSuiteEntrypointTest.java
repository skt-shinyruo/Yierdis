package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class YierdisBenchSuiteEntrypointTest {
    @Test
    public void suiteModeValidatesConfigAndStopsBeforeNormalBenchmarkPath() throws Exception {
        Path current = regularTempJar("current");

        Captured captured = captureErr(() -> YierdisBench.main(
                new String[]{"--suite", "--currentServerJar", current.toString(), "--suiteProfile", "release"}
        ));

        Assert.assertTrue(captured.err(), captured.err().contains("suite runner is not implemented yet"));
        Assert.assertFalse(captured.err(), captured.err().contains("serverJar is required"));
    }

    @Test
    public void suiteModeReportsSuiteValidationErrorsAtEntrypoint() throws Exception {
        Captured captured = captureErr(() -> YierdisBench.main(new String[]{"--suite"}));

        Assert.assertTrue(captured.err(), captured.err().contains("suite requires currentServerJar"));
        Assert.assertTrue(captured.err(), captured.err().contains("--suite"));
    }

    @Test
    public void suiteModeReportsSuiteSpecificValidationAfterValidCurrentJar() throws Exception {
        Path current = regularTempJar("current");

        Captured captured = captureErr(() -> YierdisBench.main(
                new String[]{"--suite", "--currentServerJar", current.toString(), "--comparisonMode"}
        ));

        Assert.assertTrue(captured.err(), captured.err().contains("suite does not support comparisonMode"));
        Assert.assertFalse(captured.err(), captured.err().contains("suite runner is not implemented yet"));
        Assert.assertFalse(captured.err(), captured.err().contains("suite requires currentServerJar"));
    }

    private static Captured captureErr(ThrowingRunnable runnable) throws Exception {
        PrintStream originalErr = System.err;
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        try (PrintStream capture = new PrintStream(err, true, StandardCharsets.UTF_8)) {
            System.setErr(capture);
            runnable.run();
        } finally {
            System.setErr(originalErr);
        }
        return new Captured(err.toString(StandardCharsets.UTF_8));
    }

    private static Path regularTempJar(String prefix) throws Exception {
        Path jar = Files.createTempFile(prefix, ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }

    private record Captured(String err) {
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
