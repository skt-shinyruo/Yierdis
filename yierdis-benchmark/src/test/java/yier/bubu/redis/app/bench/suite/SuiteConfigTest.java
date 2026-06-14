package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.app.bench.YierdisBenchArgs;
import yier.bubu.redis.app.bench.YierdisBenchServerArgs;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class SuiteConfigTest {
    @Test
    public void suiteRequiresCurrentJarAndAcceptsOptionalBaselineJar() throws Exception {
        Path current = regularTempJar("current");
        Path baseline = regularTempJar("baseline");

        SuiteConfig currentOnly = config(
                "--suite",
                "--currentServerJar", current.toString(),
                "--suiteProfile", "release"
        );

        Assert.assertEquals(SuiteProfileName.RELEASE, currentOnly.profile());
        Assert.assertEquals(current, currentOnly.current().jarPath());
        Assert.assertFalse(currentOnly.baseline().isPresent());
        Assert.assertTrue(currentOnly.reportDir().toString().contains("target/benchmark-reports"));
        Assert.assertEquals("current", currentOnly.current().label());

        SuiteConfig comparison = config(
                "--suite",
                "--currentServerJar", current.toString(),
                "--baselineServerJar", baseline.toString(),
                "--suiteProfile", "full"
        );

        Assert.assertEquals(SuiteProfileName.FULL, comparison.profile());
        Assert.assertEquals(List.of("baseline", "current"), comparison.artifactLabels());
        Assert.assertEquals(baseline, comparison.baseline().orElseThrow().jarPath());
    }

    @Test
    public void suiteRejectsInvalidModeCombinations() throws Exception {
        Path current = regularTempJar("current");
        Path single = regularTempJar("single");

        assertRejects("suite requires currentServerJar", "--suite");
        assertRejects("suite does not support serverJar",
                "--suite", "--currentServerJar", current.toString(), "--serverJar", single.toString());
        assertRejects("suite does not support noStartServer",
                "--suite", "--currentServerJar", current.toString(), "--noStartServer");
        assertRejects("suite does not support comparisonMode",
                "--suite", "--currentServerJar", current.toString(), "--comparisonMode");
        assertRejects("suite does not support nativeEval",
                "--suite", "--currentServerJar", current.toString(), "--nativeEval");
    }

    @Test
    public void suiteRejectsMissingJarAndBadReportDir() throws Exception {
        Path missing = Files.createTempDirectory("suite-missing-").resolve("server.jar");
        IllegalArgumentException missingJar = assertRejects(
                "currentServerJar",
                "--suite",
                "--currentServerJar", missing.toString()
        );
        Assert.assertTrue(missingJar.getMessage().contains(missing.toAbsolutePath().toString()));

        Path current = regularTempJar("current");
        Path reportFile = Files.createTempFile("suite-report", ".txt");
        assertRejects("reportDir must be a directory",
                "--suite", "--currentServerJar", current.toString(), "--reportDir", reportFile.toString());
    }

    @Test
    public void profileParsingIsCaseInsensitiveAndRejectsUnknownNames() throws Exception {
        Path current = regularTempJar("current");

        Assert.assertEquals(
                SuiteProfileName.RELEASE,
                config("--suite", "--currentServerJar", current.toString(), "--suiteProfile", "ReLeAsE").profile()
        );
        Assert.assertEquals(
                SuiteProfileName.FULL,
                config("--suite", "--currentServerJar", current.toString(), "--suiteProfile", "FULL").profile()
        );

        assertRejects("suiteProfile must be one of",
                "--suite", "--currentServerJar", current.toString(), "--suiteProfile", "nightly");
    }

    private static SuiteConfig config(String... argv) {
        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs(argv);
        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        return SuiteConfig.from(args, serverArgs);
    }

    private static IllegalArgumentException assertRejects(String messagePart, String... argv) {
        try {
            config(argv);
            Assert.fail("expected rejection containing " + messagePart);
            return null;
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
            return e;
        }
    }

    private static Path regularTempJar(String prefix) throws Exception {
        Path jar = Files.createTempFile(prefix, ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }
}
