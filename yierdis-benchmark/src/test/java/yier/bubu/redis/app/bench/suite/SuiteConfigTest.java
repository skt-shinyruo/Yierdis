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
        Path currentRelative = Path.of(".").toAbsolutePath().relativize(current.toAbsolutePath()).normalize();
        Path baselineRelative = Path.of(".").toAbsolutePath().relativize(baseline.toAbsolutePath()).normalize();

        SuiteConfig currentOnly = config(
                "--suite",
                "--currentServerJar", currentRelative.toString(),
                "--suiteProfile", "release"
        );

        Assert.assertEquals(SuiteProfileName.RELEASE, currentOnly.profile());
        Assert.assertEquals(current.toAbsolutePath().normalize(), currentOnly.current().jarPath());
        Assert.assertFalse(currentOnly.baseline().isPresent());
        Assert.assertEquals(Path.of("target", "benchmark-reports").toAbsolutePath().normalize(), currentOnly.reportDir().getParent());
        Assert.assertTrue(currentOnly.reportDir().getFileName().toString().endsWith("-release"));
        Assert.assertEquals("current", currentOnly.current().label());
        Assert.assertTrue(currentOnly.strictReplies());

        SuiteConfig comparison = config(
                "--suite",
                "--currentServerJar", currentRelative.toString(),
                "--baselineServerJar", baselineRelative.toString(),
                "--suiteProfile", "full"
        );

        Assert.assertEquals(SuiteProfileName.FULL, comparison.profile());
        Assert.assertEquals(List.of("baseline", "current"), comparison.artifactLabels());
        Assert.assertEquals(baseline.toAbsolutePath().normalize(), comparison.baseline().orElseThrow().jarPath());

        Path suppliedReportDir = Files.createTempDirectory("suite-report-dir-");
        SuiteConfig suppliedReport = config(
                "--suite",
                "--currentServerJar", current.toString(),
                "--reportDir", suppliedReportDir.toString()
        );
        Assert.assertEquals(suppliedReportDir.toAbsolutePath().normalize(), suppliedReport.reportDir());
        Assert.assertTrue(suppliedReport.reportDir().isAbsolute());
    }

    @Test
    public void suiteIncludeRedisBuildsExternalRedisArtifact() throws Exception {
        Path current = regularTempJar("current");

        SuiteConfig config = config(
                "--suite",
                "--includeRedis",
                "--redisHost", "127.0.0.1",
                "--redisPort", "6379",
                "--currentServerJar", current.toString()
        );

        Assert.assertEquals(List.of("redis", "current"), config.artifactLabels());
        Assert.assertEquals(SuiteArtifact.Kind.EXTERNAL_REDIS, config.artifactsInRunOrder().get(0).kind());
        Assert.assertEquals("127.0.0.1", config.artifactsInRunOrder().get(0).host());
        Assert.assertEquals(6379, config.artifactsInRunOrder().get(0).port());
    }

    @Test
    public void suiteIncludeRedisRejectsBaselineJarCombination() throws Exception {
        Path current = regularTempJar("current");
        Path baseline = regularTempJar("baseline");

        IllegalArgumentException rejected = assertRejects(
                "--suite",
                "--includeRedis",
                "--baselineServerJar", baseline.toString(),
                "--currentServerJar", current.toString()
        );

        Assert.assertTrue(rejected.getMessage().contains("baselineServerJar"));
        Assert.assertTrue(rejected.getMessage().contains("includeRedis"));
    }

    @Test
    public void suiteCopiesBaseServerArgs() throws Exception {
        Path current = regularTempJar("current");
        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs("--suite", "--currentServerJar", current.toString());

        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.port = 17378;
        serverArgs.normalizeAndValidate();

        SuiteConfig config = SuiteConfig.from(args, serverArgs);
        serverArgs.port = 17379;

        Assert.assertNotSame(serverArgs, config.baseServerArgs());
        Assert.assertEquals(17378, config.baseServerArgs().port);

        YierdisBenchServerArgs constructorArgs = new YierdisBenchServerArgs();
        constructorArgs.port = 18378;
        SuiteConfig constructed = new SuiteConfig(
                SuiteProfileName.RELEASE,
                new SuiteArtifact("current", current, ""),
                java.util.Optional.empty(),
                List.of(new SuiteArtifact("current", current, "")),
                Files.createTempDirectory("suite-constructor-report-"),
                "127.0.0.1",
                16378,
                "java",
                "4g",
                "4g",
                "6g",
                constructorArgs,
                true
        );
        constructorArgs.port = 18379;

        Assert.assertNotSame(constructorArgs, constructed.baseServerArgs());
        Assert.assertEquals(18378, constructed.baseServerArgs().port);
    }

    @Test
    public void suiteBaseServerArgsAccessorReturnsCopy() throws Exception {
        Path current = regularTempJar("current");
        SuiteConfig config = config("--suite", "--currentServerJar", current.toString());

        YierdisBenchServerArgs firstAccess = config.baseServerArgs();
        firstAccess.port = 19379;

        YierdisBenchServerArgs secondAccess = config.baseServerArgs();
        Assert.assertNotSame(firstAccess, secondAccess);
        Assert.assertEquals(6378, secondAccess.port);
    }

    @Test
    public void suiteRejectsInvalidModeCombinations() throws Exception {
        Path current = regularTempJar("current");
        Path single = regularTempJar("single");

        assertRejectsWithMessage("suite requires currentServerJar", "--suite");
        assertRejectsWithMessage("suite does not support serverJar",
                "--suite", "--currentServerJar", current.toString(), "--serverJar", single.toString());
        assertRejectsWithMessage("suite does not support noStartServer",
                "--suite", "--currentServerJar", current.toString(), "--noStartServer");
        assertRejectsWithMessage("suite does not support comparisonMode",
                "--suite", "--currentServerJar", current.toString(), "--comparisonMode");
        assertRejectsWithMessage("suite does not support nativeEval",
                "--suite", "--currentServerJar", current.toString(), "--nativeEval");
    }

    @Test
    public void suiteRejectsMissingJarAndBadReportDir() throws Exception {
        Path missing = Files.createTempDirectory("suite-missing-").resolve("server.jar");
        IllegalArgumentException missingJar = assertRejects(
                "--suite",
                "--currentServerJar", missing.toString()
        );
        Assert.assertTrue(missingJar.getMessage().contains("currentServerJar"));
        Assert.assertTrue(missingJar.getMessage().contains(missing.toAbsolutePath().toString()));

        Path current = regularTempJar("current");
        Path reportFile = Files.createTempFile("suite-report", ".txt");
        assertRejectsWithMessage("reportDir must be a directory",
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

        assertRejectsWithMessage("suiteProfile must be one of",
                "--suite", "--currentServerJar", current.toString(), "--suiteProfile", "nightly");
    }

    private static SuiteConfig config(String... argv) {
        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs(argv);
        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        return SuiteConfig.from(args, serverArgs);
    }

    private static IllegalArgumentException assertRejects(String... argv) {
        try {
            config(argv);
            Assert.fail("expected rejection");
            return null;
        } catch (IllegalArgumentException e) {
            return e;
        }
    }

    private static IllegalArgumentException assertRejectsWithMessage(String messagePart, String... argv) {
        IllegalArgumentException failure = assertRejects(argv);
        Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(messagePart));
        return failure;
    }

    private static Path regularTempJar(String prefix) throws Exception {
        Path jar = Files.createTempFile(prefix, ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }
}
