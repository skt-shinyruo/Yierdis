package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class BenchComparisonConfigTest {
    @Test
    public void comparisonModeRequiresBothJarFlags() throws Exception {
        Path baselineJar = regularTempJar("baseline");

        IllegalArgumentException missingCurrent = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString()
        );
        Assert.assertTrue(missingCurrent.getMessage().contains("currentServerJar"));

        Path currentJar = regularTempJar("current");
        IllegalArgumentException missingBaseline = assertRejects(
                "--comparisonMode",
                "--currentServerJar", currentJar.toString()
        );
        Assert.assertTrue(missingBaseline.getMessage().contains("baselineServerJar"));

        YierdisBench.BenchConfig config = config(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentJar.toString()
        );

        Assert.assertTrue(config.comparisonMode);
        Assert.assertFalse(config.noStartServer);
        Assert.assertEquals(baselineJar, config.baselineServerJar);
        Assert.assertEquals(currentJar, config.currentServerJar);
        Assert.assertEquals(List.of("baseline", "current"), config.backends);
        Assert.assertTrue(config.skipNativeDefragCompare);
    }

    @Test
    public void comparisonModeRejectsServerJar() throws Exception {
        Path baselineJar = regularTempJar("baseline");
        Path currentJar = regularTempJar("current");
        Path singleRunJar = regularTempJar("single");

        IllegalArgumentException rejected = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentJar.toString(),
                "--serverJar", singleRunJar.toString()
        );

        Assert.assertTrue(rejected.getMessage().contains("serverJar"));
    }

    @Test
    public void comparisonModeRejectsNoStartServer() throws Exception {
        Path baselineJar = regularTempJar("baseline");
        Path currentJar = regularTempJar("current");

        IllegalArgumentException rejected = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentJar.toString(),
                "--noStartServer"
        );

        Assert.assertTrue(rejected.getMessage().contains("noStartServer"));
    }

    @Test
    public void comparisonModeRejectsNativeEval() throws Exception {
        Path baselineJar = regularTempJar("baseline");
        Path currentJar = regularTempJar("current");

        IllegalArgumentException rejected = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentJar.toString(),
                "--nativeEval"
        );

        Assert.assertTrue(rejected.getMessage().contains("nativeEval"));
    }

    @Test
    public void comparisonModeRejectsMissingOrNonRegularJarPathsBeforeLaunch() throws Exception {
        Path currentJar = regularTempJar("current");
        Path missingBaseline = Files.createTempDirectory("missing-baseline-").resolve("server.jar");
        IllegalArgumentException missing = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", missingBaseline.toString(),
                "--currentServerJar", currentJar.toString()
        );
        Assert.assertTrue(missing.getMessage().contains("baselineServerJar"));
        Assert.assertTrue(missing.getMessage().contains(missingBaseline.toAbsolutePath().toString()));

        Path baselineJar = regularTempJar("baseline");
        Path currentDirectory = Files.createTempDirectory("current-dir-");
        IllegalArgumentException nonRegular = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentDirectory.toString()
        );
        Assert.assertTrue(nonRegular.getMessage().contains("currentServerJar"));
        Assert.assertTrue(nonRegular.getMessage().contains(currentDirectory.toAbsolutePath().toString()));
    }

    @Test
    public void singleRunServerJarAndNoStartServerBehaviorRemainUnchanged() throws Exception {
        Path serverJar = regularTempJar("single");

        YierdisBench.BenchConfig jarConfig = config("--serverJar", serverJar.toString());
        Assert.assertFalse(jarConfig.comparisonMode);
        Assert.assertEquals(serverJar, jarConfig.serverJar);
        Assert.assertEquals(List.of("foreign"), jarConfig.backends);

        YierdisBench.BenchConfig externalConfig = config("--noStartServer");
        Assert.assertFalse(externalConfig.comparisonMode);
        Assert.assertTrue(externalConfig.noStartServer);
        Assert.assertEquals(List.of("external"), externalConfig.backends);
    }

    private static YierdisBench.BenchConfig config(String... argv) {
        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs(argv);
        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        return YierdisBench.BenchConfig.from(args, serverArgs);
    }

    private static IllegalArgumentException assertRejects(String... argv) {
        try {
            config(argv);
            Assert.fail("expected IllegalArgumentException");
            return null;
        } catch (IllegalArgumentException e) {
            return e;
        }
    }

    private static Path regularTempJar(String prefix) throws Exception {
        Path jar = Files.createTempFile(prefix, ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }
}
