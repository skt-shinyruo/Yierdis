package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public class YierdisBenchEntrypointTest {
    private static final List<String> REPLACEMENT_OPTIONS = List.of(
            "--host",
            "--port",
            "--requests",
            "--clients",
            "--data-size",
            "--pipeline",
            "--keyspace",
            "--keep-alive",
            "--tests",
            "--precision",
            "--seed",
            "--format",
            "--username",
            "--password",
            "--database"
    );

    private static final List<String> LEGACY_OPTIONS = List.of(
            "--portBase",
            "--noStartServer",
            "--serverJar",
            "--comparisonMode",
            "--baselineServerJar",
            "--currentServerJar",
            "--suite",
            "--suiteProfile",
            "--reportDir",
            "--includeRedis",
            "--redisHost",
            "--redisPort",
            "--redisLabel",
            "--redisUser",
            "--redisAuth",
            "--redisDb",
            "--javaCmd",
            "--xms",
            "--xmx",
            "--maxDirectMemory",
            "--dataSize",
            "--latencyRequests",
            "--latencyClients",
            "--skipPrefill",
            "--skipLatency",
            "--strictReplies",
            "--skipNativeDefragCompare",
            "--nativeEval",
            "--nativeEvalIterations"
    );

    @Test
    public void helpUsesTheReplacementCommandWithoutConnecting() {
        CommandLine commandLine = YierdisBench.commandLine();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(err));

        Assert.assertEquals(0, commandLine.execute("--help"));

        String usage = out.toString();
        for (String option : REPLACEMENT_OPTIONS) {
            Assert.assertTrue("missing replacement option " + option, usage.contains(option));
        }
        for (String option : LEGACY_OPTIONS) {
            Assert.assertFalse("legacy option remains in help " + option, usage.contains(option));
        }
        Assert.assertTrue(usage.contains("storage"));
        Assert.assertEquals("", err.toString());
    }

    @Test
    public void storageHelpListsFootprintOptionsWithoutRunningTheWorkload() {
        CommandLine commandLine = YierdisBench.commandLine();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(err));

        Assert.assertEquals(0, commandLine.execute("storage", "--help"));

        String usage = out.toString();
        Assert.assertTrue(usage.contains("--keys"));
        Assert.assertTrue(usage.contains("--key-size"));
        Assert.assertTrue(usage.contains("--value-size"));
        Assert.assertTrue(usage.contains("--warmup-operations"));
        Assert.assertTrue(usage.contains("--precision"));
        Assert.assertTrue(usage.contains("--format"));
        Assert.assertEquals("", err.toString());
    }

    @Test
    public void legacyOptionsAreUsageErrors() {
        for (String option : List.of("--suite", "--serverJar")) {
            CommandLine commandLine = YierdisBench.commandLine();
            StringWriter out = new StringWriter();
            StringWriter err = new StringWriter();
            commandLine.setOut(new PrintWriter(out));
            commandLine.setErr(new PrintWriter(err));

            Assert.assertEquals(option, 2, commandLine.execute(option));
            Assert.assertEquals("", out.toString());
            Assert.assertTrue(err.toString(), err.toString().contains(option));
        }
    }
}
