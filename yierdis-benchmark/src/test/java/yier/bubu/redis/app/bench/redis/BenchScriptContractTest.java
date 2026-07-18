package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class BenchScriptContractTest {
    @Test
    public void benchScriptBuildsOnlyTheClientAndTargetsAnExistingServer() throws IOException {
        String script = Files.readString(repoRoot().resolve("scripts/bench.sh"));

        Assert.assertTrue(script.contains("-pl yierdis-benchmark -am"));
        Assert.assertTrue(script.contains("--host"));
        Assert.assertTrue(script.contains("--port"));
        Assert.assertTrue(script.contains("--requests"));
        Assert.assertTrue(script.contains("--clients"));
        Assert.assertTrue(script.contains("--data-size"));
        Assert.assertTrue(script.contains("--pipeline"));
        Assert.assertFalse(script.contains("--serverJar"));
        Assert.assertFalse(script.contains("currentServerJar"));
        Assert.assertFalse(script.contains("baselineServerJar"));
        Assert.assertFalse(script.contains("--suite"));
    }

    @Test
    public void currentBenchmarkDocumentationDoesNotClaimRetiredPerformanceGate() throws IOException {
        String documentation = Files.readString(
                repoRoot().resolve("docs/project-docs/testing-and-debugging.md")
        );

        Assert.assertFalse(documentation.contains("四命令 `0.90` benchmark gate"));
    }

    private static Path repoRoot() {
        if (System.getProperty("maven.multiModuleProjectDirectory") == null) {
            Path moduleRoot = Path.of(System.getProperty("basedir"));
            System.setProperty("maven.multiModuleProjectDirectory", moduleRoot.getParent().toString());
        }
        return Path.of(System.getProperty("maven.multiModuleProjectDirectory"));
    }
}
