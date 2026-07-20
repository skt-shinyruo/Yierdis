package yier.bubu.redis.app.bench.storage;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class StorageBenchScriptContractTest {
    @Test
    public void storageScriptUsesExplicitSubcommandAndMillionKeyDefault() throws IOException {
        Path scriptPath = repoRoot().resolve("scripts/storage-bench.sh");
        String script = Files.readString(scriptPath);

        Assert.assertTrue("storage benchmark script must be executable", Files.isExecutable(scriptPath));
        Assert.assertTrue(script.contains("-pl yierdis-benchmark -am"));
        Assert.assertTrue(script.contains("storage \\"));
        Assert.assertTrue(script.contains("STORAGE_KEYS=\"${STORAGE_KEYS:-1000000}\""));
        Assert.assertTrue(script.contains("--keys \"$STORAGE_KEYS\""));
        Assert.assertTrue(script.contains("--key-size \"$STORAGE_KEY_SIZE\""));
        Assert.assertTrue(script.contains("--value-size \"$STORAGE_VALUE_SIZE\""));
        Assert.assertTrue(script.contains("--warmup-operations \"$STORAGE_WARMUP_OPERATIONS\""));
        Assert.assertTrue(script.contains("--precision \"$STORAGE_PRECISION\""));
        Assert.assertTrue(script.contains("--format \"$FORMAT\""));
        Assert.assertFalse(script.contains("--host"));
        Assert.assertFalse(script.contains("--port"));
    }

    private static Path repoRoot() {
        String[] properties = {"maven.multiModuleProjectDirectory", "basedir", "user.dir"};
        for (String property : properties) {
            String value = System.getProperty(property);
            if (value == null || value.isBlank()) {
                continue;
            }
            for (Path candidate = Path.of(value).toAbsolutePath().normalize();
                 candidate != null;
                 candidate = candidate.getParent()) {
                if (isRepositoryRoot(candidate)) {
                    return candidate;
                }
            }
        }
        throw new IllegalStateException("Unable to locate the Yierdis repository root");
    }

    private static boolean isRepositoryRoot(Path candidate) {
        return Files.isRegularFile(candidate.resolve("pom.xml"))
                && Files.isRegularFile(candidate.resolve("scripts/storage-bench.sh"))
                && Files.isRegularFile(candidate.resolve("yierdis-benchmark/pom.xml"));
    }
}
