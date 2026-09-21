package yier.bubu.redis.app.bench.storage;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchTestRoot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class StorageBenchScriptContractTest {
    @Test
    public void storageScriptUsesExplicitSubcommandAndMillionKeyDefault() throws IOException {
        Path scriptPath = repoRoot().resolve("scripts/storage-bench.sh");
        String script = Files.readString(scriptPath);

        Assert.assertTrue("storage benchmark script must be executable", Files.isExecutable(scriptPath));
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

        Path libraryPath = repoRoot().resolve("scripts/lib.sh");
        String library = Files.readString(libraryPath);
        Assert.assertTrue(script.contains("lib.sh"));
        Assert.assertTrue(library.contains("-pl yierdis-benchmark -am"));
    }

    private static Path repoRoot() {
        return BenchTestRoot.repoRoot();
    }
}
