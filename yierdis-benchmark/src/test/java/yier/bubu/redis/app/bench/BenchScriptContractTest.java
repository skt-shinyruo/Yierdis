package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class BenchScriptContractTest {
    @Test
    public void benchScriptDoesNotExposeDeprecatedBackendSelection() throws IOException {
        String script = Files.readString(findRepoRoot().resolve("scripts/bench.sh"));

        Assert.assertFalse(script.contains("BACKENDS="));
        Assert.assertFalse(script.contains("--backends"));
    }

    private static Path findRepoRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isRegularFile(current.resolve("scripts/bench.sh"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("repository root not found");
    }
}
