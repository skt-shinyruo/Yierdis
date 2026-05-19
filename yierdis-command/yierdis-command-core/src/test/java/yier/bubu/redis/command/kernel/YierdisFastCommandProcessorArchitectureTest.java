package yier.bubu.redis.command.kernel;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class YierdisFastCommandProcessorArchitectureTest {
    @Test
    public void processorDoesNotOwnTransactionQueueChangeContextOrExceptionTranslationDetails() throws IOException {
        String source = Files.readString(processorSource(), StandardCharsets.UTF_8);

        assertNotContains(source, "import yier.bubu.redis.storage.api.DbChangeContext;");
        assertNotContains(source, "import yier.bubu.redis.storage.api.DbChangeListener;");
        assertNotContains(source, "import yier.bubu.redis.storage.api.WrongTypeException;");
        assertNotContains(source, "import yier.bubu.redis.storage.api.YierdisCommandException;");
        assertNotContains(source, "import yier.bubu.redis.runtime.api.YierdisChangeEvent;");
        assertNotContains(source, "import yier.bubu.redis.runtime.api.YierdisChangeEventBridge;");
        assertNotContains(source, "import yier.bubu.redis.execution.api.ExecutionRecord;");
        assertNotContains(source, "DbChangeContext.open(");
        assertNotContains(source, ".tryEnqueue(");
        assertNotContains(source, ".markAborted(");
    }

    private static Path processorSource() {
        Path moduleRoot = Path.of("").toAbsolutePath().normalize();
        Path fromModule = moduleRoot.resolve(
                "src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java"
        );
        if (Files.isRegularFile(fromModule)) {
            return fromModule;
        }
        Path fromRepo = moduleRoot.resolve(
                "yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java"
        );
        if (Files.isRegularFile(fromRepo)) {
            return fromRepo;
        }
        Assert.fail("cannot locate YierdisFastCommandProcessor.java from " + moduleRoot);
        return fromModule;
    }

    private static void assertNotContains(String source, String forbidden) {
        Assert.assertFalse("processor should not contain: " + forbidden, source.contains(forbidden));
    }
}
