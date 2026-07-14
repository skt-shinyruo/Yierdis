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
        assertNotContains(source, "changeEmitter");
        assertNotContains(source, "YierdisCommandProcessorOptions");
        assertNotContains(source, ".tryEnqueue(");
        assertNotContains(source, ".markAborted(");
    }

    @Test
    public void legacyChangeRecordingTypesAreAbsent() {
        Assert.assertNull(sourceFileOrNull("CommandChangeEmitter.java"));
        Assert.assertNull(sourceFileOrNull("CommandChangeObserver.java"));
        Assert.assertNull(sourceFileOrNull("YierdisCommandProcessorOptions.java"));
    }

    private static Path processorSource() {
        return sourceFile("YierdisFastCommandProcessor.java");
    }

    private static Path sourceFile(String filename) {
        Path source = sourceFileOrNull(filename);
        if (source != null) {
            return source;
        }
        Assert.fail("cannot locate " + filename + " from " + Path.of("").toAbsolutePath().normalize());
        return Path.of(filename);
    }

    private static Path sourceFileOrNull(String filename) {
        Path moduleRoot = Path.of("").toAbsolutePath().normalize();
        Path fromModule = moduleRoot.resolve(
                "src/main/java/yier/bubu/redis/command/kernel/" + filename
        );
        if (Files.isRegularFile(fromModule)) {
            return fromModule;
        }
        Path fromRepo = moduleRoot.resolve(
                "yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/" + filename
        );
        if (Files.isRegularFile(fromRepo)) {
            return fromRepo;
        }
        return null;
    }

    private static void assertNotContains(String source, String forbidden) {
        Assert.assertFalse("source should not contain: " + forbidden, source.contains(forbidden));
    }

}
