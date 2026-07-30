package yier.bubu.redis.command.defaults;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class CommandPipelineArchitectureTest {
    @Test
    public void connectionCommandsUseDirectSemanticPipeline() throws IOException {
        String source = Files.readString(mainSourceRoot().resolve("connection/CoreConnectionCommands.java"));

        for (String forbidden : List.of(
                "CommandDefinition", "CommandParsers", "ArgReader",
                "CommandPreparationContext", "RedisReplyWriter",
                ".reply()", "new PreparedCommand"
        )) {
            Assert.assertFalse("legacy command path remains: " + forbidden, source.contains(forbidden));
        }
        Assert.assertEquals(8, occurrences(source, "new CommandSpec("));
    }

    @Test
    public void stringCommandsUseDirectSemanticPipeline() throws IOException {
        String source = Files.readString(mainSourceRoot().resolve("string/StringCommands.java"));

        for (String forbidden : List.of(
                "CommandDefinition", "CommandParsers", "ArgReader",
                "CommandParseResult", "CommandParseError", "CommandPreparationContext",
                "RedisReplyWriter", "BulkStringReplyAdapter", ".reply()", "new PreparedCommand"
        )) {
            Assert.assertFalse("legacy command path remains: " + forbidden, source.contains(forbidden));
        }
        Assert.assertEquals(9, occurrences(source, "new CommandSpec("));
    }

    @Test
    public void keyspaceCommandsUseDirectSemanticPipeline() throws IOException {
        String source = Files.readString(mainSourceRoot().resolve("keyspace/KeyCommands.java"));

        for (String forbidden : List.of(
                "CommandDefinition", "CommandParsers", "ArgReader",
                "CommandParseResult", "CommandParseError", "CommandPreparationContext",
                "RedisReplyWriter", "BulkStringReplyAdapter", ".reply()", "new PreparedCommand",
                "sliceResetFromRequest", "clearScratch"
        )) {
            Assert.assertFalse("legacy command path remains: " + forbidden, source.contains(forbidden));
        }
        Assert.assertEquals(14, occurrences(source, "new CommandSpec("));
    }

    private static int occurrences(String source, String needle) {
        int count = 0;
        int offset = 0;
        while ((offset = source.indexOf(needle, offset)) >= 0) {
            count++;
            offset += needle.length();
        }
        return count;
    }

    private static Path mainSourceRoot() {
        Path moduleRelative = Path.of("src/main/java/yier/bubu/redis/command/defaults");
        return Files.exists(moduleRelative)
                ? moduleRelative
                : Path.of("yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults");
    }
}
