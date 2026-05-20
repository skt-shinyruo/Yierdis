package yier.bubu.redis.command.defaults.string;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class StringCommandsMutationRecordingTest {
    @Test
    public void stringCommandsDoNotManuallyUnpackWriteMutationOutcomes() throws IOException {
        String source = Files.readString(sourcePath());

        Assert.assertFalse(
                "String write commands should record DB write results through CommandSupport helpers",
                source.contains("recordMutation(ctx, result.mutationOutcome())")
        );
    }

    private static Path sourcePath() {
        Path moduleRelative = Path.of("src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java");
        if (Files.exists(moduleRelative)) {
            return moduleRelative;
        }
        return Path.of("yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java");
    }
}
