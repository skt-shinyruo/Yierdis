package yier.bubu.redis.command.defaults;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class KeyspaceZSetCommandsMutationRecordingTest {
    private static final String MANUAL_WRITE_RESULT_MUTATION_RECORDING =
            "recordMutation(ctx, result.mutationOutcome())";

    @Test
    public void keyspaceAndZSetCommandsDoNotManuallyUnpackWriteMutationOutcomes() throws IOException {
        assertNoManualWriteResultMutationRecording(
                "Keyspace write commands should record DB write results through CommandSupport helpers",
                sourcePath("keyspace/KeyCommands.java")
        );
        assertNoManualWriteResultMutationRecording(
                "ZSet write commands should record DB write results through CommandSupport helpers",
                sourcePath("zset/ZSetCommands.java")
        );
    }

    private static void assertNoManualWriteResultMutationRecording(String message, Path source) throws IOException {
        String text = Files.readString(source);

        Assert.assertFalse(message, text.contains(MANUAL_WRITE_RESULT_MUTATION_RECORDING));
    }

    private static Path sourcePath(String commandSource) {
        Path moduleRelative = Path.of("src/main/java/yier/bubu/redis/command/defaults", commandSource);
        if (Files.exists(moduleRelative)) {
            return moduleRelative;
        }
        return Path.of("yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults", commandSource);
    }
}
