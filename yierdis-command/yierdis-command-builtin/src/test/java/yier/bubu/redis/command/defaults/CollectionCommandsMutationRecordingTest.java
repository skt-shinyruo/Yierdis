package yier.bubu.redis.command.defaults;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class CollectionCommandsMutationRecordingTest {
    private static final String MANUAL_WRITE_RESULT_MUTATION_RECORDING =
            "recordMutation(ctx, result.mutationOutcome())";

    @Test
    public void collectionCommandsDoNotManuallyUnpackWriteMutationOutcomes() throws IOException {
        assertNoManualWriteResultMutationRecording(
                "Set write commands should record DB write results through CommandSupport helpers",
                sourcePath("set/SetCommands.java")
        );
        assertNoManualWriteResultMutationRecording(
                "Hash write commands should record DB write results through CommandSupport helpers",
                sourcePath("hash/HashCommands.java")
        );
        assertNoManualWriteResultMutationRecording(
                "HLL write commands should record DB write results through CommandSupport helpers",
                sourcePath("hll/HllCommands.java")
        );
        assertNoManualWriteResultMutationRecording(
                "List write commands should record DB write results through CommandSupport helpers",
                sourcePath("list/ListCommands.java")
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
