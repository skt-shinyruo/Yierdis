package yier.bubu.redis.command.defaults;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class CommandBuiltinDbAccessBoundaryTest {
    private static final Set<String> DB_ENGINE_ALLOWED_FILES = Set.of(
            "CommandDb.java",
            "CommandSupport.java",
            "DefaultCommandModules.java"
    );

    @Test
    public void ordinaryCommandsUseCommandDbFacadeInsteadOfDbEngine() throws IOException {
        Path mainRoot = mainSourceRoot();
        List<String> offenders = new ArrayList<>();
        int scanned = 0;

        try (Stream<Path> files = Files.walk(mainRoot)) {
            for (Path file : files.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".java"))
                    .toList()) {
                scanned++;
                String source = Files.readString(file);
                String fileName = file.getFileName().toString();
                String relative = mainRoot.relativize(file).toString();

                if (!DB_ENGINE_ALLOWED_FILES.contains(fileName) && source.contains("DbEngine")) {
                    offenders.add(relative + " references DbEngine directly");
                }
                if (!DB_ENGINE_ALLOWED_FILES.contains(fileName)
                        && source.contains("support.db(")) {
                    offenders.add(relative + " calls support.db(ctx) instead of CommandDb facade access");
                }
            }
        }

        Assert.assertTrue("architecture guard scanned no command-builtin Java sources", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail(String.join("\n", offenders));
        }
    }

    private static Path mainSourceRoot() {
        Path moduleRelative = Path.of("src/main/java/yier/bubu/redis/command/defaults");
        if (Files.exists(moduleRelative)) {
            return moduleRelative;
        }
        return Path.of("yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults");
    }
}
