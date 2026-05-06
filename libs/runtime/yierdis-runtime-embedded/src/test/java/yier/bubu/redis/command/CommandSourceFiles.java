package yier.bubu.redis.command;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

final class CommandSourceFiles {
    private CommandSourceFiles() {
    }

    static String readCommandDefaults(String fileName) throws IOException {
        return Files.readString(
                repoRoot()
                        .resolve("libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command")
                        .resolve(fileName)
                        .normalize(),
                StandardCharsets.UTF_8
        );
    }

    private static Path repoRoot() {
        Path current = Paths.get("").toAbsolutePath().normalize();
        for (Path p = current; p != null; p = p.getParent()) {
            if (Files.isRegularFile(p.resolve("pom.xml"))
                    && Files.isDirectory(p.resolve("libs/command/yierdis-command-defaults"))
                    && Files.isDirectory(p.resolve("libs/runtime/yierdis-runtime-embedded"))) {
                return p;
            }
        }
        throw new IllegalStateException("Cannot locate repository root from " + current);
    }
}
