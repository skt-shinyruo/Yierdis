package yier.bubu.redis.integration.command;

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
                        .resolve("yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command")
                        .resolve(resolveDefaultsPath(fileName))
                        .normalize(),
                StandardCharsets.UTF_8
        );
    }

    private static Path resolveDefaultsPath(String fileName) {
        return switch (fileName) {
            case "StringCommands.java" -> Path.of("defaults", "string", fileName);
            case "HashCommands.java" -> Path.of("defaults", "hash", fileName);
            case "ListCommands.java" -> Path.of("defaults", "list", fileName);
            case "SetCommands.java" -> Path.of("defaults", "set", fileName);
            case "ZSetCommands.java" -> Path.of("defaults", "zset", fileName);
            case "HllCommands.java" -> Path.of("defaults", "hll", fileName);
            case "KeyCommands.java" -> Path.of("defaults", "keyspace", fileName);
            case "CoreConnectionCommands.java" -> Path.of("defaults", "connection", fileName);
            default -> Path.of("defaults", fileName);
        };
    }

    private static Path repoRoot() {
        Path current = Paths.get("").toAbsolutePath().normalize();
        for (Path p = current; p != null; p = p.getParent()) {
            if (Files.isRegularFile(p.resolve("pom.xml"))
                    && Files.isDirectory(p.resolve("yierdis-command/yierdis-command-builtin"))
                    && Files.isDirectory(p.resolve("yierdis-server/yierdis-server-runtime"))) {
                return p;
            }
        }
        throw new IllegalStateException("Cannot locate repository root from " + current);
    }
}
