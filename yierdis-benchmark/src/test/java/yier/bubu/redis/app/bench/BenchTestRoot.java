package yier.bubu.redis.app.bench;

import java.nio.file.Files;
import java.nio.file.Path;

public final class BenchTestRoot {
    private BenchTestRoot() {
    }

    public static Path repoRoot() {
        return discoverRepoRoot(
                propertyPath("maven.multiModuleProjectDirectory"),
                propertyPath("basedir"),
                propertyPath("user.dir")
        );
    }

    public static Path discoverRepoRoot(Path explicitRoot, Path... fallbackCandidates) {
        Path configuredRoot = normalize(explicitRoot);
        if (isRepositoryRoot(configuredRoot)) {
            return configuredRoot;
        }
        for (Path candidate : fallbackCandidates) {
            for (Path current = normalize(candidate);
                 current != null;
                 current = current.getParent()) {
                if (isRepositoryRoot(current)) {
                    return current;
                }
            }
        }
        throw new IllegalStateException(
                "Unable to locate repository root containing pom.xml, scripts/bench.sh, "
                        + "and yierdis-benchmark/pom.xml"
        );
    }

    private static Path propertyPath(String name) {
        String value = System.getProperty(name);
        return value == null || value.isBlank() ? null : Path.of(value);
    }

    private static Path normalize(Path path) {
        return path == null ? null : path.toAbsolutePath().normalize();
    }

    private static boolean isRepositoryRoot(Path path) {
        return path != null
                && Files.isRegularFile(path.resolve("pom.xml"))
                && Files.isRegularFile(path.resolve("scripts/bench.sh"))
                && Files.isRegularFile(path.resolve("yierdis-benchmark/pom.xml"));
    }
}
