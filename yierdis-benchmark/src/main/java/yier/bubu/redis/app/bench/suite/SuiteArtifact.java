package yier.bubu.redis.app.bench.suite;

import java.nio.file.Path;
import java.util.Objects;

public record SuiteArtifact(String label, Path jarPath, String commitLabel) {
    public SuiteArtifact {
        if (!"current".equals(label) && !"baseline".equals(label)) {
            throw new IllegalArgumentException("artifact label must be current or baseline");
        }
        Objects.requireNonNull(jarPath, "jarPath");
        commitLabel = commitLabel == null ? "" : commitLabel;
    }
}
