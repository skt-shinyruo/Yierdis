package yier.bubu.redis.app.bench.suite;

import java.nio.file.Path;
import java.util.Objects;

public record SuiteArtifact(
        String label,
        Kind kind,
        Path jarPath,
        String host,
        int port,
        String commitLabel,
        String authUser,
        String authPassword,
        int db
) {
    public enum Kind {
        YIERDIS_JAR,
        EXTERNAL_REDIS
    }

    public SuiteArtifact {
        Objects.requireNonNull(label, "label");
        Objects.requireNonNull(kind, "kind");
        commitLabel = commitLabel == null ? "" : commitLabel;
        authUser = authUser == null ? "" : authUser;
        authPassword = authPassword == null ? "" : authPassword;
        host = host == null ? "" : host;
        if (kind == Kind.YIERDIS_JAR && jarPath == null) {
            throw new IllegalArgumentException("jarPath is required for YIERDIS_JAR artifacts");
        }
        if (kind == Kind.EXTERNAL_REDIS) {
            if (host.isBlank()) {
                throw new IllegalArgumentException("host is required for EXTERNAL_REDIS artifacts");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("port must be in 1..65535 for EXTERNAL_REDIS artifacts");
            }
        }
    }

    public SuiteArtifact(String label, Path jarPath, String commitLabel) {
        this(label, Kind.YIERDIS_JAR, jarPath, "", 0, commitLabel, "", "", 0);
    }

    public static SuiteArtifact yierdisJar(String label, Path jarPath, String commitLabel) {
        return new SuiteArtifact(label, Kind.YIERDIS_JAR, jarPath, "", 0, commitLabel, "", "", 0);
    }

    public static SuiteArtifact externalRedis(
            String label,
            String host,
            int port,
            String authUser,
            String authPassword,
            int db
    ) {
        return new SuiteArtifact(label, Kind.EXTERNAL_REDIS, null, host, port, "", authUser, authPassword, db);
    }
}
