package yier.bubu.redis.app.bench.suite;

import java.util.Locale;

public enum SuiteProfileName {
    RELEASE("release"),
    FULL("full");

    private final String cliName;

    SuiteProfileName(String cliName) {
        this.cliName = cliName;
    }

    public String cliName() {
        return cliName;
    }

    public static SuiteProfileName parse(String value) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        for (SuiteProfileName candidate : values()) {
            if (candidate.cliName.equals(normalized)) {
                return candidate;
            }
        }
        throw new IllegalArgumentException("suiteProfile must be one of: release, full");
    }
}
