package yier.bubu.redis.app.bench.suite;

import java.util.Map;

public record ObservationSnapshot(Map<String, String> values) {
    public ObservationSnapshot {
        values = values == null ? Map.of() : Map.copyOf(values);
    }

    public static ObservationSnapshot empty() {
        return new ObservationSnapshot(Map.of());
    }
}
