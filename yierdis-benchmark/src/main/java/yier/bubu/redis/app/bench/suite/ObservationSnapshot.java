package yier.bubu.redis.app.bench.suite;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public record ObservationSnapshot(Map<String, String> values) {
    public ObservationSnapshot {
        if (values == null || values.isEmpty()) {
            values = Map.of();
        } else {
            Map<String, String> sorted = new TreeMap<>();
            for (Map.Entry<String, String> entry : values.entrySet()) {
                sorted.put(Objects.requireNonNull(entry.getKey(), "observation key"),
                        Objects.requireNonNull(entry.getValue(), "observation value"));
            }
            values = Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
        }
    }

    public static ObservationSnapshot empty() {
        return new ObservationSnapshot(Map.of());
    }
}
