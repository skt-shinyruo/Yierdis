package yier.bubu.redis.app.bench.suite;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public record ObservationSnapshot(Map<String, String> values, Map<String, Long> outboundReplyGauges) {
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
        if (outboundReplyGauges == null || outboundReplyGauges.isEmpty()) {
            outboundReplyGauges = Map.of();
        } else {
            Map<String, Long> sorted = new TreeMap<>();
            for (Map.Entry<String, Long> entry : outboundReplyGauges.entrySet()) {
                String key = Objects.requireNonNull(entry.getKey(), "outbound reply gauge key");
                Long value = Objects.requireNonNull(entry.getValue(), "outbound reply gauge value");
                if (value < 0L) {
                    throw new IllegalArgumentException("outbound reply gauge value must be >= 0: " + key);
                }
                sorted.put(key, value);
            }
            outboundReplyGauges = Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
        }
    }

    public ObservationSnapshot(Map<String, String> values) {
        this(values, Map.of());
    }

    public static ObservationSnapshot empty() {
        return new ObservationSnapshot(Map.of(), Map.of());
    }
}
