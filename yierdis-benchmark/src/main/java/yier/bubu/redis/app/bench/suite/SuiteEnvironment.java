package yier.bubu.redis.app.bench.suite;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record SuiteEnvironment(Map<String, String> values) {
    public SuiteEnvironment {
        if (values == null) {
            values = Map.of();
        } else {
            values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        }
    }

    public static SuiteEnvironment capture() {
        Map<String, String> captured = new LinkedHashMap<>();
        captured.put("java.version", System.getProperty("java.version", ""));
        captured.put("java.vm.name", System.getProperty("java.vm.name", ""));
        captured.put("os.name", System.getProperty("os.name", ""));
        captured.put("os.arch", System.getProperty("os.arch", ""));
        captured.put("available.processors", Integer.toString(Runtime.getRuntime().availableProcessors()));
        return new SuiteEnvironment(captured);
    }
}
