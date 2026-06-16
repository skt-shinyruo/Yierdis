package yier.bubu.redis.app.bench.suite;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record MetricSummary(String name, int sampleCount, double min, double median, double mean, double max) {
    public static Map<String, MetricSummary> summarizeRepeats(List<IterationResult> iterations) {
        Map<String, List<Double>> valuesByName = new LinkedHashMap<>();
        for (IterationResult iteration : iterations) {
            if (iteration.kind() != IterationResult.Kind.REPEAT) {
                continue;
            }
            for (SuiteMetric metric : iteration.metrics()) {
                valuesByName.computeIfAbsent(metric.name(), ignored -> new ArrayList<>()).add(metric.value());
            }
        }

        Map<String, MetricSummary> summaries = new LinkedHashMap<>();
        for (Map.Entry<String, List<Double>> entry : valuesByName.entrySet()) {
            summaries.put(entry.getKey(), of(entry.getKey(), entry.getValue()));
        }
        return summaries;
    }

    private static MetricSummary of(String name, List<Double> source) {
        List<Double> values = new ArrayList<>(source);
        values.sort(Comparator.naturalOrder());
        int n = values.size();
        double sum = 0.0;
        for (double value : values) {
            sum += value;
        }
        double median = n % 2 == 1
                ? values.get(n / 2)
                : (values.get(n / 2 - 1) + values.get(n / 2)) / 2.0;
        return new MetricSummary(name, n, values.get(0), median, sum / n, values.get(n - 1));
    }
}
