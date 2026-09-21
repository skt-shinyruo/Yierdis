package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.app.bench.LatencyRecorder;

import java.util.Objects;

import static yier.bubu.redis.app.bench.BenchOutput.append;
import static yier.bubu.redis.app.bench.BenchOutput.format;

public final class BenchmarkOutputRenderer {
    private static final String CSV_HEADER = "\"test\",\"rps\",\"avg_latency_ms\","
            + "\"min_latency_ms\",\"p50_latency_ms\",\"p95_latency_ms\","
            + "\"p99_latency_ms\",\"max_latency_ms\",\"status\",\"reason\"";

    public String render(BenchmarkConfig config, BenchmarkRunResult run) {
        BenchmarkConfig requiredConfig = Objects.requireNonNull(config, "config");
        BenchmarkRunResult requiredRun = Objects.requireNonNull(run, "run");
        return switch (requiredConfig.format()) {
            case HUMAN -> renderHuman(requiredConfig, requiredRun);
            case QUIET -> renderQuiet(requiredRun);
            case CSV -> renderCsv(requiredRun);
        };
    }

    private static String renderHuman(BenchmarkConfig config, BenchmarkRunResult run) {
        StringBuilder output = new StringBuilder();
        for (BenchmarkCaseResult result : run.cases()) {
            if (!output.isEmpty()) {
                output.append('\n');
            }
            output.append("====== ");
            appendDisplayText(output, result.testCase().title());
            output.append(" ======\n");
            if (result.status() == BenchmarkStatus.SUCCESS) {
                appendHumanSuccess(output, config, result.statistics());
            } else {
                appendHumanNonSuccess(output, result);
            }
        }
        return output.toString();
    }

    private static void appendHumanSuccess(
            StringBuilder output,
            BenchmarkConfig config,
            BenchmarkStatistics statistics
    ) {
        append(
                output,
                "  %d requests completed in %.3f seconds\n",
                statistics.completedRequests(),
                statistics.elapsedMillis() / 1_000.0
        );
        append(output, "  %d parallel clients\n", config.clients());
        append(output, "  %d bytes payload\n", config.dataSize());
        append(output, "  keep alive: %d\n", config.keepAlive() ? 1 : 0);
        output.append('\n')
                .append("Summary:\n");
        append(
                output,
                "  throughput summary: %.2f requests per second\n",
                statistics.requestsPerSecond()
        );
        output.append("  latency summary (msec):\n")
                .append("          avg       min       p50       p95       p99       max\n");
        LatencyRecorder.Summary latency = statistics.latency();
        append(
                output,
                "        %.3f     %.3f     %.3f     %.3f     %.3f     %.3f\n",
                milliseconds(latency.mean()),
                milliseconds(latency.min()),
                milliseconds(latency.p50()),
                milliseconds(latency.p95()),
                milliseconds(latency.p99()),
                milliseconds(latency.max())
        );
    }

    private static void appendHumanNonSuccess(
            StringBuilder output,
            BenchmarkCaseResult result
    ) {
        output.append("status: ")
                .append(result.status())
                .append('\n');
        if (result.status() == BenchmarkStatus.FAILED) {
            output.append("completed replies: ")
                    .append(result.completedReplies())
                    .append('\n');
        }
        output.append("reason: ");
        appendDisplayText(output, result.reason());
        output.append('\n');
    }

    private static String renderQuiet(BenchmarkRunResult run) {
        StringBuilder output = new StringBuilder();
        for (BenchmarkCaseResult result : run.cases()) {
            appendDisplayText(output, result.testCase().title());
            output.append(": ");
            switch (result.status()) {
                case SUCCESS -> append(
                        output,
                        "%.2f requests per second, p50=%.3f msec\n",
                        result.statistics().requestsPerSecond(),
                        milliseconds(result.statistics().latency().p50())
                );
                case FAILED -> {
                    output.append("FAILED after ")
                            .append(result.completedReplies())
                            .append(" replies (");
                    appendDisplayText(output, result.reason());
                    output.append(")\n");
                }
                case UNSUPPORTED, SKIPPED -> {
                    output.append(result.status()).append(" (");
                    appendDisplayText(output, result.reason());
                    output.append(")\n");
                }
            }
        }
        return output.toString();
    }

    private static String renderCsv(BenchmarkRunResult run) {
        StringBuilder output = new StringBuilder(CSV_HEADER).append('\n');
        for (BenchmarkCaseResult result : run.cases()) {
            if (result.status() == BenchmarkStatus.SUCCESS) {
                BenchmarkStatistics statistics = result.statistics();
                LatencyRecorder.Summary latency = statistics.latency();
                appendCsvRow(
                        output,
                        result.testCase().title(),
                        format("%.2f", statistics.requestsPerSecond()),
                        format("%.3f", milliseconds(latency.mean())),
                        format("%.3f", milliseconds(latency.min())),
                        format("%.3f", milliseconds(latency.p50())),
                        format("%.3f", milliseconds(latency.p95())),
                        format("%.3f", milliseconds(latency.p99())),
                        format("%.3f", milliseconds(latency.max())),
                        result.status().name(),
                        ""
                );
            } else {
                appendCsvRow(
                        output,
                        result.testCase().title(),
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        result.status().name(),
                        result.reason()
                );
            }
        }
        return output.toString();
    }

    private static void appendCsvRow(StringBuilder output, String... fields) {
        for (int index = 0; index < fields.length; index++) {
            if (index > 0) {
                output.append(',');
            }
            output.append('"')
                    .append(fields[index].replace("\"", "\"\""))
                    .append('"');
        }
        output.append('\n');
    }

    private static void appendDisplayText(StringBuilder output, String value) {
        for (int index = 0; index < value.length(); index++) {
            switch (value.charAt(index)) {
                case '\\' -> output.append("\\\\");
                case '\r' -> output.append("\\r");
                case '\n' -> output.append("\\n");
                default -> output.append(value.charAt(index));
            }
        }
    }

    private static double milliseconds(double micros) {
        return micros / 1_000.0;
    }
}
