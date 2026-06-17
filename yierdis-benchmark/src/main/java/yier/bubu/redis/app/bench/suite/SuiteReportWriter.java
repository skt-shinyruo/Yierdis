package yier.bubu.redis.app.bench.suite;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class SuiteReportWriter {
    private SuiteReportWriter() {
    }

    public static void writeAll(SuiteRunResult result, Path reportDir) throws IOException {
        Files.createDirectories(reportDir);
        Files.writeString(reportDir.resolve("suite-result.json"), SuiteJsonWriter.write(result), StandardCharsets.UTF_8);
        Files.writeString(reportDir.resolve("metrics.csv"), SuiteCsvWriter.metricsCsv(result), StandardCharsets.UTF_8);
        Files.writeString(reportDir.resolve("comparisons.csv"), SuiteCsvWriter.comparisonsCsv(result), StandardCharsets.UTF_8);
        Files.writeString(reportDir.resolve("report.md"), SuiteMarkdownWriter.write(result), StandardCharsets.UTF_8);
    }
}
