package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.Set;

public class BenchmarkOutputRendererTest {
    private static final String CSV_HEADER = "\"test\",\"rps\",\"avg_latency_ms\","
            + "\"min_latency_ms\",\"p50_latency_ms\",\"p95_latency_ms\","
            + "\"p99_latency_ms\",\"max_latency_ms\",\"status\",\"reason\"";

    private final RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();
    private final BenchmarkOutputRenderer renderer = new BenchmarkOutputRenderer();

    @Test
    public void csvKeepsOfficialColumnsFirstAndLeavesUnsupportedMetricsEmpty() {
        BenchmarkRunResult run = new BenchmarkRunResult(List.of(
                success("ping_inline"),
                BenchmarkCaseResult.unsupported(
                        catalog.caseById("spop"),
                        "Yierdis does not support SPOP"
                )
        ));

        String rendered = renderer.render(config(BenchmarkFormat.CSV, true, 4), run);

        Assert.assertEquals(CSV_HEADER + "\n"
                + "\"PING_INLINE\",\"2500.00\",\"1.234\",\"0.100\",\"1.000\","
                + "\"2.000\",\"3.000\",\"4.567\",\"SUCCESS\",\"\"\n"
                + "\"SPOP\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
                + "\"UNSUPPORTED\",\"Yierdis does not support SPOP\"\n", rendered);
    }

    @Test
    public void csvSuccessUsesExactFixedPrecisionMilliseconds() {
        BenchmarkRunResult run = new BenchmarkRunResult(List.of(success("ping_inline")));

        String rendered = renderer.render(config(BenchmarkFormat.CSV, true, 0), run);

        Assert.assertEquals(CSV_HEADER + "\n"
                + "\"PING_INLINE\",\"2500.00\",\"1.234\",\"0.100\",\"1.000\","
                + "\"2.000\",\"3.000\",\"4.567\",\"SUCCESS\",\"\"\n", rendered);
    }

    @Test
    public void quietAndHumanOutputNeverRenderFakeMetricsForFailure() {
        BenchmarkRunResult run = new BenchmarkRunResult(List.of(
                BenchmarkCaseResult.failed(catalog.caseById("set"), 7, "disconnect")
        ));

        String quiet = renderer.render(config(BenchmarkFormat.QUIET, true, 3), run);
        String human = renderer.render(config(BenchmarkFormat.HUMAN, true, 3), run);

        Assert.assertEquals("SET: FAILED after 7 replies (disconnect)\n", quiet);
        Assert.assertEquals("====== SET ======\n"
                + "status: FAILED\n"
                + "completed replies: 7\n"
                + "reason: disconnect\n", human);
        assertNoFakeMetrics(quiet);
        assertNoFakeMetrics(human);
    }

    @Test
    public void quietSuccessUsesOfficialSingleLineStyle() {
        BenchmarkRunResult run = new BenchmarkRunResult(List.of(success("ping_inline")));

        Assert.assertEquals(
                "PING_INLINE: 2500.00 requests per second, p50=1.000 msec\n",
                renderer.render(config(BenchmarkFormat.QUIET, true, 2), run)
        );
    }

    @Test
    public void humanSuccessUsesActualRunAndConfigFields() {
        BenchmarkRunResult run = new BenchmarkRunResult(List.of(success("ping_inline")));
        String expectedKeepAlive = "====== PING_INLINE ======\n"
                + "  5 requests completed in 0.002 seconds\n"
                + "  4 parallel clients\n"
                + "  3 bytes payload\n"
                + "  keep alive: 1\n"
                + "\n"
                + "Summary:\n"
                + "  throughput summary: 2500.00 requests per second\n"
                + "  latency summary (msec):\n"
                + "          avg       min       p50       p95       p99       max\n"
                + "        1.234     0.100     1.000     2.000     3.000     4.567\n";

        Assert.assertEquals(
                expectedKeepAlive,
                renderer.render(config(BenchmarkFormat.HUMAN, true, 4), run)
        );
        Assert.assertTrue(
                renderer.render(config(BenchmarkFormat.HUMAN, false, 4), run)
                        .contains("  keep alive: 0\n")
        );
    }

    @Test
    public void allNonSuccessStatusesCarryReasonsWithoutFakeMetrics() {
        BenchmarkRunResult run = nonSuccessRun();

        String quiet = renderer.render(config(BenchmarkFormat.QUIET, true, 3), run);
        String human = renderer.render(config(BenchmarkFormat.HUMAN, true, 3), run);
        String csv = renderer.render(config(BenchmarkFormat.CSV, true, 3), run);

        Assert.assertEquals("SPOP: UNSUPPORTED (missing command)\n"
                + "GET: SKIPPED (setup failed)\n"
                + "SET: FAILED after 7 replies (disconnect)\n", quiet);
        Assert.assertEquals("====== SPOP ======\n"
                + "status: UNSUPPORTED\n"
                + "reason: missing command\n"
                + "\n"
                + "====== GET ======\n"
                + "status: SKIPPED\n"
                + "reason: setup failed\n"
                + "\n"
                + "====== SET ======\n"
                + "status: FAILED\n"
                + "completed replies: 7\n"
                + "reason: disconnect\n", human);
        Assert.assertEquals(CSV_HEADER + "\n"
                + "\"SPOP\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
                + "\"UNSUPPORTED\",\"missing command\"\n"
                + "\"GET\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
                + "\"SKIPPED\",\"setup failed\"\n"
                + "\"SET\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
                + "\"FAILED\",\"disconnect\"\n", csv);
        assertNoFakeMetrics(quiet);
        assertNoFakeMetrics(human);
        Assert.assertFalse(csv.contains("\"0.00"));
    }

    @Test
    public void renderingIsLocaleIndependentAndPrecisionDoesNotChangeDecimals() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            String rendered = renderer.render(
                    config(BenchmarkFormat.QUIET, true, 0),
                    new BenchmarkRunResult(List.of(success("ping_inline")))
            );

            Assert.assertEquals(
                    "PING_INLINE: 2500.00 requests per second, p50=1.000 msec\n",
                    rendered
            );
            Assert.assertFalse(rendered.contains("2500,00"));
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    public void csvEscapesEveryFieldAccordingToRfc4180() {
        RedisBenchmarkCase custom = customCase("CUSTOM\\PATH,\"TITLE\"\r\nNEXT");
        BenchmarkRunResult run = new BenchmarkRunResult(List.of(
                BenchmarkCaseResult.failed(
                        custom,
                        7,
                        "because\\n, \"quoted\"\r\nand more"
                )
        ));

        Assert.assertEquals(CSV_HEADER + "\n"
                + "\"CUSTOM\\PATH,\"\"TITLE\"\"\r\nNEXT\",\"\",\"\",\"\",\"\",\"\","
                + "\"\",\"\",\"FAILED\",\"because\\n, \"\"quoted\"\"\r\nand more\"\n",
                renderer.render(config(BenchmarkFormat.CSV, true, 3), run));
    }

    @Test
    public void emptyRunsAndReportsHaveRequiredTrailingNewlines() {
        BenchmarkRunResult empty = new BenchmarkRunResult(List.of());

        Assert.assertEquals("", renderer.render(config(BenchmarkFormat.HUMAN, true, 3), empty));
        Assert.assertEquals("", renderer.render(config(BenchmarkFormat.QUIET, true, 3), empty));
        Assert.assertEquals(
                CSV_HEADER + "\n",
                renderer.render(config(BenchmarkFormat.CSV, true, 3), empty)
        );

        for (BenchmarkFormat format : BenchmarkFormat.values()) {
            assertExactlyOneTrailingNewline(renderer.render(
                    config(format, true, 3),
                    new BenchmarkRunResult(List.of(success("ping_inline")))
            ));
            assertExactlyOneTrailingNewline(renderer.render(
                    config(format, true, 3),
                    nonSuccessRun()
            ));
        }
    }

    @Test
    public void quietEscapesDynamicTextSoEachCaseOccupiesOnePhysicalLine() {
        String rendered = renderer.render(
                config(BenchmarkFormat.QUIET, true, 3),
                displayEscapingRun()
        );

        Assert.assertEquals("CUSTOM\\\\TITLE\\r\\nNEXT: FAILED after 7 replies "
                + "(disconnect\\\\nraw\\r\\nretry\\r\\n)\n"
                + "SPOP: UNSUPPORTED (missing\\r\\ncommand\\r\\n)\n"
                + "GET: SKIPPED (setup\\r\\nfailed\\r\\n)\n", rendered);
        Assert.assertEquals(3L, rendered.chars().filter(value -> value == '\n').count());
        Assert.assertFalse(rendered.contains("\r"));
    }

    @Test
    public void humanEscapesDynamicTextWithoutTrimmingNonSuccessReasons() {
        String rendered = renderer.render(
                config(BenchmarkFormat.HUMAN, true, 3),
                displayEscapingRun()
        );

        Assert.assertEquals("====== CUSTOM\\\\TITLE\\r\\nNEXT ======\n"
                + "status: FAILED\n"
                + "completed replies: 7\n"
                + "reason: disconnect\\\\nraw\\r\\nretry\\r\\n\n"
                + "\n"
                + "====== SPOP ======\n"
                + "status: UNSUPPORTED\n"
                + "reason: missing\\r\\ncommand\\r\\n\n"
                + "\n"
                + "====== GET ======\n"
                + "status: SKIPPED\n"
                + "reason: setup\\r\\nfailed\\r\\n\n", rendered);
        Assert.assertEquals(12L, rendered.chars().filter(value -> value == '\n').count());
        Assert.assertFalse(rendered.contains("\r"));
    }

    @Test
    public void humanReportEscapesTrailingReasonLineBreaksWithoutLosingData() {
        BenchmarkRunResult run = new BenchmarkRunResult(List.of(
                BenchmarkCaseResult.failed(
                        catalog.caseById("set"),
                        7,
                        "disconnect\r\n\n"
                )
        ));

        Assert.assertEquals("====== SET ======\n"
                + "status: FAILED\n"
                + "completed replies: 7\n"
                + "reason: disconnect\\r\\n\\n\n",
                renderer.render(config(BenchmarkFormat.HUMAN, true, 3), run));
    }

    @Test
    public void nullInputsFailClearly() {
        BenchmarkConfig config = config(BenchmarkFormat.HUMAN, true, 3);
        BenchmarkRunResult run = new BenchmarkRunResult(List.of());

        NullPointerException nullConfig = Assert.assertThrows(
                NullPointerException.class,
                () -> renderer.render(null, run)
        );
        NullPointerException nullRun = Assert.assertThrows(
                NullPointerException.class,
                () -> renderer.render(config, null)
        );

        Assert.assertEquals("config", nullConfig.getMessage());
        Assert.assertEquals("run", nullRun.getMessage());
    }

    private BenchmarkCaseResult success(String id) {
        return BenchmarkCaseResult.success(catalog.caseById(id), statistics());
    }

    private BenchmarkRunResult nonSuccessRun() {
        return new BenchmarkRunResult(List.of(
                BenchmarkCaseResult.unsupported(catalog.caseById("spop"), "missing command"),
                BenchmarkCaseResult.skipped(catalog.caseById("get"), "setup failed"),
                BenchmarkCaseResult.failed(catalog.caseById("set"), 7, "disconnect")
        ));
    }

    private BenchmarkRunResult displayEscapingRun() {
        return new BenchmarkRunResult(List.of(
                BenchmarkCaseResult.failed(
                        customCase("CUSTOM\\TITLE\r\nNEXT"),
                        7,
                        "disconnect\\nraw\r\nretry\r\n"
                ),
                BenchmarkCaseResult.unsupported(
                        catalog.caseById("spop"),
                        "missing\r\ncommand\r\n"
                ),
                BenchmarkCaseResult.skipped(
                        catalog.caseById("get"),
                        "setup\r\nfailed\r\n"
                )
        ));
    }

    private static RedisBenchmarkCase customCase(String title) {
        return new RedisBenchmarkCase(
                "custom",
                title,
                Set.of("custom"),
                RedisBenchmarkCommandTemplate.inline("PING\r\n"),
                Set.of("PING"),
                BenchmarkReplyExpectation.PONG,
                RedisBenchmarkCase.Support.available(),
                ""
        );
    }

    private static BenchmarkStatistics statistics() {
        BenchmarkLatencyRecorder.Summary latency = new BenchmarkLatencyRecorder.Summary(
                2,
                1_234.0,
                100,
                1_000,
                2_000,
                3_000,
                4_567
        );
        return BenchmarkStatistics.from(2, 5, 6, 2, 2, latency);
    }

    private static BenchmarkConfig config(
            BenchmarkFormat format,
            boolean keepAlive,
            int precision
    ) {
        return new BenchmarkConfig(
                "127.0.0.1",
                16379,
                2,
                4,
                3,
                1,
                OptionalLong.empty(),
                keepAlive,
                Set.of(),
                precision,
                7L,
                format,
                "",
                "",
                0
        );
    }

    private static void assertNoFakeMetrics(String rendered) {
        Assert.assertFalse(rendered.contains("requests per second"));
        Assert.assertFalse(rendered.contains("latency"));
        Assert.assertFalse(rendered.contains("0.00"));
    }

    private static void assertExactlyOneTrailingNewline(String rendered) {
        Assert.assertTrue(rendered.endsWith("\n"));
        Assert.assertFalse(rendered.endsWith("\n\n"));
    }
}
