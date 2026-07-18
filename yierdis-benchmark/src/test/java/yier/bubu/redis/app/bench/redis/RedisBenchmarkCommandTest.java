package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class RedisBenchmarkCommandTest {
    private final RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();
    private final BenchmarkOutputRenderer renderer = new BenchmarkOutputRenderer();

    @Test
    public void commandRendersInjectedRunAndReturnsRunExitCode() {
        AtomicInteger calls = new AtomicInteger();
        Function<BenchmarkConfig, BenchmarkRunResult> fake = config -> {
            calls.incrementAndGet();
            return new BenchmarkRunResult(List.of(
                    BenchmarkCaseResult.unsupported(catalog.caseById("spop"), "missing")
            ));
        };
        CommandCapture capture = commandLine(new RedisBenchmarkCommand(fake, renderer));

        int exitCode = capture.commandLine.execute("--tests", "spop", "--format", "quiet");

        Assert.assertEquals(0, exitCode);
        Assert.assertEquals(1, calls.get());
        Assert.assertEquals("SPOP: UNSUPPORTED (missing)\n", capture.out.toString());
        Assert.assertEquals("", capture.err.toString());
    }

    @Test
    public void failedRunRendersCompleteResultAndExitsOne() {
        AtomicInteger calls = new AtomicInteger();
        Function<BenchmarkConfig, BenchmarkRunResult> fake = config -> {
            calls.incrementAndGet();
            return new BenchmarkRunResult(List.of(
                    BenchmarkCaseResult.failed(catalog.caseById("set"), 7, "disconnect")
            ));
        };
        CommandCapture capture = commandLine(new RedisBenchmarkCommand(fake, renderer));

        int exitCode = capture.commandLine.execute("--tests", "set", "--format", "quiet");

        Assert.assertEquals(1, exitCode);
        Assert.assertEquals(1, calls.get());
        Assert.assertEquals("SET: FAILED after 7 replies (disconnect)\n", capture.out.toString());
        Assert.assertEquals("", capture.err.toString());
    }

    @Test
    public void unknownSelectorIsUsageErrorWithoutNetworkTraffic() {
        RedisBenchmarkCommand command = new RedisBenchmarkCommand(
                new RedisBenchmark()::run,
                renderer
        );
        CommandCapture capture = commandLine(command);

        int exitCode = capture.commandLine.execute(
                "--host", "127.0.0.1",
                "--port", "1",
                "--tests", "no_such_test",
                "--format", "quiet"
        );

        Assert.assertEquals(2, exitCode);
        Assert.assertEquals("", capture.out.toString());
        Assert.assertTrue(capture.err.toString(),
                capture.err.toString().contains("unknown benchmark selector(s): no_such_test"));
    }

    @Test
    public void invalidConvertedConfigIsUsageErrorBeforeInjectedRunner() {
        AtomicInteger calls = new AtomicInteger();
        Function<BenchmarkConfig, BenchmarkRunResult> fake = config -> {
            calls.incrementAndGet();
            return new BenchmarkRunResult(List.of());
        };

        assertUsageErrorBeforeRunner(fake, "requests must be > 0", "--requests", "0");
        assertUsageErrorBeforeRunner(fake, "format must be one of", "--format", "json");

        Assert.assertEquals(0, calls.get());
    }

    @Test
    public void suppliedCliValuesReachInjectedRunner() {
        AtomicReference<BenchmarkConfig> suppliedConfig = new AtomicReference<>();
        Function<BenchmarkConfig, BenchmarkRunResult> fake = config -> {
            suppliedConfig.set(config);
            return new BenchmarkRunResult(List.of(
                    BenchmarkCaseResult.unsupported(catalog.caseById("spop"), "missing")
            ));
        };
        CommandCapture capture = commandLine(new RedisBenchmarkCommand(fake, renderer));

        int exitCode = capture.commandLine.execute(
                "--host", " benchmark.example ",
                "--port", "6380",
                "--requests", "321",
                "--clients", "12",
                "--data-size", "4096",
                "--pipeline", "8",
                "--keyspace", "2048",
                "--keep-alive=false",
                "--tests", "SPOP",
                "--precision", "4",
                "--seed", "42",
                "--format", "QuIeT",
                "--username", "benchmark-user",
                "--password", "secret",
                "--database", "2"
        );

        Assert.assertEquals(0, exitCode);
        BenchmarkConfig config = suppliedConfig.get();
        Assert.assertNotNull(config);
        Assert.assertEquals("benchmark.example", config.host());
        Assert.assertEquals(6380, config.port());
        Assert.assertEquals(321, config.requests());
        Assert.assertEquals(12, config.clients());
        Assert.assertEquals(4096, config.dataSize());
        Assert.assertEquals(8, config.pipeline());
        Assert.assertEquals(2048L, config.keyspace().orElseThrow());
        Assert.assertFalse(config.keepAlive());
        Assert.assertEquals(Set.of("spop"), config.tests());
        Assert.assertEquals(4, config.precision());
        Assert.assertEquals(42L, config.seed());
        Assert.assertEquals(BenchmarkFormat.QUIET, config.format());
        Assert.assertEquals("benchmark-user", config.username());
        Assert.assertEquals("secret", config.password());
        Assert.assertEquals(2, config.database());
        Assert.assertEquals("SPOP: UNSUPPORTED (missing)\n", capture.out.toString());
        Assert.assertEquals("", capture.err.toString());
    }

    @Test
    public void unexpectedErrorIsNotDowngraded() {
        AssertionError failure = new AssertionError("unexpected");
        RedisBenchmarkCommand command = new RedisBenchmarkCommand(config -> {
            throw failure;
        }, renderer);
        CommandCapture capture = commandLine(command);

        AssertionError thrown = Assert.assertThrows(
                AssertionError.class,
                () -> capture.commandLine.execute("--tests", "set")
        );

        Assert.assertSame(failure, thrown);
        Assert.assertEquals("", capture.out.toString());
    }

    @Test
    public void injectedCollaboratorsAreRequired() {
        Function<BenchmarkConfig, BenchmarkRunResult> fake = config -> new BenchmarkRunResult(List.of());

        NullPointerException nullRunner = Assert.assertThrows(
                NullPointerException.class,
                () -> new RedisBenchmarkCommand(null, renderer)
        );
        NullPointerException nullRenderer = Assert.assertThrows(
                NullPointerException.class,
                () -> new RedisBenchmarkCommand(fake, null)
        );

        Assert.assertEquals("runner", nullRunner.getMessage());
        Assert.assertEquals("renderer", nullRenderer.getMessage());
    }

    private void assertUsageErrorBeforeRunner(
            Function<BenchmarkConfig, BenchmarkRunResult> fake,
            String expectedError,
            String... arguments
    ) {
        CommandCapture capture = commandLine(new RedisBenchmarkCommand(fake, renderer));

        Assert.assertEquals(2, capture.commandLine.execute(arguments));
        Assert.assertEquals("", capture.out.toString());
        Assert.assertTrue(capture.err.toString(), capture.err.toString().contains(expectedError));
    }

    private static CommandCapture commandLine(RedisBenchmarkCommand command) {
        CommandLine commandLine = new CommandLine(command);
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(err));
        return new CommandCapture(commandLine, out, err);
    }

    private record CommandCapture(CommandLine commandLine, StringWriter out, StringWriter err) {
    }
}
