package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NioBenchmarkRunnerTest {
    private static final Duration SERVER_WAIT = Duration.ofSeconds(5);
    private static final RedisBenchmarkCatalog CATALOG = new RedisBenchmarkCatalog();
    private static final RedisBenchmarkCase PING =
            CATALOG.caseById("ping_inline");
    private static final RedisBenchmarkCase GET = CATALOG.caseById("get");
    private static final RedisBenchmarkCase LRANGE = CATALOG.caseById("lrange_100");
    private static final NioBenchmarkClient.ReplyLimits SMALL_REPLY_LIMITS =
            new NioBenchmarkClient.ReplyLimits(16, 16, 16, 3, 4);

    @Test(timeout = 5_000)
    public void pipelineOneCompletesConfiguredRequestsAcrossClients() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.immediatePong(3)) {
            BenchmarkStatistics statistics = execute(server, 11, 3, 1, BenchmarkClock.system());
            List<Integer> perConnection =
                    server.awaitCommandCountsPerConnection(11, 3, SERVER_WAIT);

            assertCounters(statistics, 11, 11, 11, 11);
            Assert.assertEquals(3, server.acceptedConnectionCount());
            Assert.assertEquals(11, server.commandCount());
            Assert.assertEquals(11, perConnection.stream().mapToInt(Integer::intValue).sum());
        }
    }

    @Test(timeout = 5_000)
    public void finalPipelineIsFullButOnlyConfiguredSamplesAreRecorded() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.batchedPong(1, 4)) {
            BenchmarkStatistics statistics = execute(server, 10, 1, 4, BenchmarkClock.system());

            Assert.assertEquals(
                    List.of(12),
                    server.awaitCommandCountsPerConnection(12, 1, SERVER_WAIT)
            );
            assertCounters(statistics, 10, 12, 12, 10);
            Assert.assertEquals(12, server.commandCount());
        }
    }

    @Test(timeout = 5_000)
    public void everyReplyInOnePipelineUsesTheSameFirstReadLatency() throws Exception {
        ManualClock clock = new ManualClock();
        try (ScriptedRespServer server = ScriptedRespServer.batchedPong(
                1,
                3,
                () -> clock.advanceMicros(250)
        )) {
            BenchmarkStatistics statistics = execute(server, 3, 1, 3, clock);

            Assert.assertEquals(
                    List.of(3),
                    server.awaitCommandCountsPerConnection(3, 1, SERVER_WAIT)
            );
            assertCounters(statistics, 3, 3, 3, 3);
            BenchmarkLatencyRecorder expectedLatency = new BenchmarkLatencyRecorder(3);
            for (int reply = 0; reply < 3; reply++) {
                expectedLatency.recordMicros(250);
            }
            Assert.assertEquals(expectedLatency.summary(), statistics.latency());
            Assert.assertEquals(4, clock.callCount());
        }
    }

    @Test(timeout = 5_000)
    public void stopDrainsThresholdClientButDoesNotWaitForAnotherOutstandingClient()
            throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.thresholdBoundaryScenario()) {
            BenchmarkClock clock = server.coordinatedClock(BenchmarkClock.system());
            BenchmarkStatistics statistics = execute(server, 7, 3, 3, clock);

            Assert.assertEquals(
                    List.of(3, 3, 3),
                    server.awaitCommandCountsPerConnection(9, 3, SERVER_WAIT)
            );
            server.awaitSentReplies(7, SERVER_WAIT);
            assertCounters(statistics, 7, 7, 9, 7);
            Assert.assertEquals(7, server.sentReplyCount());
            Assert.assertFalse(server.stallWasReleased());
        }
    }

    @Test(timeout = 5_000)
    public void bulkPayloadAtLimitCanExceedFixedReadBufferWithFraming() throws Exception {
        byte[] reply = ascii("$16\r\n0123456789abcdef\r\n");
        try (ScriptedRespServer server = ScriptedRespServer.immediate(1, reply)) {
            BenchmarkStatistics statistics = execute(
                    server,
                    GET,
                    1,
                    1,
                    1,
                    BenchmarkClock.system(),
                    SMALL_REPLY_LIMITS
            );

            Assert.assertEquals(
                    List.of(1),
                    server.awaitCommandCountsPerConnection(1, 1, SERVER_WAIT)
            );
            assertCounters(statistics, 1, 1, 1, 1);
        }
    }

    @Test(timeout = 5_000)
    public void arrayAggregateCanExceedFixedReadBufferAndPerBulkLimit() throws Exception {
        byte[] reply = ascii(
                "*3\r\n$4\r\nabcd\r\n$4\r\nefgh\r\n$4\r\nijkl\r\n"
        );
        try (ScriptedRespServer server = ScriptedRespServer.immediate(1, reply)) {
            BenchmarkStatistics statistics = execute(
                    server,
                    LRANGE,
                    1,
                    1,
                    1,
                    BenchmarkClock.system(),
                    SMALL_REPLY_LIMITS
            );

            Assert.assertEquals(
                    List.of(1),
                    server.awaitCommandCountsPerConnection(1, 1, SERVER_WAIT)
            );
            assertCounters(statistics, 1, 1, 1, 1);
        }
    }

    private static BenchmarkStatistics execute(
            ScriptedRespServer server,
            int requests,
            int clients,
            int pipeline,
            BenchmarkClock clock
    ) throws Exception {
        return execute(
                server,
                PING,
                requests,
                clients,
                pipeline,
                clock,
                NioBenchmarkClient.ReplyLimits.defaults()
        );
    }

    private static BenchmarkStatistics execute(
            ScriptedRespServer server,
            RedisBenchmarkCase testCase,
            int requests,
            int clients,
            int pipeline,
            BenchmarkClock clock,
            NioBenchmarkClient.ReplyLimits replyLimits
    ) throws Exception {
        BenchmarkConfig config = new BenchmarkConfig(
                server.host(),
                server.port(),
                requests,
                clients,
                3,
                pipeline,
                OptionalLong.empty(),
                true,
                Set.of(testCase.id()),
                3,
                19L,
                BenchmarkFormat.HUMAN,
                "",
                "",
                0
        );
        NioBenchmarkRunner runner = new NioBenchmarkRunner(clock, replyLimits);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<BenchmarkStatistics> execution = executor.submit(() -> runner.execute(
                testCase,
                config,
                BenchmarkPayload.generate(config.dataSize()),
                new BenchmarkRandom(config.seed())
        ));
        try {
            return execution.get(4, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new AssertionError(cause);
        } finally {
            execution.cancel(true);
            executor.shutdownNow();
        }
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static void assertCounters(
            BenchmarkStatistics statistics,
            int requested,
            long completed,
            long wire,
            long samples
    ) {
        Assert.assertEquals(requested, statistics.requestedRequests());
        Assert.assertEquals(completed, statistics.completedRequests());
        Assert.assertEquals(wire, statistics.wireRequests());
        Assert.assertEquals(samples, statistics.histogramSamples());
        Assert.assertEquals(samples, statistics.latency().count());
    }

    private static final class ManualClock implements BenchmarkClock {
        private final AtomicLong nanos = new AtomicLong();
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public long nanoTime() {
            calls.incrementAndGet();
            return nanos.get();
        }

        void advanceMicros(long micros) {
            nanos.addAndGet(TimeUnit.MICROSECONDS.toNanos(micros));
        }

        int callCount() {
            return calls.get();
        }
    }
}
