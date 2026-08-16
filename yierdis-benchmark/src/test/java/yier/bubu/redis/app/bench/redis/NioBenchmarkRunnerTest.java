package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
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
import java.util.function.LongSupplier;

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
    public void keepAliveFalseReconnectsAfterEveryPipeline() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.immediatePong(2)) {
            BenchmarkStatistics statistics = execute(
                    server,
                    PING,
                    6,
                    2,
                    2,
                    false,
                    System::nanoTime,
                    NioBenchmarkClient.ReplyLimits.defaults()
            );

            Assert.assertEquals(4, server.awaitAcceptedConnections(4, SERVER_WAIT));
            Assert.assertEquals(
                    List.of(0, 2, 2, 2),
                    server.awaitCommandCountsPerConnection(6, 4, SERVER_WAIT)
            );
            assertCounters(statistics, 6, 6, 6, 6);
        }
    }

    @Test(timeout = 5_000)
    public void authAndSelectPrefixMeasuredCommandsWithoutEnteringCounts() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.authAndSelectAware()) {
            BenchmarkStatistics statistics = execute(
                    server,
                    PING,
                    6,
                    2,
                    2,
                    false,
                    System::nanoTime,
                    NioBenchmarkClient.ReplyLimits.defaults(),
                    "benchmark-user",
                    "secret",
                    2
            );

            Assert.assertEquals(4, server.awaitAcceptedConnections(4, SERVER_WAIT));
            Assert.assertEquals(
                    List.of(0, 4, 4, 4),
                    server.awaitCommandCountsPerConnection(12, 4, SERVER_WAIT)
            );
            Assert.assertEquals(
                    List.of("AUTH", "SELECT", "PING"),
                    server.firstConnectionCommandPrefix(3)
            );
            Assert.assertTrue(server.everyConnectionStartsWith(List.of("AUTH", "SELECT")));
            Assert.assertEquals(
                    List.of("AUTH", "benchmark-user", "secret"),
                    server.firstConnectionCommandArguments(0)
            );
            Assert.assertEquals(6, server.measuredCommandReplies());
            assertCounters(statistics, 6, 6, 6, 6);
        }

        ManualClock clock = new ManualClock();
        try (ScriptedRespServer server = ScriptedRespServer.authAndSelectAware(
                () -> clock.advanceMicros(250)
        )) {
            BenchmarkStatistics statistics = execute(
                    server,
                    PING,
                    1,
                    1,
                    1,
                    true,
                    server.coordinatedClock(clock),
                    NioBenchmarkClient.ReplyLimits.defaults(),
                    "benchmark-user",
                    "secret",
                    2
            );

            BenchmarkLatencyRecorder expectedLatency = new BenchmarkLatencyRecorder(3);
            expectedLatency.recordMicros(250);
            Assert.assertEquals(expectedLatency.summary(), statistics.latency());
            Assert.assertEquals(4, clock.callCount());
        }

        try (ScriptedRespServer server = ScriptedRespServer.authAndSelectAware()) {
            BenchmarkStatistics statistics = execute(
                    server,
                    PING,
                    1,
                    1,
                    1,
                    true,
                    System::nanoTime,
                    NioBenchmarkClient.ReplyLimits.defaults(),
                    "",
                    "password-only",
                    0
            );

            Assert.assertEquals(
                    List.of(2),
                    server.awaitCommandCountsPerConnection(2, 1, SERVER_WAIT)
            );
            Assert.assertEquals(
                    List.of("AUTH", "password-only"),
                    server.firstConnectionCommandArguments(0)
            );
            assertCounters(statistics, 1, 1, 1, 1);
        }
    }

    @Test(timeout = 5_000)
    public void fragmentedRepliesAndPartialWritesStillComplete() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.fragmentingEveryByte(
                "+PONG\r\n"
        )) {
            BenchmarkStatistics statistics = execute(
                    server,
                    20,
                    4,
                    3,
                    System::nanoTime
            );
            List<Integer> perConnection =
                    server.awaitCommandCountsPerConnection(21, 4, SERVER_WAIT);

            Assert.assertEquals(20, statistics.requestedRequests());
            Assert.assertTrue(statistics.completedRequests() >= 20);
            Assert.assertTrue(statistics.completedRequests() <= 21);
            Assert.assertEquals(21, statistics.wireRequests());
            Assert.assertEquals(20, statistics.histogramSamples());
            Assert.assertEquals(20, statistics.latency().count());
            Assert.assertEquals(21, perConnection.stream().mapToInt(Integer::intValue).sum());
        }

        ByteBuffer prefix = ByteBuffer.wrap(ascii("AUTH"));
        ByteBuffer pipeline = ByteBuffer.wrap(ascii("PING"));
        ByteBuffer[] buffers = {prefix, pipeline};
        NioBenchmarkClient.GatheringWrite shortWrite = sources -> {
            int budget = 3;
            long written = 0;
            for (ByteBuffer source : sources) {
                int transferred = Math.min(budget, source.remaining());
                source.position(source.position() + transferred);
                budget -= transferred;
                written += transferred;
                if (budget == 0) {
                    break;
                }
            }
            return written;
        };

        Assert.assertFalse(NioBenchmarkClient.writeBatch(shortWrite, buffers, pipeline));
        Assert.assertEquals(3, prefix.position());
        Assert.assertEquals(0, pipeline.position());
        Assert.assertFalse(NioBenchmarkClient.writeBatch(shortWrite, buffers, pipeline));
        Assert.assertEquals(4, prefix.position());
        Assert.assertEquals(2, pipeline.position());
        Assert.assertTrue(NioBenchmarkClient.writeBatch(shortWrite, buffers, pipeline));
        Assert.assertEquals(4, prefix.position());
        Assert.assertEquals(4, pipeline.position());
    }

    @Test(timeout = 5_000)
    public void runnerCompletesAuthAndPipelineAcrossDeterministicPartialWrites()
            throws Exception {
        List<Long> aggregateWritePositions = new ArrayList<>();
        NioBenchmarkRunner runner = runnerWithWriteFactory(channel -> cappedSocketWrite(
                channel,
                3,
                aggregateWritePositions
        ));
        try (ScriptedRespServer server = ScriptedRespServer.authAndSelectAware()) {
            BenchmarkStatistics statistics = execute(
                    server,
                    PING,
                    2,
                    1,
                    2,
                    true,
                    System::nanoTime,
                    NioBenchmarkClient.ReplyLimits.defaults(),
                    "benchmark-user",
                    "secret",
                    2,
                    runner
            );

            Assert.assertEquals(
                    List.of(4),
                    server.awaitCommandCountsPerConnection(4, 1, SERVER_WAIT)
            );
            Assert.assertEquals(
                    List.of("AUTH", "SELECT", "PING", "PING"),
                    server.firstConnectionCommandPrefix(4)
            );
            assertCounters(statistics, 2, 2, 2, 2);
        }

        Assert.assertTrue(aggregateWritePositions.size() > 1);
        long previousPosition = 0;
        for (long position : aggregateWritePositions) {
            Assert.assertTrue(position >= previousPosition);
            Assert.assertTrue(position - previousPosition <= 3);
            previousPosition = position;
        }
        Assert.assertTrue(previousPosition > 3);
    }

    @Test(timeout = 5_000)
    public void resetInsidePartialCommandIsReportedByScriptedServer() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.neverResponding()) {
            try (Socket socket = new Socket()) {
                socket.setSoLinger(true, 0);
                socket.connect(new InetSocketAddress(server.host(), server.port()));
                server.awaitAcceptedConnections(1, SERVER_WAIT);
                socket.getOutputStream().write(ascii(
                        "*2\r\n$4\r\nPING\r\n$5\r\nabc"
                ));
                socket.getOutputStream().flush();
            }

            AssertionError failure = Assert.assertThrows(
                    AssertionError.class,
                    () -> server.awaitCommandCountsPerConnection(
                            1,
                            1,
                            Duration.ofMillis(500)
                    )
            );

            Assert.assertEquals("scripted RESP server failed", failure.getMessage());
            Assert.assertTrue(failure.getCause() instanceof SocketException);
        }
    }

    @Test(timeout = 5_000)
    public void errorReplyDisconnectAndWrongShapeFailTheCase() throws Exception {
        assertExecutionFails(
                ScriptedRespServer.respondingWith("-ERR boom\r\n"),
                "ERR boom",
                0
        );
        assertExecutionFails(
                ScriptedRespServer.closingAfterCommands(1),
                "disconnect",
                0
        );
        assertExecutionFails(
                ScriptedRespServer.respondingWith(":1\r\n"),
                "expected PONG",
                0
        );
        assertExecutionFails(
                ScriptedRespServer.respondingWith("!invalid\r\n"),
                "unsupported reply marker",
                0
        );
    }

    @Test(timeout = 5_000)
    public void coalescedExtraReplyFailsImmediately() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.coalescedPongAndExtraReply()) {
            BenchmarkExecutionException failure = Assert.assertThrows(
                    BenchmarkExecutionException.class,
                    () -> execute(server, 1, 1, 1, System::nanoTime)
            );

            Assert.assertEquals(1, failure.completedReplies());
            Assert.assertTrue(failure.detail().contains("unexpected extra reply"));
            Assert.assertTrue(failure.getMessage().contains("unexpected extra reply"));
            Assert.assertNotNull(failure.getCause());
            Assert.assertEquals(
                    List.of(1),
                    server.awaitCommandCountsPerConnection(1, 1, SERVER_WAIT)
            );
        }
    }

    @Test(timeout = 5_000)
    public void rejectedPrefixFailsWithoutCountingItAsBenchmarkTraffic() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.rejectingAuth(
                "-WRONGPASS invalid password\r\n"
        )) {
            BenchmarkExecutionException failure = Assert.assertThrows(
                    BenchmarkExecutionException.class,
                    () -> execute(
                            server,
                            PING,
                            5,
                            1,
                            2,
                            true,
                            System::nanoTime,
                            NioBenchmarkClient.ReplyLimits.defaults(),
                            "default",
                            "bad",
                            0
                    )
            );

            Assert.assertEquals(0, failure.completedReplies());
            Assert.assertTrue(failure.detail().contains("WRONGPASS"));
            Assert.assertTrue(failure.getMessage().contains("WRONGPASS"));
            Assert.assertEquals(0, server.measuredCommandReplies());
        }
    }

    @Test(timeout = 5_000)
    public void noProgressTimesOutWithStructuredFailure() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.neverResponding()) {
            BenchmarkExecutionException failure = Assert.assertThrows(
                    BenchmarkExecutionException.class,
                    () -> execute(
                            server,
                            PING,
                            3,
                            1,
                            1,
                            true,
                            System::nanoTime,
                            NioBenchmarkClient.ReplyLimits.defaults(),
                            "",
                            "",
                            0,
                            Duration.ofMillis(100)
                    )
            );

            Assert.assertEquals(0, failure.completedReplies());
            Assert.assertTrue(failure.detail().contains("no progress timeout"));
            Assert.assertTrue(failure.getMessage().contains("no progress timeout"));
            Assert.assertNotNull(failure.getCause());
            Assert.assertEquals(
                    List.of(1),
                    server.awaitCommandCountsPerConnection(1, 1, SERVER_WAIT)
            );
        }
    }

    @Test(timeout = 5_000)
    public void cleanupFailureIsSuppressedWithoutMaskingPrimaryFailure() throws Exception {
        IOException cleanupFailure = new IOException("cleanup boom");
        NioBenchmarkRunner runner = new NioBenchmarkRunner(
                System::nanoTime,
                NioBenchmarkClient.ReplyLimits.defaults(),
                System::nanoTime,
                Duration.ofSeconds(30),
                client -> {
                    client.close();
                    throw cleanupFailure;
                }
        );
        try (ScriptedRespServer server = ScriptedRespServer.respondingWith(
                "-ERR primary boom\r\n"
        )) {
            BenchmarkExecutionException failure = Assert.assertThrows(
                    BenchmarkExecutionException.class,
                    () -> execute(
                            server,
                            PING,
                            3,
                            1,
                            1,
                            true,
                            System::nanoTime,
                            NioBenchmarkClient.ReplyLimits.defaults(),
                            "",
                            "",
                            0,
                            runner
                    )
            );

            Assert.assertEquals(0, failure.completedReplies());
            Assert.assertTrue(failure.detail().contains("ERR primary boom"));
            Assert.assertTrue(failure.getMessage().contains("ERR primary boom"));
            Assert.assertArrayEquals(
                    new Throwable[]{cleanupFailure},
                    failure.getCause().getSuppressed()
            );
        }
    }

    @Test(timeout = 5_000)
    public void uncheckedCleanupFailureIsSuppressedWithoutMaskingPrimaryFailure()
            throws Exception {
        RuntimeException cleanupFailure = new RuntimeException("unchecked cleanup boom");
        NioBenchmarkRunner runner = runnerWithCleanup(client -> {
            client.close();
            throw cleanupFailure;
        });
        try (ScriptedRespServer server = ScriptedRespServer.respondingWith(
                "-ERR primary boom\r\n"
        )) {
            BenchmarkExecutionException failure = Assert.assertThrows(
                    BenchmarkExecutionException.class,
                    () -> execute(server, 3, 1, 1, runner)
            );

            Assert.assertEquals(0, failure.completedReplies());
            Assert.assertTrue(failure.detail().contains("ERR primary boom"));
            Assert.assertTrue(failure.getMessage().startsWith(
                    PING.title() + " failed after 0/3 replies"
            ));
            Assert.assertArrayEquals(
                    new Throwable[]{cleanupFailure},
                    failure.getCause().getSuppressed()
            );
        }
    }

    @Test(timeout = 5_000)
    public void uncheckedCleanupWithoutPrimaryUsesTypedFailure() throws Exception {
        RuntimeException cleanupFailure = new RuntimeException("unchecked cleanup only");
        NioBenchmarkRunner runner = runnerWithCleanup(client -> {
            client.close();
            throw cleanupFailure;
        });
        try (ScriptedRespServer server = ScriptedRespServer.immediatePong(1)) {
            BenchmarkExecutionException failure = Assert.assertThrows(
                    BenchmarkExecutionException.class,
                    () -> execute(server, 1, 1, 1, runner)
            );

            Assert.assertEquals(1, failure.completedReplies());
            Assert.assertEquals("unchecked cleanup only", failure.detail());
            Assert.assertSame(cleanupFailure, failure.getCause());
        }
    }

    @Test(timeout = 5_000)
    public void cleanupErrorWithoutPrimaryPropagatesUnchanged() throws Exception {
        AssertionError cleanupFailure = new AssertionError("fatal cleanup");
        NioBenchmarkRunner runner = runnerWithCleanup(client -> {
            client.close();
            throw cleanupFailure;
        });
        try (ScriptedRespServer server = ScriptedRespServer.immediatePong(1)) {
            AssertionError thrown = Assert.assertThrows(
                    AssertionError.class,
                    () -> execute(server, 1, 1, 1, runner)
            );

            Assert.assertSame(cleanupFailure, thrown);
        }
    }

    @Test(timeout = 5_000)
    public void pipelineOneCompletesConfiguredRequestsAcrossClients() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.immediatePong(3)) {
            BenchmarkStatistics statistics = execute(server, 11, 3, 1, System::nanoTime);
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
            BenchmarkStatistics statistics = execute(server, 10, 1, 4, System::nanoTime);

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
            LongSupplier clock = server.coordinatedClock(System::nanoTime);
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
                    System::nanoTime,
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
                    System::nanoTime,
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
            LongSupplier clock
    ) throws Exception {
        return execute(
                server,
                PING,
                requests,
                clients,
                pipeline,
                true,
                clock,
                NioBenchmarkClient.ReplyLimits.defaults()
        );
    }

    private static BenchmarkStatistics execute(
            ScriptedRespServer server,
            int requests,
            int clients,
            int pipeline,
            NioBenchmarkRunner runner
    ) throws Exception {
        return execute(
                server,
                PING,
                requests,
                clients,
                pipeline,
                true,
                System::nanoTime,
                NioBenchmarkClient.ReplyLimits.defaults(),
                "",
                "",
                0,
                runner
        );
    }

    private static BenchmarkStatistics execute(
            ScriptedRespServer server,
            RedisBenchmarkCase testCase,
            int requests,
            int clients,
            int pipeline,
            LongSupplier clock,
            NioBenchmarkClient.ReplyLimits replyLimits
    ) throws Exception {
        return execute(
                server,
                testCase,
                requests,
                clients,
                pipeline,
                true,
                clock,
                replyLimits
        );
    }

    private static BenchmarkStatistics execute(
            ScriptedRespServer server,
            RedisBenchmarkCase testCase,
            int requests,
            int clients,
            int pipeline,
            boolean keepAlive,
            LongSupplier clock,
            NioBenchmarkClient.ReplyLimits replyLimits
    ) throws Exception {
        return execute(
                server,
                testCase,
                requests,
                clients,
                pipeline,
                keepAlive,
                clock,
                replyLimits,
                "",
                "",
                0
        );
    }

    private static BenchmarkStatistics execute(
            ScriptedRespServer server,
            RedisBenchmarkCase testCase,
            int requests,
            int clients,
            int pipeline,
            boolean keepAlive,
            LongSupplier clock,
            NioBenchmarkClient.ReplyLimits replyLimits,
            String username,
            String password,
            int database
    ) throws Exception {
        return execute(
                server,
                testCase,
                requests,
                clients,
                pipeline,
                keepAlive,
                clock,
                replyLimits,
                username,
                password,
                database,
                new NioBenchmarkRunner(clock, replyLimits)
        );
    }

    private static BenchmarkStatistics execute(
            ScriptedRespServer server,
            RedisBenchmarkCase testCase,
            int requests,
            int clients,
            int pipeline,
            boolean keepAlive,
            LongSupplier clock,
            NioBenchmarkClient.ReplyLimits replyLimits,
            String username,
            String password,
            int database,
            Duration noProgressTimeout
    ) throws Exception {
        return execute(
                server,
                testCase,
                requests,
                clients,
                pipeline,
                keepAlive,
                clock,
                replyLimits,
                username,
                password,
                database,
                new NioBenchmarkRunner(
                        clock,
                        replyLimits,
                        System::nanoTime,
                        noProgressTimeout
                )
        );
    }

    private static BenchmarkStatistics execute(
            ScriptedRespServer server,
            RedisBenchmarkCase testCase,
            int requests,
            int clients,
            int pipeline,
            boolean keepAlive,
            LongSupplier clock,
            NioBenchmarkClient.ReplyLimits replyLimits,
            String username,
            String password,
            int database,
            NioBenchmarkRunner runner
    ) throws Exception {
        BenchmarkConfig config = new BenchmarkConfig(
                server.host(),
                server.port(),
                requests,
                clients,
                3,
                pipeline,
                OptionalLong.empty(),
                keepAlive,
                Set.of(testCase.id()),
                3,
                19L,
                BenchmarkFormat.HUMAN,
                username,
                password,
                database
        );
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

    private static NioBenchmarkRunner runnerWithCleanup(
            NioBenchmarkRunner.ClientCleanup clientCleanup
    ) {
        return new NioBenchmarkRunner(
                System::nanoTime,
                NioBenchmarkClient.ReplyLimits.defaults(),
                System::nanoTime,
                Duration.ofSeconds(30),
                clientCleanup
        );
    }

    private static NioBenchmarkRunner runnerWithWriteFactory(
            NioBenchmarkRunner.ClientWriteFactory clientWriteFactory
    ) {
        return new NioBenchmarkRunner(
                System::nanoTime,
                NioBenchmarkClient.ReplyLimits.defaults(),
                System::nanoTime,
                Duration.ofSeconds(30),
                NioBenchmarkClient::close,
                clientWriteFactory
        );
    }

    private static NioBenchmarkClient.GatheringWrite cappedSocketWrite(
            SocketChannel channel,
            int maxBytes,
            List<Long> aggregateWritePositions
    ) {
        AtomicLong aggregatePosition = new AtomicLong();
        return sources -> {
            ByteBuffer[] cappedSources = new ByteBuffer[sources.length];
            int remainingBudget = maxBytes;
            for (int index = 0; index < sources.length; index++) {
                ByteBuffer cappedSource = sources[index].duplicate();
                int allowedBytes = Math.min(remainingBudget, cappedSource.remaining());
                cappedSource.limit(cappedSource.position() + allowedBytes);
                cappedSources[index] = cappedSource;
                remainingBudget -= allowedBytes;
            }
            long written;
            try {
                written = channel.write(cappedSources);
            } finally {
                for (int index = 0; index < sources.length; index++) {
                    sources[index].position(cappedSources[index].position());
                }
            }
            aggregateWritePositions.add(aggregatePosition.addAndGet(written));
            return written;
        };
    }

    private static void assertExecutionFails(
            ScriptedRespServer server,
            String expectedDetail,
            long expectedCompletedReplies
    ) throws Exception {
        try (server) {
            BenchmarkExecutionException failure = Assert.assertThrows(
                    BenchmarkExecutionException.class,
                    () -> execute(server, 3, 1, 1, System::nanoTime)
            );

            Assert.assertEquals(expectedCompletedReplies, failure.completedReplies());
            Assert.assertTrue(failure.detail(), failure.detail().contains(expectedDetail));
            Assert.assertTrue(failure.getMessage().contains(expectedDetail));
            Assert.assertNotNull(failure.getCause());
        }
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

    private static final class ManualClock implements LongSupplier {
        private final AtomicLong nanos = new AtomicLong();
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public long getAsLong() {
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
