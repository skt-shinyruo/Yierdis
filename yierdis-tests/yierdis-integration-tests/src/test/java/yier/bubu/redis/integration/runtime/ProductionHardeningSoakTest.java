package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.integration.protocol.RespTcpTestSupport;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudgetStats;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class ProductionHardeningSoakTest {
    private static final long MAXMEMORY_BYTES = 2L * 1024L * 1024L;
    private static final long REPLY_GLOBAL_BYTES = 2L * 1024L * 1024L;
    private static final long REPLY_CONNECTION_BYTES = 1L * 1024L * 1024L;
    private static final long REPLY_MAX_BYTES = 512L * 1024L;
    private static final int COMMIT_MAX_EVENTS = 4_096;
    private static final long COMMIT_MAX_BYTES = 8L * 1024L * 1024L;
    private static final int LARGE_REPLY_BYTES = 64 * 1024;
    private static final int SOAK_CYCLE_COUNT = 4;
    private static final long WARM_PAGE_BOUND_BYTES = 23L * 64L * 1024L;
    private static final long MIXED_ITERATION_INTERVAL_MILLIS = 25L;
    private static final long RSS_MONOTONIC_GROWTH_TOLERANCE_BYTES = 16L * 1024L * 1024L;

    @Test
    public void reportSamplesIncludeRssAndCycleBaselineTelemetry() {
        Sample sample = new Sample(
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L
        );

        String json = sample.toJson();
        Assert.assertTrue("soak sample must record RSS", json.contains("\"rssBytes\":"));
        Assert.assertTrue("soak sample must identify its workload cycle", json.contains("\"cycle\":"));
    }

    @Test
    public void allowsMinorRssGrowthAcrossCompletedCycles() {
        assertFinalThreeRssDoNotGrowMonotonically(List.of(
                new CycleCompletion(0L, 0L, 0L, 0L, 0L, 100L * 1024L * 1024L),
                new CycleCompletion(1L, 0L, 0L, 0L, 0L, 101L * 1024L * 1024L),
                new CycleCompletion(2L, 0L, 0L, 0L, 0L, 102L * 1024L * 1024L),
                new CycleCompletion(3L, 0L, 0L, 0L, 0L, 103L * 1024L * 1024L)
        ));
    }

    @Test
    public void rejectsMaterialMonotonicRssGrowthAcrossCompletedCycles() {
        try {
            assertFinalThreeRssDoNotGrowMonotonically(List.of(
                    new CycleCompletion(0L, 0L, 0L, 0L, 0L, 100L * 1024L * 1024L),
                    new CycleCompletion(1L, 0L, 0L, 0L, 0L, 110L * 1024L * 1024L),
                    new CycleCompletion(2L, 0L, 0L, 0L, 0L, 120L * 1024L * 1024L),
                    new CycleCompletion(3L, 0L, 0L, 0L, 0L, 130L * 1024L * 1024L)
            ));
            Assert.fail("material monotonic RSS growth must fail the soak invariant");
        } catch (AssertionError expected) {
            Assert.assertTrue(expected.getMessage().contains("RSS"));
        }
    }

    @Test
    public void runsDeterministicBoundedWorkloadAndLeavesNoOwnershipBehind() throws Exception {
        SoakConfig config = SoakConfig.fromSystemProperties();
        List<Sample> samples = new ArrayList<>();
        List<CycleCompletion> completedCycles = new ArrayList<>();
        AtomicLong delayedCommitCallbacks = new AtomicLong();
        AtomicLong replySequence = new AtomicLong();
        Path report = reportPath(config);
        Throwable failure = null;
        FinalOwnership finalOwnership = FinalOwnership.unavailable();

        YierdisServerBootstrap server = YierdisServerBootstrap.startForTests(
                executionEngine -> executionEngine,
                builder -> builder
                        .changeSink(event -> {
                            delayedCommitCallbacks.incrementAndGet();
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
                        })
                        .commitStreamMaxEvents(COMMIT_MAX_EVENTS)
                        .commitStreamMaxRetainedBytes(COMMIT_MAX_BYTES)
                        .commitStreamShutdownTimeoutMillis(5_000L),
                serverArgs()
        );
        InboundMemoryBudget inbound = server.inboundMemoryBudgetForTests();
        OutboundMemoryBudget outbound = server.outboundMemoryBudgetForTests();
        ReplyEgressStats replyStats = server.replyEgressStatsForTests();
        ChildChannelRegistry children = server.childChannelRegistryForTests();
        try {
            try (server) {
                try (Socket client = RespTcpTestSupport.connect(server)) {
                    SplittableRandom random = new SplittableRandom(config.seed());
                    long startedNanos = System.nanoTime();
                    long durationNanos = config.duration().toNanos();
                    CycleCompletion baseline = null;

                    for (int cycle = 0; cycle < SOAK_CYCLE_COUNT; cycle++) {
                        String prefix = "soak:cycle:" + cycle;
                        long cycleDeadlineNanos = startedNanos + durationNanos * (cycle + 1L) / SOAK_CYCLE_COUNT;
                        long nextSampleNanos = System.nanoTime();

                        warmEvictionAndExpiry(client, random, replySequence, prefix);
                        exerciseCountedPop(client, replySequence, prefix);
                        exercisePipelinedLargeReplies(client, random, replySequence, prefix);
                        exerciseSlowReaderDisconnect(server, random, replySequence, prefix);
                        String[] mixedPayloads = payloadPool(random, 64, 4 * 1024);
                        sample(
                                client,
                                inbound,
                                outbound,
                                replyStats,
                                children,
                                startedNanos,
                                replySequence.get(),
                                cycle,
                                samples
                        );

                        long iteration = 0L;
                        while (System.nanoTime() < cycleDeadlineNanos) {
                            exerciseMixedIteration(client, iteration, replySequence, prefix, mixedPayloads);
                            if (System.nanoTime() >= nextSampleNanos) {
                                sample(
                                        client,
                                        inbound,
                                        outbound,
                                        replyStats,
                                        children,
                                        startedNanos,
                                        replySequence.get(),
                                        cycle,
                                        samples
                                );
                                nextSampleNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100L);
                            }
                            iteration++;
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(MIXED_ITERATION_INTERVAL_MILLIS));
                        }

                        cleanupCycle(client, replySequence, prefix);
                        awaitCommitDrain(client);
                        awaitCycleOwnership(inbound, outbound, replyStats, children);
                        Sample completionSample = sample(
                                client,
                                inbound,
                                outbound,
                                replyStats,
                                children,
                                startedNanos,
                                replySequence.get(),
                                cycle,
                                samples
                        );
                        CycleCompletion completion = CycleCompletion.from(cycle, completionSample);
                        if (baseline == null) {
                            baseline = completion;
                        } else {
                            assertCycleReturnedToBaseline(
                                    baseline,
                                    completion,
                                    highestNativeMetadataCommittedBytes(samples)
                            );
                        }
                        completedCycles.add(completion);
                    }

                    Assert.assertEquals("worker must stay responsive", "+PONG\r\n", execute(client, "PING"));
                    replySequence.incrementAndGet();
                    awaitCommitDrain(client);
                    assertFinalThreeRssDoNotGrowMonotonically(completedCycles);
                    Assert.assertEquals("soak must complete each configured cycle", SOAK_CYCLE_COUNT, completedCycles.size());
                    Assert.assertTrue("soak must emit multiple samples", samples.size() >= SOAK_CYCLE_COUNT);
                    Assert.assertTrue("commit sink delay was not exercised", delayedCommitCallbacks.get() > 0L);
                }
                awaitTerminalOwnership(inbound, outbound, replyStats, children);
            }
            finalOwnership = FinalOwnership.capture(inbound, outbound, replyStats, children);
            finalOwnership.assertZero();
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            if (finalOwnership.inboundReservedBytes() < 0L) {
                finalOwnership = FinalOwnership.capture(inbound, outbound, replyStats, children);
            }
            writeReport(
                    report,
                    config,
                    samples,
                    completedCycles,
                    replySequence.get(),
                    delayedCommitCallbacks.get(),
                    finalOwnership,
                    failure
            );
        }
    }

    private static String[] serverArgs() {
        return new String[]{
                "--port", "0",
                "--databases", "1",
                "--cleanupIntervalMillis", "5",
                "--executorQueueCapacity", "512",
                "--executorQueueMaxBytes", "1048576",
                "--executorSchedulingPolicy", "fair",
                "--protocolGlobalInFlightBytes", "1048576",
                "--replyGlobalCapacityBytes", Long.toString(REPLY_GLOBAL_BYTES),
                "--replyPerConnectionCapacityBytes", Long.toString(REPLY_CONNECTION_BYTES),
                "--replyMaxTotalBytes", Long.toString(REPLY_MAX_BYTES),
                "--replyChunkPayloadBytes", "16384",
                "--replyControlReservationBytes", "4096",
                "--replyDrainTimeoutMillis", "5000",
                "--maxmemoryBytes", Long.toString(MAXMEMORY_BYTES),
                "--maxmemoryScope", "global",
                "--maxmemoryPolicy", "allkeys-lru",
                "--maxmemorySamples", "256",
                "--evictionTimeLimitMillis", "100"
        };
    }

    private static void warmEvictionAndExpiry(
            Socket client,
            SplittableRandom random,
            AtomicLong replySequence,
            String prefix
    ) throws IOException {
        String evictionPayload = payload(random, 16 * 1024);
        for (int index = 0; index < 128; index++) {
            assertSimpleString(execute(client, "SET", prefix + ":evict:" + index, evictionPayload));
            replySequence.incrementAndGet();
        }
        assertSimpleString(execute(client, "SET", prefix + ":expiry", "short-lived"));
        replySequence.incrementAndGet();
        assertInteger(execute(client, "PEXPIRE", prefix + ":expiry", "2"));
        replySequence.incrementAndGet();
        sleepMillis(8L);
        Assert.assertEquals("expired key must not remain visible", "$-1\r\n", execute(client, "GET", prefix + ":expiry"));
        replySequence.incrementAndGet();
    }

    private static void exerciseMixedIteration(
            Socket client,
            long iteration,
            AtomicLong replySequence,
            String prefix,
            String[] payloads
    ) throws IOException {
        int payloadIndex = (int) (iteration & 63L);
        assertSimpleString(execute(
                client,
                "SET",
                prefix + ":key:" + payloadIndex,
                payloads[payloadIndex]
        ));
        replySequence.incrementAndGet();
        long boundedMember = iteration & 31L;
        assertInteger(execute(
                client,
                "HSET",
                prefix + ":hash:" + (iteration & 7L),
                "field:" + boundedMember,
                "value:" + boundedMember
        ));
        replySequence.incrementAndGet();
        assertInteger(execute(
                client,
                "ZADD",
                prefix + ":zset:" + (iteration & 7L),
                Long.toString(iteration),
                "member:" + boundedMember
        ));
        replySequence.incrementAndGet();
        assertNoError(execute(client, "GET", prefix + ":key:" + (iteration & 63L)));
        replySequence.incrementAndGet();

        if ((iteration & 15L) == 0L) {
            assertNoError(execute(client, "KEYS", prefix + ":*") );
            replySequence.incrementAndGet();
            assertNoError(execute(client, "SCAN", "0", "MATCH", prefix + ":*", "COUNT", "16"));
            replySequence.incrementAndGet();
        }
    }

    private static void exerciseCountedPop(Socket client, AtomicLong replySequence, String prefix) throws IOException {
        String key = prefix + ":counted-pop";
        assertInteger(execute(client, "LPUSH", key, "one", "two", "three"));
        replySequence.incrementAndGet();
        String popped = execute(client, "LPOP", key, "2");
        assertNoError(popped);
        Assert.assertTrue("counted LPOP must produce an aggregate reply: " + popped, popped.startsWith("*2\r\n"));
        replySequence.incrementAndGet();
    }

    private static void exercisePipelinedLargeReplies(
            Socket client,
            SplittableRandom random,
            AtomicLong replySequence,
            String prefix
    )
            throws IOException {
        String value = payload(random, LARGE_REPLY_BYTES);
        String key = prefix + ":large";
        assertSimpleString(execute(client, "SET", key, value));
        replySequence.incrementAndGet();
        RespTcpTestSupport.writePipeline(
                client,
                new String[]{"GET", key},
                new String[]{"PING"},
                new String[]{"GET", key}
        );
        Assert.assertEquals("first pipelined large reply", value, RespTcpTestSupport.bulkPayload(RespTcpTestSupport.readFrame(client)));
        Assert.assertEquals("pipelined PING must remain between large replies", "+PONG\r\n", RespTcpTestSupport.readFrame(client));
        Assert.assertEquals("second pipelined large reply", value, RespTcpTestSupport.bulkPayload(RespTcpTestSupport.readFrame(client)));
        replySequence.addAndGet(3L);
    }

    private static void exerciseSlowReaderDisconnect(
            YierdisServerBootstrap server,
            SplittableRandom random,
            AtomicLong replySequence,
            String prefix
    ) throws IOException, InterruptedException {
        try (Socket slow = RespTcpTestSupport.connect(server)) {
            slow.setReceiveBufferSize(1_024);
            String value = payload(random, LARGE_REPLY_BYTES);
            String key = prefix + ":slow-reader";
            assertSimpleString(execute(slow, "SET", key, value));
            replySequence.incrementAndGet();
            RespTcpTestSupport.writeCommand(slow, "GET", key);
            Assert.assertEquals("slow reader must receive reply start", '$', slow.getInputStream().read());
            slow.setSoLinger(true, 0);
        }
        awaitSlowReaderCleanup(
                server.outboundMemoryBudgetForTests(),
                server.replyEgressStatsForTests(),
                server.childChannelRegistryForTests()
        );
    }

    private static void cleanupCycle(Socket client, AtomicLong replySequence, String prefix) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int index = 0; index < 128; index++) {
            keys.add(prefix + ":evict:" + index);
        }
        for (int index = 0; index < 64; index++) {
            keys.add(prefix + ":key:" + index);
        }
        for (int index = 0; index < 8; index++) {
            keys.add(prefix + ":hash:" + index);
            keys.add(prefix + ":zset:" + index);
        }
        keys.add(prefix + ":counted-pop");
        keys.add(prefix + ":large");
        keys.add(prefix + ":slow-reader");
        keys.add(prefix + ":expiry");

        String[] command = new String[keys.size() + 1];
        command[0] = "DEL";
        for (int index = 0; index < keys.size(); index++) {
            command[index + 1] = keys.get(index);
        }
        assertInteger(execute(client, command));
        replySequence.incrementAndGet();
    }

    private static Sample sample(
            Socket client,
            InboundMemoryBudget inbound,
            OutboundMemoryBudget outbound,
            ReplyEgressStats replyStats,
            ChildChannelRegistry children,
            long startedNanos,
            long replySequence,
            long cycle,
            List<Sample> samples
    ) throws IOException {
        Map<String, Long> memory = numericInfo(execute(client, "INFO", "memory"));
        Map<String, Long> stats = numericInfo(execute(client, "INFO", "stats"));
        InboundMemoryBudgetStats inboundStats = inbound.stats();
        OutboundMemoryBudgetStats outboundStats = outbound.stats();
        ReplyEgressStats.Snapshot egress = replyStats.snapshot();

        Sample sample = new Sample(
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos),
                Math.max(0L, Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()),
                required(memory, "yierdis_offheap_used_bytes"),
                required(memory, "yierdis_maxmemory_used_bytes"),
                inboundStats.reservedBytes(),
                required(stats, "yierdis_commit_stream_reserved_events"),
                required(stats, "yierdis_commit_stream_reserved_bytes"),
                outboundStats.reservedBytes(),
                outboundStats.allocatedBytes(),
                outboundStats.activeSlots(),
                egress.activeSources(),
                egress.activeChunks(),
                children.activeChannelCount(),
                saturatedAdd(
                        saturatedAdd(inboundStats.rejectedConnections(), outboundStats.capacityRejects()),
                        required(stats, "yierdis_commit_stream_rejected_writes")
                ),
                replySequence,
                readRssBytes(),
                required(memory, "yierdis_native_metadata_committed_bytes"),
                required(memory, "yierdis_native_data_committed_bytes"),
                required(memory, "yierdis_native_live_objects"),
                required(memory, "yierdis_native_live_regions"),
                cycle
        );
        assertSampleWithinBounds(sample, inboundStats, outboundStats);
        samples.add(sample);
        return sample;
    }

    private static void assertSampleWithinBounds(
            Sample sample,
            InboundMemoryBudgetStats inbound,
            OutboundMemoryBudgetStats outbound
    ) {
        for (long value : sample.numericValues()) {
            Assert.assertTrue("sample contains a negative counter: " + sample, value >= 0L);
        }
        Assert.assertTrue("inbound admission exceeded configured capacity: " + sample,
                sample.inboundReservedBytes() <= inbound.capacityBytes());
        Assert.assertTrue("outbound reservation exceeded configured capacity: " + sample,
                sample.outboundReservedBytes() <= REPLY_GLOBAL_BYTES);
        Assert.assertTrue("outbound allocated bytes exceeded reservation: " + sample,
                sample.outboundAllocatedBytes() <= sample.outboundReservedBytes());
        Assert.assertTrue("commit stream event capacity exceeded: " + sample,
                sample.commitReservedEvents() <= COMMIT_MAX_EVENTS);
        Assert.assertTrue("commit stream byte capacity exceeded: " + sample,
                sample.commitReservedBytes() <= COMMIT_MAX_BYTES);
        Assert.assertTrue("maxmemory usage exceeded configured limit: " + sample,
                sample.maxmemoryUsedBytes() <= MAXMEMORY_BYTES);
    }

    private static void awaitCycleOwnership(
            InboundMemoryBudget inbound,
            OutboundMemoryBudget outbound,
            ReplyEgressStats replyStats,
            ChildChannelRegistry children
    ) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            InboundMemoryBudgetStats inboundStats = inbound.stats();
            OutboundMemoryBudgetStats outboundStats = outbound.stats();
            ReplyEgressStats.Snapshot egress = replyStats.snapshot();
            if (inboundStats.reservedBytes() == inboundStats.readCreditBytes()
                    && inboundStats.retainedInputCapacityBytes() == 0L
                    && inboundStats.consolidationBytes() == 0L
                    && outboundStats.reservedBytes() == 0L
                    && outboundStats.allocatedBytes() == 0L
                    && outboundStats.activeSlots() == 0L
                    && egress.activeChunks() == 0L
                    && egress.activeSources() == 0L
                    && children.activeChannelCount() <= 1) {
                return;
            }
            Thread.sleep(10L);
        }
        Assert.fail("cycle ownership did not converge: inbound=" + inbound.stats()
                + ", outbound=" + outbound.stats()
                + ", reply=" + replyStats.snapshot()
                + ", children=" + children.activeChannelCount());
    }

    private static void awaitTerminalOwnership(
            InboundMemoryBudget inbound,
            OutboundMemoryBudget outbound,
            ReplyEgressStats replyStats,
            ChildChannelRegistry children
    ) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            InboundMemoryBudgetStats inboundStats = inbound.stats();
            OutboundMemoryBudgetStats outboundStats = outbound.stats();
            ReplyEgressStats.Snapshot egress = replyStats.snapshot();
            if (inboundStats.reservedBytes() == 0L
                    && inboundStats.readCreditBytes() == 0L
                    && inboundStats.retainedInputCapacityBytes() == 0L
                    && inboundStats.consolidationBytes() == 0L
                    && outboundStats.reservedBytes() == 0L
                    && outboundStats.allocatedBytes() == 0L
                    && outboundStats.activeSlots() == 0L
                    && egress.activeChunks() == 0L
                    && egress.activeSources() == 0L
                    && children.activeChannelCount() <= 1) {
                return;
            }
            Thread.sleep(10L);
        }
        Assert.fail("ownership did not converge: inbound=" + inbound.stats()
                + ", outbound=" + outbound.stats()
                + ", reply=" + replyStats.snapshot()
                + ", children=" + children.activeChannelCount());
    }

    private static void awaitSlowReaderCleanup(
            OutboundMemoryBudget outbound,
            ReplyEgressStats replyStats,
            ChildChannelRegistry children
    ) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            OutboundMemoryBudgetStats outboundStats = outbound.stats();
            ReplyEgressStats.Snapshot egress = replyStats.snapshot();
            if (outboundStats.reservedBytes() == 0L
                    && outboundStats.allocatedBytes() == 0L
                    && outboundStats.activeSlots() == 0L
                    && egress.activeChunks() == 0L
                    && egress.activeSources() == 0L
                    && children.activeChannelCount() <= 1) {
                return;
            }
            Thread.sleep(10L);
        }
        Assert.fail("slow reader ownership did not converge: outbound=" + outbound.stats()
                + ", reply=" + replyStats.snapshot()
                + ", children=" + children.activeChannelCount());
    }

    private static void awaitCommitDrain(Socket client) throws IOException, InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            String frame = execute(client, "INFO", "stats");
            String body = RespTcpTestSupport.bulkPayload(frame);
            Assert.assertFalse("commit stream worker failed: " + body, body.contains("yierdis_commit_stream_state:FAILED"));
            Map<String, Long> stats = numericInfo(frame);
            if (required(stats, "yierdis_commit_stream_reserved_events") == 0L
                    && required(stats, "yierdis_commit_stream_reserved_bytes") == 0L
                    && required(stats, "yierdis_commit_stream_callback_active") == 0L) {
                return;
            }
            Thread.sleep(10L);
        }
        Assert.fail("commit stream did not drain before shutdown");
    }

    private static String execute(Socket client, String... command) throws IOException {
        RespTcpTestSupport.writeCommand(client, command);
        String frame = RespTcpTestSupport.readFrame(client);
        assertNoError(frame);
        return frame;
    }

    private static void assertSimpleString(String frame) {
        Assert.assertEquals("expected simple OK reply", "+OK\r\n", frame);
    }

    private static void assertInteger(String frame) {
        assertNoError(frame);
        Assert.assertTrue("expected integer reply: " + frame, frame.startsWith(":"));
    }

    private static void assertNoError(String frame) {
        Assert.assertFalse("unexpected command error: " + frame, frame.startsWith("-"));
    }

    private static Map<String, Long> numericInfo(String bulkFrame) {
        String body = RespTcpTestSupport.bulkPayload(bulkFrame);
        Map<String, Long> values = new LinkedHashMap<>();
        for (String line : body.split("\\r?\\n")) {
            int separator = line.indexOf(':');
            if (separator <= 0) {
                continue;
            }
            String rawValue = line.substring(separator + 1);
            try {
                values.put(line.substring(0, separator), Long.parseLong(rawValue));
            } catch (NumberFormatException ignored) {
                // INFO includes textual section and state fields; numeric fields are sampled below.
            }
        }
        return values;
    }

    private static long required(Map<String, Long> values, String key) {
        Long value = values.get(key);
        Assert.assertNotNull("missing required INFO sample field: " + key, value);
        return value;
    }

    private static long readRssBytes() throws IOException {
        for (String line : Files.readAllLines(Path.of("/proc/self/status"), StandardCharsets.UTF_8)) {
            if (!line.startsWith("VmRSS:")) {
                continue;
            }
            String[] fields = line.trim().split("\\s+");
            if (fields.length < 2) {
                break;
            }
            try {
                return Math.multiplyExact(Long.parseLong(fields[1]), 1024L);
            } catch (ArithmeticException | NumberFormatException failure) {
                throw new IOException("invalid VmRSS line: " + line, failure);
            }
        }
        throw new IOException("VmRSS is unavailable from /proc/self/status");
    }

    private static long highestNativeMetadataCommittedBytes(List<Sample> samples) {
        long highest = 0L;
        for (Sample sample : samples) {
            highest = Math.max(highest, sample.nativeMetadataCommittedBytes());
        }
        return highest;
    }

    private static void assertCycleReturnedToBaseline(
            CycleCompletion baseline,
            CycleCompletion completion,
            long metadataHighWaterBytes
    ) {
        Assert.assertEquals(
                "native live objects must return to the warm cycle baseline",
                baseline.nativeLiveObjects(),
                completion.nativeLiveObjects()
        );
        Assert.assertEquals(
                "native live regions must return to the warm cycle baseline",
                baseline.nativeLiveRegions(),
                completion.nativeLiveRegions()
        );
        long committedBound = saturatedAdd(metadataHighWaterBytes, WARM_PAGE_BOUND_BYTES);
        Assert.assertTrue(
                "native committed bytes exceed metadata high-water plus warm-page bound: " + completion,
                completion.nativeCommittedBytes() <= committedBound
        );
    }

    private static void assertFinalThreeRssDoNotGrowMonotonically(List<CycleCompletion> completions) {
        Assert.assertTrue("soak needs one warmup and three measured cycles", completions.size() >= 4);
        CycleCompletion first = completions.get(completions.size() - 3);
        CycleCompletion second = completions.get(completions.size() - 2);
        CycleCompletion third = completions.get(completions.size() - 1);
        boolean growsMonotonically = first.rssBytes() <= second.rssBytes()
                && second.rssBytes() <= third.rssBytes()
                && third.rssBytes() - first.rssBytes() > RSS_MONOTONIC_GROWTH_TOLERANCE_BYTES;
        Assert.assertFalse(
                "RSS grew materially and monotonically across the final three completed cycles: "
                        + first.rssBytes() + ", " + second.rssBytes() + ", " + third.rssBytes()
                        + " (tolerance=" + RSS_MONOTONIC_GROWTH_TOLERANCE_BYTES + ")",
                growsMonotonically
        );
    }

    private static String payload(SplittableRandom random, int length) {
        char[] chars = new char[length];
        char seed = (char) ('a' + random.nextInt(26));
        for (int index = 0; index < chars.length; index++) {
            chars[index] = (char) ('a' + ((seed - 'a' + index) % 26));
        }
        return new String(chars);
    }

    private static String[] payloadPool(SplittableRandom random, int count, int payloadLength) {
        String[] payloads = new String[count];
        for (int index = 0; index < payloads.length; index++) {
            payloads[index] = payload(random, payloadLength);
        }
        return payloads;
    }

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for expiry", interrupted);
        }
    }

    private static Path reportPath(SoakConfig config) throws IOException {
        Path directory = Path.of(System.getProperty("yierdis.soak.reportDir", "target/production-hardening-soak"));
        Files.createDirectories(directory);
        return directory.resolve("soak-" + config.seed() + "-" + System.currentTimeMillis() + ".jsonl");
    }

    private static void writeReport(
            Path report,
            SoakConfig config,
            List<Sample> samples,
            List<CycleCompletion> completedCycles,
            long replySequence,
            long delayedCommitCallbacks,
            FinalOwnership finalOwnership,
            Throwable failure
    ) throws IOException {
        List<String> lines = new ArrayList<>(samples.size() + completedCycles.size() + 2);
        lines.add("{\"type\":\"metadata\",\"seed\":" + config.seed()
                + ",\"durationSeconds\":" + config.duration().toSeconds()
                + ",\"cycleCount\":" + SOAK_CYCLE_COUNT
                + ",\"argv\":\"" + json(String.join(" ", serverArgs())) + "\""
                + ",\"java\":\"" + json(System.getProperty("java.version")) + "\""
                + ",\"os\":\"" + json(System.getProperty("os.name") + " " + System.getProperty("os.arch")) + "\""
                + ",\"commit\":\"" + json(System.getProperty("yierdis.soak.commit", "unknown")) + "\"}");
        for (Sample sample : samples) {
            lines.add(sample.toJson());
        }
        for (CycleCompletion completion : completedCycles) {
            lines.add(completion.toJson());
        }
        lines.add(finalOwnership.toJson(replySequence, delayedCommitCallbacks, failure));
        Files.write(report, lines, StandardCharsets.UTF_8);
    }

    private static String json(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    private static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private record SoakConfig(long seed, Duration duration) {
        private static SoakConfig fromSystemProperties() {
            long seed = Long.parseLong(System.getProperty("yierdis.soak.seed", "20260710"));
            long seconds = Long.parseLong(System.getProperty("yierdis.soak.durationSeconds", "15"));
            if (seconds <= 0L) {
                throw new IllegalArgumentException("yierdis.soak.durationSeconds must be > 0");
            }
            return new SoakConfig(seed, Duration.ofSeconds(seconds));
        }
    }

    private record Sample(
            long elapsedMillis,
            long heapBytes,
            long nativeBytes,
            long maxmemoryUsedBytes,
            long inboundReservedBytes,
            long commitReservedEvents,
            long commitReservedBytes,
            long outboundReservedBytes,
            long outboundAllocatedBytes,
            long activeReplySlots,
            long activeReplySources,
            long activeReplyChunks,
            long childChannels,
            long rejects,
            long orderingSequence,
            long rssBytes,
            long nativeMetadataCommittedBytes,
            long nativeDataCommittedBytes,
            long nativeLiveObjects,
            long nativeLiveRegions,
            long cycle
    ) {
        private long[] numericValues() {
            return new long[]{
                    elapsedMillis, heapBytes, nativeBytes, maxmemoryUsedBytes, inboundReservedBytes,
                    commitReservedEvents, commitReservedBytes, outboundReservedBytes, outboundAllocatedBytes,
                    activeReplySlots, activeReplySources, activeReplyChunks, childChannels, rejects, orderingSequence,
                    rssBytes, nativeMetadataCommittedBytes, nativeDataCommittedBytes, nativeLiveObjects,
                    nativeLiveRegions, cycle
            };
        }

        private String toJson() {
            return String.format(
                    Locale.ROOT,
                    "{\"type\":\"sample\",\"elapsedMillis\":%d,\"heapBytes\":%d,\"nativeBytes\":%d,"
                            + "\"maxmemoryUsedBytes\":%d,\"inboundReservedBytes\":%d,\"commitReservedEvents\":%d,"
                            + "\"commitReservedBytes\":%d,\"outboundReservedBytes\":%d,\"outboundAllocatedBytes\":%d,"
                            + "\"activeReplySlots\":%d,\"activeReplySources\":%d,\"activeReplyChunks\":%d,"
                            + "\"childChannels\":%d,\"rejects\":%d,\"orderingSequence\":%d,\"rssBytes\":%d,"
                            + "\"nativeMetadataCommittedBytes\":%d,\"nativeDataCommittedBytes\":%d,"
                            + "\"nativeLiveObjects\":%d,\"nativeLiveRegions\":%d,\"cycle\":%d}",
                    elapsedMillis,
                    heapBytes,
                    nativeBytes,
                    maxmemoryUsedBytes,
                    inboundReservedBytes,
                    commitReservedEvents,
                    commitReservedBytes,
                    outboundReservedBytes,
                    outboundAllocatedBytes,
                    activeReplySlots,
                    activeReplySources,
                    activeReplyChunks,
                    childChannels,
                    rejects,
                    orderingSequence,
                    rssBytes,
                    nativeMetadataCommittedBytes,
                    nativeDataCommittedBytes,
                    nativeLiveObjects,
                    nativeLiveRegions,
                    cycle
            );
        }
    }

    private record CycleCompletion(
            long cycle,
            long nativeLiveObjects,
            long nativeLiveRegions,
            long nativeMetadataCommittedBytes,
            long nativeDataCommittedBytes,
            long rssBytes
    ) {
        private static CycleCompletion from(long cycle, Sample sample) {
            return new CycleCompletion(
                    cycle,
                    sample.nativeLiveObjects(),
                    sample.nativeLiveRegions(),
                    sample.nativeMetadataCommittedBytes(),
                    sample.nativeDataCommittedBytes(),
                    sample.rssBytes()
            );
        }

        private long nativeCommittedBytes() {
            return saturatedAdd(nativeMetadataCommittedBytes, nativeDataCommittedBytes);
        }

        private String toJson() {
            return "{\"type\":\"cycle\",\"cycle\":" + cycle
                    + ",\"nativeLiveObjects\":" + nativeLiveObjects
                    + ",\"nativeLiveRegions\":" + nativeLiveRegions
                    + ",\"nativeMetadataCommittedBytes\":" + nativeMetadataCommittedBytes
                    + ",\"nativeDataCommittedBytes\":" + nativeDataCommittedBytes
                    + ",\"rssBytes\":" + rssBytes + "}";
        }
    }

    private record FinalOwnership(
            long inboundReservedBytes,
            long inboundReadCreditBytes,
            long inboundRetainedInputBytes,
            long inboundConsolidationBytes,
            long outboundReservedBytes,
            long outboundAllocatedBytes,
            long outboundActiveSlots,
            long activeReplySources,
            long activeReplyChunks,
            long childChannels
    ) {
        private static FinalOwnership unavailable() {
            return new FinalOwnership(-1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L);
        }

        private static FinalOwnership capture(
                InboundMemoryBudget inbound,
                OutboundMemoryBudget outbound,
                ReplyEgressStats replyStats,
                ChildChannelRegistry children
        ) {
            InboundMemoryBudgetStats inboundStats = inbound.stats();
            OutboundMemoryBudgetStats outboundStats = outbound.stats();
            ReplyEgressStats.Snapshot egress = replyStats.snapshot();
            return new FinalOwnership(
                    inboundStats.reservedBytes(),
                    inboundStats.readCreditBytes(),
                    inboundStats.retainedInputCapacityBytes(),
                    inboundStats.consolidationBytes(),
                    outboundStats.reservedBytes(),
                    outboundStats.allocatedBytes(),
                    outboundStats.activeSlots(),
                    egress.activeSources(),
                    egress.activeChunks(),
                    children.activeChannelCount()
            );
        }

        private void assertZero() {
            for (long value : values()) {
                Assert.assertEquals("final ownership must be zero", 0L, value);
            }
        }

        private String toJson(long replySequence, long delayedCommitCallbacks, Throwable failure) {
            return "{\"type\":\"final\",\"inboundReservedBytes\":" + inboundReservedBytes
                    + ",\"inboundReadCreditBytes\":" + inboundReadCreditBytes
                    + ",\"inboundRetainedInputBytes\":" + inboundRetainedInputBytes
                    + ",\"inboundConsolidationBytes\":" + inboundConsolidationBytes
                    + ",\"outboundReservedBytes\":" + outboundReservedBytes
                    + ",\"outboundAllocatedBytes\":" + outboundAllocatedBytes
                    + ",\"outboundActiveSlots\":" + outboundActiveSlots
                    + ",\"activeReplySources\":" + activeReplySources
                    + ",\"activeReplyChunks\":" + activeReplyChunks
                    + ",\"childChannels\":" + childChannels
                    + ",\"orderingSequence\":" + replySequence
                    + ",\"delayedCommitCallbacks\":" + delayedCommitCallbacks
                    + ",\"failure\":\"" + json(failure == null ? "" : failure.toString()) + "\"}";
        }

        private long[] values() {
            return new long[]{
                    inboundReservedBytes,
                    inboundReadCreditBytes,
                    inboundRetainedInputBytes,
                    inboundConsolidationBytes,
                    outboundReservedBytes,
                    outboundAllocatedBytes,
                    outboundActiveSlots,
                    activeReplySources,
                    activeReplyChunks,
                    childChannels
            };
        }
    }
}
