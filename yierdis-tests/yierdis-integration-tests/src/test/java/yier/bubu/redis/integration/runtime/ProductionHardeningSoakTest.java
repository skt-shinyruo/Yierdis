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

    @Test
    public void runsDeterministicBoundedWorkloadAndLeavesNoOwnershipBehind() throws Exception {
        SoakConfig config = SoakConfig.fromSystemProperties();
        List<Sample> samples = new ArrayList<>();
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
                    long deadlineNanos = startedNanos + config.duration().toNanos();
                    long nextSampleNanos = startedNanos;

                    sample(client, inbound, outbound, replyStats, children, startedNanos, replySequence.get(), samples);
                    exerciseCountedPop(client, replySequence);
                    warmEvictionAndExpiry(client, random, replySequence);
                    exercisePipelinedLargeReplies(client, random, replySequence);
                    exerciseSlowReaderDisconnect(server, random, replySequence);

                    long iteration = 0L;
                    while (System.nanoTime() < deadlineNanos) {
                        exerciseMixedIteration(client, random, iteration, replySequence);
                        if (System.nanoTime() >= nextSampleNanos) {
                            sample(client, inbound, outbound, replyStats, children, startedNanos, replySequence.get(), samples);
                            nextSampleNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100L);
                        }
                        iteration++;
                    }

                    Assert.assertEquals("worker must stay responsive", "+PONG\r\n", execute(client, "PING"));
                    replySequence.incrementAndGet();
                    awaitCommitDrain(client);
                    sample(client, inbound, outbound, replyStats, children, startedNanos, replySequence.get(), samples);
                    Assert.assertTrue("soak must emit multiple samples", samples.size() >= 2);
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
            writeReport(report, config, samples, replySequence.get(), delayedCommitCallbacks.get(), finalOwnership, failure);
        }
    }

    private static String[] serverArgs() {
        return new String[]{
                "--port", "0",
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

    private static void warmEvictionAndExpiry(Socket client, SplittableRandom random, AtomicLong replySequence) throws IOException {
        for (int index = 0; index < 128; index++) {
            assertSimpleString(execute(client, "SET", "soak:evict:" + index, payload(random, 16 * 1024)));
            replySequence.incrementAndGet();
        }
        assertSimpleString(execute(client, "SET", "soak:expiry", "short-lived"));
        replySequence.incrementAndGet();
        assertInteger(execute(client, "PEXPIRE", "soak:expiry", "2"));
        replySequence.incrementAndGet();
        sleepMillis(8L);
        Assert.assertEquals("expired key must not remain visible", "$-1\r\n", execute(client, "GET", "soak:expiry"));
        replySequence.incrementAndGet();
    }

    private static void exerciseMixedIteration(
            Socket client,
            SplittableRandom random,
            long iteration,
            AtomicLong replySequence
    ) throws IOException {
        String suffix = Long.toUnsignedString(random.nextLong(), 36);
        assertSimpleString(execute(client, "SET", "soak:key:" + suffix, payload(random, 4 * 1024)));
        replySequence.incrementAndGet();
        long boundedMember = iteration & 31L;
        assertInteger(execute(
                client,
                "HSET",
                "soak:hash:" + (iteration & 7L),
                "field:" + boundedMember,
                "value:" + suffix
        ));
        replySequence.incrementAndGet();
        assertInteger(execute(
                client,
                "ZADD",
                "soak:zset:" + (iteration & 7L),
                Long.toString(iteration),
                "member:" + boundedMember
        ));
        replySequence.incrementAndGet();
        assertNoError(execute(client, "GET", "soak:key:" + suffix));
        replySequence.incrementAndGet();

        if ((iteration & 15L) == 0L) {
            assertNoError(execute(client, "KEYS", "soak:*") );
            replySequence.incrementAndGet();
            assertNoError(execute(client, "SCAN", "0", "MATCH", "soak:*", "COUNT", "16"));
            replySequence.incrementAndGet();
        }
    }

    private static void exerciseCountedPop(Socket client, AtomicLong replySequence) throws IOException {
        assertInteger(execute(client, "LPUSH", "soak:counted-pop", "one", "two", "three"));
        replySequence.incrementAndGet();
        String popped = execute(client, "LPOP", "soak:counted-pop", "2");
        assertNoError(popped);
        Assert.assertTrue("counted LPOP must produce an aggregate reply: " + popped, popped.startsWith("*2\r\n"));
        replySequence.incrementAndGet();
    }

    private static void exercisePipelinedLargeReplies(Socket client, SplittableRandom random, AtomicLong replySequence)
            throws IOException {
        String value = payload(random, LARGE_REPLY_BYTES);
        assertSimpleString(execute(client, "SET", "soak:large", value));
        replySequence.incrementAndGet();
        RespTcpTestSupport.writePipeline(
                client,
                new String[]{"GET", "soak:large"},
                new String[]{"PING"},
                new String[]{"GET", "soak:large"}
        );
        Assert.assertEquals("first pipelined large reply", value, RespTcpTestSupport.bulkPayload(RespTcpTestSupport.readFrame(client)));
        Assert.assertEquals("pipelined PING must remain between large replies", "+PONG\r\n", RespTcpTestSupport.readFrame(client));
        Assert.assertEquals("second pipelined large reply", value, RespTcpTestSupport.bulkPayload(RespTcpTestSupport.readFrame(client)));
        replySequence.addAndGet(3L);
    }

    private static void exerciseSlowReaderDisconnect(
            YierdisServerBootstrap server,
            SplittableRandom random,
            AtomicLong replySequence
    ) throws IOException, InterruptedException {
        try (Socket slow = RespTcpTestSupport.connect(server)) {
            slow.setReceiveBufferSize(1_024);
            String value = payload(random, LARGE_REPLY_BYTES);
            assertSimpleString(execute(slow, "SET", "soak:slow-reader", value));
            replySequence.incrementAndGet();
            RespTcpTestSupport.writeCommand(slow, "GET", "soak:slow-reader");
            Assert.assertEquals("slow reader must receive reply start", '$', slow.getInputStream().read());
            slow.setSoLinger(true, 0);
        }
        awaitSlowReaderCleanup(
                server.outboundMemoryBudgetForTests(),
                server.replyEgressStatsForTests(),
                server.childChannelRegistryForTests()
        );
    }

    private static void sample(
            Socket client,
            InboundMemoryBudget inbound,
            OutboundMemoryBudget outbound,
            ReplyEgressStats replyStats,
            ChildChannelRegistry children,
            long startedNanos,
            long replySequence,
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
                replySequence
        );
        assertSampleWithinBounds(sample, inboundStats, outboundStats);
        samples.add(sample);
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

    private static String payload(SplittableRandom random, int length) {
        char[] chars = new char[length];
        char seed = (char) ('a' + random.nextInt(26));
        for (int index = 0; index < chars.length; index++) {
            chars[index] = (char) ('a' + ((seed - 'a' + index) % 26));
        }
        return new String(chars);
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
            long replySequence,
            long delayedCommitCallbacks,
            FinalOwnership finalOwnership,
            Throwable failure
    ) throws IOException {
        List<String> lines = new ArrayList<>(samples.size() + 2);
        lines.add("{\"type\":\"metadata\",\"seed\":" + config.seed()
                + ",\"durationSeconds\":" + config.duration().toSeconds()
                + ",\"argv\":\"" + json(String.join(" ", serverArgs())) + "\""
                + ",\"java\":\"" + json(System.getProperty("java.version")) + "\""
                + ",\"os\":\"" + json(System.getProperty("os.name") + " " + System.getProperty("os.arch")) + "\""
                + ",\"commit\":\"" + json(System.getProperty("yierdis.soak.commit", "unknown")) + "\"}");
        for (Sample sample : samples) {
            lines.add(sample.toJson());
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
            long orderingSequence
    ) {
        private long[] numericValues() {
            return new long[]{
                    elapsedMillis, heapBytes, nativeBytes, maxmemoryUsedBytes, inboundReservedBytes,
                    commitReservedEvents, commitReservedBytes, outboundReservedBytes, outboundAllocatedBytes,
                    activeReplySlots, activeReplySources, activeReplyChunks, childChannels, rejects, orderingSequence
            };
        }

        private String toJson() {
            return String.format(
                    Locale.ROOT,
                    "{\"type\":\"sample\",\"elapsedMillis\":%d,\"heapBytes\":%d,\"nativeBytes\":%d,"
                            + "\"maxmemoryUsedBytes\":%d,\"inboundReservedBytes\":%d,\"commitReservedEvents\":%d,"
                            + "\"commitReservedBytes\":%d,\"outboundReservedBytes\":%d,\"outboundAllocatedBytes\":%d,"
                            + "\"activeReplySlots\":%d,\"activeReplySources\":%d,\"activeReplyChunks\":%d,"
                            + "\"childChannels\":%d,\"rejects\":%d,\"orderingSequence\":%d}",
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
                    orderingSequence
            );
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
