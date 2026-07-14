package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ObservationClientTest {
    @Test
    public void formatsArrayRepliesAsKeyValuePairs() {
        RespClientCodec.RespReply reply = array(
                bulk("queued_tasks"),
                integer(3),
                bulk("queued_bytes"),
                integer(9)
        );

        Assert.assertEquals("queued_tasks=3; queued_bytes=9", ObservationClient.formatReply(reply));
    }

    @Test
    public void formatsBulkRepliesAsUtf8Text() {
        RespClientCodec.RespReply reply = bulk("# Server\nversion:1\n");

        Assert.assertEquals("# Server\nversion:1\n", ObservationClient.formatReply(reply));
    }

    @Test
    public void formatsScalarNullAndNestedReplies() {
        Assert.assertEquals("", ObservationClient.formatReply(null));
        Assert.assertEquals("OK", ObservationClient.formatReply(simple("OK")));
        Assert.assertEquals("ERR unknown command", ObservationClient.formatReply(error("ERR unknown command")));
        Assert.assertEquals("42", ObservationClient.formatReply(integer(42)));
        Assert.assertEquals("null", ObservationClient.formatReply(nullReply()));
        Assert.assertEquals("outer=inner=7", ObservationClient.formatReply(array(bulk("outer"), array(bulk("inner"), integer(7)))));
    }

    @Test
    public void formatsOddLengthArraysWithEmptyValue() {
        Assert.assertEquals("queued_tasks=", ObservationClient.formatReply(array(bulk("queued_tasks"))));
    }

    @Test
    public void snapshotStoresImmutableDeterministicValues() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("z", "last");
        values.put("a", "first");
        ObservationSnapshot snapshot = new ObservationSnapshot(values);
        values.put("m", "mutated");

        Assert.assertEquals(List.of("a", "z"), List.copyOf(snapshot.values().keySet()));
        Assert.assertEquals("first", snapshot.values().get("a"));
        Assert.assertEquals("last", snapshot.values().get("z"));
        Assert.assertFalse(snapshot.values().containsKey("m"));
        Assert.assertThrows(UnsupportedOperationException.class, () -> snapshot.values().put("b", "value"));
    }

    @Test
    public void snapshotAcceptsNullMapAsEmptyAndRejectsNullEntries() {
        Assert.assertTrue(new ObservationSnapshot(null).values().isEmpty());
        Assert.assertTrue(ObservationSnapshot.empty().values().isEmpty());

        Map<String, String> nullKey = new LinkedHashMap<>();
        nullKey.put(null, "value");
        Assert.assertThrows(NullPointerException.class, () -> new ObservationSnapshot(nullKey));

        Map<String, String> nullValue = new LinkedHashMap<>();
        nullValue.put("key", null);
        Assert.assertThrows(NullPointerException.class, () -> new ObservationSnapshot(nullValue));
    }

    @Test
    public void extractsOutboundReplyGaugesFromYierdisInfoWithoutDiscardingRawInfo() {
        String info = "# Stats\n"
                + "yierdis_outbound_reserved_bytes:128\n"
                + "yierdis_outbound_allocated_bytes:64\n"
                + "yierdis_outbound_peak_reserved_bytes:1024\n"
                + "yierdis_outbound_peak_allocated_bytes:512\n"
                + "yierdis_outbound_capacity_rejects:3\n"
                + "yierdis_outbound_waiting_connections:2\n"
                + "yierdis_outbound_write_failures:1\n"
                + "yierdis_outbound_active_slots:not-a-number\n";

        ObservationSnapshot snapshot = new ObservationSnapshot(Map.of("INFO", info),
                ObservationClient.extractOutboundReplyGauges(info));

        Assert.assertEquals(info, snapshot.values().get("INFO"));
        Assert.assertEquals(Map.of(
                "outbound_reserved_bytes", 128L,
                "outbound_allocated_bytes", 64L,
                "outbound_peak_reserved_bytes", 1_024L,
                "outbound_peak_allocated_bytes", 512L,
                "outbound_capacity_rejects", 3L,
                "outbound_waiting_connections", 2L,
                "outbound_write_failures", 1L
        ), snapshot.outboundReplyGauges());
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> snapshot.outboundReplyGauges().put("outbound_reserved_bytes", 0L));
    }

    @Test
    public void captureRejectsInvalidHostAndPort() {
        ObservationClient client = new ObservationClient();

        Assert.assertThrows(NullPointerException.class, () -> client.capture(null, 6379));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.capture(" ", 6379));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.capture("localhost", 0));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.capture("localhost", 65536));
    }

    @Test
    public void captureRedisObservationDoesNotRequireStats() throws Exception {
        try (RedisSuiteTestSupport.RedisLikeObservationServer server = RedisSuiteTestSupport.RedisLikeObservationServer.start()) {
            Assert.assertTrue(server.awaitListening());
            SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "", "", 0);

            ObservationSnapshot snapshot = new ObservationClient().capture(artifact);

            Assert.assertTrue(snapshot.values().containsKey("INFO"));
            Assert.assertTrue(snapshot.values().containsKey("MEMORY STATS"));
            Assert.assertFalse(snapshot.values().containsKey("STATS"));
            Assert.assertEquals(List.of("INFO", "MEMORY STATS"), List.copyOf(snapshot.values().keySet()));
            Assert.assertTrue(server.awaitCommands(2).containsAll(List.of("INFO", "MEMORY STATS")));
        }
    }

    @Test
    public void captureRedisObservationAuthenticatesAndSelectsConfiguredDb() throws Exception {
        try (RedisSuiteTestSupport.RedisLikeObservationServer server = RedisSuiteTestSupport.RedisLikeObservationServer.start()) {
            Assert.assertTrue(server.awaitListening());
            SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "bench-user", "bench-secret", 4);

            ObservationSnapshot snapshot = new ObservationClient().capture(artifact);

            Assert.assertTrue(snapshot.values().containsKey("INFO"));
            Assert.assertTrue(snapshot.values().containsKey("MEMORY STATS"));
            Assert.assertEquals(List.of(
                    "AUTH BENCH-USER BENCH-SECRET", "SELECT 4", "INFO",
                    "AUTH BENCH-USER BENCH-SECRET", "SELECT 4", "MEMORY STATS"
            ), server.awaitCommands(6));
        }
    }

    @Test
    public void captureEnvironmentMetadataAuthenticatesAndSelectsConfiguredDb() throws Exception {
        try (RedisSuiteTestSupport.RedisLikeObservationServer server = RedisSuiteTestSupport.RedisLikeObservationServer.start()) {
            Assert.assertTrue(server.awaitListening());
            SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "bench-user", "bench-secret", 4);

            Map<String, String> metadata = new ObservationClient().captureEnvironmentMetadata(artifact);

            Assert.assertTrue(metadata.containsKey("redis.info.server"));
            Assert.assertEquals(List.of("AUTH BENCH-USER BENCH-SECRET", "SELECT 4", "INFO"), server.awaitCommands(3));
        }
    }

    @Test
    public void captureRedisObservationReportsBootstrapSelectFailure() throws Exception {
        try (RedisSuiteTestSupport.RedisLikeObservationServer server =
                     RedisSuiteTestSupport.RedisLikeObservationServer.start(Map.of("SELECT 4", errorBytes("ERR DB index is out of range")))) {
            Assert.assertTrue(server.awaitListening());
            SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "bench-user", "bench-secret", 4);

            ObservationSnapshot snapshot = new ObservationClient().capture(artifact);

            Assert.assertTrue(snapshot.values().get("INFO").contains("ERR DB index is out of range"));
            Assert.assertTrue(snapshot.values().get("MEMORY STATS").contains("ERR DB index is out of range"));
            Assert.assertEquals(List.of(
                    "AUTH BENCH-USER BENCH-SECRET", "SELECT 4",
                    "AUTH BENCH-USER BENCH-SECRET", "SELECT 4"
            ), server.awaitCommands(4));
        }
    }

    @Test
    public void captureEnvironmentMetadataReportsBootstrapAuthFailure() throws Exception {
        try (RedisSuiteTestSupport.RedisLikeObservationServer server =
                     RedisSuiteTestSupport.RedisLikeObservationServer.start(Map.of("AUTH BENCH-USER BENCH-SECRET", errorBytes("WRONGPASS invalid username-password pair")))) {
            Assert.assertTrue(server.awaitListening());
            SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "bench-user", "bench-secret", 4);

            Map<String, String> metadata = new ObservationClient().captureEnvironmentMetadata(artifact);

            Assert.assertTrue(metadata.get("redis.info.server").contains("WRONGPASS invalid username-password pair"));
            Assert.assertEquals(List.of("AUTH BENCH-USER BENCH-SECRET"), server.awaitCommands(1));
        }
    }

    private static RespClientCodec.RespReply simple(String text) {
        return new RespClientCodec.RespReply(RespClientCodec.RespReply.Kind.SIMPLE_STRING, text, null, null, null);
    }

    private static RespClientCodec.RespReply error(String text) {
        return new RespClientCodec.RespReply(RespClientCodec.RespReply.Kind.ERROR, text, null, null, null);
    }

    private static RespClientCodec.RespReply integer(long value) {
        return new RespClientCodec.RespReply(RespClientCodec.RespReply.Kind.INTEGER, null, null, value, null);
    }

    private static RespClientCodec.RespReply bulk(String value) {
        return bulk(value.getBytes(StandardCharsets.UTF_8));
    }

    private static RespClientCodec.RespReply bulk(byte[] bytes) {
        return new RespClientCodec.RespReply(RespClientCodec.RespReply.Kind.BULK_STRING, null, bytes, null, null);
    }

    private static RespClientCodec.RespReply nullReply() {
        return new RespClientCodec.RespReply(RespClientCodec.RespReply.Kind.NULL, null, null, null, null);
    }

    private static RespClientCodec.RespReply array(RespClientCodec.RespReply... values) {
        return new RespClientCodec.RespReply(RespClientCodec.RespReply.Kind.ARRAY, null, null, null, List.of(values));
    }

    private static byte[] errorBytes(String message) {
        return ("-" + message + "\r\n").getBytes(StandardCharsets.US_ASCII);
    }
}
