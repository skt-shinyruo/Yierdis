package yier.bubu.redis.protocol.resp;

import java.util.function.BiFunction;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.RedisReplyRenderer;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationSink;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RedisReplyRespContractTest {
    private final RespReplySizer sizer = new RespReplySizer();
    private final BiFunction<Integer, BytesSink, RedisReplyWriter> writerFactory = RespReplyWriter::new;

    @Test
    public void scalarBulkAndNullRepliesHaveExactRespPlans() {
        for (ReplyFixture fixture : scalarFixtures()) {
            assertExactContract(fixture);
        }
    }

    @Test
    public void mapsUseResp2FallbacksAndResp3NativeTypes() {
        for (ReplyFixture fixture : aggregateFixtures()) {
            assertExactContract(fixture);
        }
    }

    @Test
    public void streamedSequencesAndMapsKeepSizingAndEmissionInLockstep() {
        for (ReplyFixture fixture : streamedFixtures()) {
            assertExactContract(fixture);
        }
    }

    @Test
    public void controlErrorsUseMaximumPlanningAndTheControlReservation() {
        RedisReply reply = RedisReplies.controlError(
                "OOM command not allowed when used memory > 'maxmemory'.");

        for (int version : new int[]{2, 3}) {
            ReplyPlan plan = sizer.apply(version, reply.shape());
            ControlTrackingSink sink = new ControlTrackingSink();
            RedisReplyWriter writer = writerFactory.apply(version, sink);

            RedisReplyRenderer.render(reply, writer);

            Assert.assertTrue("RESP" + version + " control plan", plan.reserveMaximum());
            Assert.assertEquals("RESP" + version + " captured version", version, plan.protocolVersion());
            Assert.assertEquals(0L, plan.encodedUpperBoundBytes());
            Assert.assertEquals(0L, plan.retainedSourceBytes());
            Assert.assertTrue("RESP" + version + " control reservation", sink.controlReservationUsed);
            Assert.assertEquals(
                    "-OOM command not allowed when used memory > 'maxmemory'.\r\n",
                    sink.utf8());
        }
    }

    @Test
    public void controlErrorsCannotBeNestedInsideOrdinaryAggregates() {
        RedisReply controlError = RedisReplies.controlError("OOM rejected");

        Assert.assertThrows(IllegalArgumentException.class,
                () -> RedisReplies.array(List.of(controlError)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> RedisReplies.map(List.of(RedisReplies.integer(1), controlError)));
    }

    @Test
    public void contractFixturesCoverEveryPermittedRedisReplyVariant() {
        Set<Class<?>> covered = new HashSet<>();
        for (ReplyFixture fixture : allOrdinaryFixtures()) {
            covered.add(fixture.reply().getClass());
        }
        covered.add(RedisReplies.controlError("ERR control").getClass());

        Set<Class<?>> permitted = new HashSet<>(
                Arrays.asList(RedisReply.class.getPermittedSubclasses()));
        Assert.assertEquals(permitted, covered);
    }

    private void assertExactContract(ReplyFixture fixture) {
        assertExactContract(fixture, 2, fixture.resp2());
        assertExactContract(fixture, 3, fixture.resp3());
    }

    private void assertExactContract(ReplyFixture fixture, int version, String expectedWire) {
        ReplyPlan plan = sizer.apply(version, fixture.reply().shape());
        ByteArraySink sink = new ByteArraySink();
        RedisReplyWriter writer = writerFactory.apply(version, sink);

        RedisReplyRenderer.render(fixture.reply(), writer);

        byte[] actual = sink.bytes();
        String context = fixture.name() + " RESP" + version;
        Assert.assertFalse(context + " must not reserve maximum", plan.reserveMaximum());
        Assert.assertEquals(context + " captured version", version, plan.protocolVersion());
        Assert.assertEquals(context + " retained bytes",
                fixture.retainedSourceBytes(), plan.retainedSourceBytes());
        Assert.assertTrue(context + " plan must bound encoded bytes",
                plan.encodedUpperBoundBytes() >= actual.length);
        Assert.assertEquals(context + " exact encoded bytes",
                plan.encodedUpperBoundBytes(), actual.length);
        Assert.assertEquals(context + " wire bytes", expectedWire, sink.utf8());
    }

    private static List<ReplyFixture> allOrdinaryFixtures() {
        List<ReplyFixture> fixtures = new ArrayList<>();
        fixtures.addAll(scalarFixtures());
        fixtures.addAll(aggregateFixtures());
        fixtures.addAll(streamedFixtures());
        return fixtures;
    }

    private static List<ReplyFixture> scalarFixtures() {
        return List.of(
                fixture("simple string", RedisReplies.simpleString("OK"),
                        "+OK\r\n", "+OK\r\n"),
                fixture("error", RedisReplies.error("wrong"),
                        "-ERR wrong\r\n", "-ERR wrong\r\n"),
                fixture("integer", RedisReplies.integer(-42L),
                        ":-42\r\n", ":-42\r\n"),
                fixture("retained bulk string",
                        RedisReplies.bulkString(4, 13L, sink -> sink.bulkString(bytes("data"))),
                        "$4\r\ndata\r\n", "$4\r\ndata\r\n", 13L),
                fixture("null value", RedisReplies.nullValue(),
                        "$-1\r\n", "_\r\n"),
                fixture("null array", RedisReplies.nullArray(),
                        "*-1\r\n", "_\r\n")
        );
    }

    private static List<ReplyFixture> aggregateFixtures() {
        return List.of(
                fixture("map",
                        RedisReplies.map(List.of(
                                RedisReplies.bulkString(bytes("key")),
                                RedisReplies.integer(7L)
                        )),
                        "*2\r\n$3\r\nkey\r\n:7\r\n",
                        "%1\r\n$3\r\nkey\r\n:7\r\n"),
                // RESP2/RESP3 长度差异被放大的复合形状：头与 null 在两个版本下字节数都不同。
                // contractFixturesCoverEveryPermittedRedisReplyVariant 强制每个未来新回复变体进入本契约测试，
                // 届时任一版本编码更长（如未来 RESP3 类型）都会被逐版本精确相等的断言守住。
                fixture("wide map with null value",
                        RedisReplies.map(List.of(
                                RedisReplies.bulkString(bytes("k0")), RedisReplies.integer(0L),
                                RedisReplies.bulkString(bytes("k1")), RedisReplies.integer(1L),
                                RedisReplies.bulkString(bytes("k2")), RedisReplies.integer(2L),
                                RedisReplies.bulkString(bytes("k3")), RedisReplies.integer(3L),
                                RedisReplies.bulkString(bytes("k4")), RedisReplies.integer(4L),
                                RedisReplies.bulkString(bytes("k5")), RedisReplies.integer(5L),
                                RedisReplies.bulkString(bytes("k6")), RedisReplies.integer(6L),
                                RedisReplies.bulkString(bytes("k7")), RedisReplies.nullValue()
                        )),
                        "*16\r\n$2\r\nk0\r\n:0\r\n$2\r\nk1\r\n:1\r\n"
                                + "$2\r\nk2\r\n:2\r\n$2\r\nk3\r\n:3\r\n"
                                + "$2\r\nk4\r\n:4\r\n$2\r\nk5\r\n:5\r\n"
                                + "$2\r\nk6\r\n:6\r\n$2\r\nk7\r\n$-1\r\n",
                        "%8\r\n$2\r\nk0\r\n:0\r\n$2\r\nk1\r\n:1\r\n"
                                + "$2\r\nk2\r\n:2\r\n$2\r\nk3\r\n:3\r\n"
                                + "$2\r\nk4\r\n:4\r\n$2\r\nk5\r\n:5\r\n"
                                + "$2\r\nk6\r\n:6\r\n$2\r\nk7\r\n_\r\n")
        );
    }

    private static List<ReplyFixture> streamedFixtures() {
        RedisReply sequence = RedisReplies.sequence(3, 17L, consumer -> {
            consumer.accept(1);
            consumer.accept(-1);
            consumer.accept(3);
        }, sink -> {
            sink.bulkString(bytes("a"));
            sink.bulkStringNull();
            sink.bulkString(bytes("xyz"));
        });
        RedisReply map = RedisReplies.byteMap(2, 23L, consumer -> {
            consumer.accept(1);
            consumer.accept(1);
            consumer.accept(1);
            consumer.accept(-1);
        }, sink -> {
            sink.bulkString(bytes("k"));
            sink.bulkString(bytes("v"));
            sink.bulkString(bytes("n"));
            sink.bulkStringNull();
        });
        RedisReply set = RedisReplies.byteSet(2, 29L, consumer -> {
            consumer.accept(1);
            consumer.accept(-1);
        }, sink -> {
            sink.bulkString(bytes("a"));
            sink.bulkStringNull();
        });

        return List.of(
                fixture("streamed sequence", sequence,
                        "*3\r\n$1\r\na\r\n$-1\r\n$3\r\nxyz\r\n",
                        "*3\r\n$1\r\na\r\n_\r\n$3\r\nxyz\r\n", 17L),
                fixture("streamed set", set,
                        "*2\r\n$1\r\na\r\n$-1\r\n",
                        "~2\r\n$1\r\na\r\n_\r\n", 29L),
                fixture("streamed map", map,
                        "*4\r\n$1\r\nk\r\n$1\r\nv\r\n$1\r\nn\r\n$-1\r\n",
                        "%2\r\n$1\r\nk\r\n$1\r\nv\r\n$1\r\nn\r\n_\r\n", 23L)
        );
    }

    private static ReplyFixture fixture(
            String name,
            RedisReply reply,
            String resp2,
            String resp3
    ) {
        return fixture(name, reply, resp2, resp3, 0L);
    }

    private static ReplyFixture fixture(
            String name,
            RedisReply reply,
            String resp2,
            String resp3,
            long retainedSourceBytes
    ) {
        return new ReplyFixture(name, reply, resp2, resp3, retainedSourceBytes);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private record ReplyFixture(
            String name,
            RedisReply reply,
            String resp2,
            String resp3,
            long retainedSourceBytes
    ) {
    }

    private static class ByteArraySink implements BytesSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void writeBytes(byte[] src, int srcIndex, int len) {
            out.write(src, srcIndex, len);
        }

        byte[] bytes() {
            return out.toByteArray();
        }

        String utf8() {
            return out.toString(StandardCharsets.UTF_8);
        }
    }

    private static final class ControlTrackingSink extends ByteArraySink
            implements ReplyReservationSink {
        private boolean controlReservationUsed;

        @Override
        public void require(ReplyPlan plan) {
        }

        @Override
        public void useControlReservation() {
            controlReservationUsed = true;
        }

        @Override
        public long writtenBytes() {
            return bytes().length;
        }
    }

}
