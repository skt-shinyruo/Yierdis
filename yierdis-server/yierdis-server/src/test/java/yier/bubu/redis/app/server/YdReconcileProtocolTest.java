package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.memory.YierdisDbHealth;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class YdReconcileProtocolTest {
    private static final String MISCONF_DEGRADED =
            "MISCONF DB is in a degraded state; writes are disabled";

    @Test
    public void degradedReadsSkipExpiredKeysAndReconcileRestoresOnlyTheSelectedDb() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                TestServerConfigs.config("databases", "2", "noCleanup")
        ); Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            Assert.assertEquals("OK", roundTrip(out, in, "SET", "live", "v"));
            Assert.assertEquals("OK", roundTrip(out, in, "SET", "exp", "v", "PXAT", "1"));
            Assert.assertEquals("OK", roundTrip(out, in, "SELECT", "1"));
            Assert.assertEquals("OK", roundTrip(out, in, "SET", "other", "v"));
            Assert.assertEquals("OK", roundTrip(out, in, "SELECT", "0"));
            degrade(server, 0);
            degrade(server, 1);

            Assert.assertEquals("v", roundTrip(out, in, "GET", "live"));
            Assert.assertNull(roundTrip(out, in, "GET", "exp"));
            Assert.assertEquals(new RespError(MISCONF_DEGRADED), roundTrip(out, in, "DEL", "exp"));
            Assert.assertEquals(new RespError(MISCONF_DEGRADED), roundTrip(out, in, "FLUSHDB"));
            Assert.assertEquals(new RespError(MISCONF_DEGRADED), roundTrip(out, in, "SET", "later", "no"));

            Assert.assertEquals("OK", roundTrip(out, in, "SELECT", "1"));
            Assert.assertEquals(new RespError(MISCONF_DEGRADED), roundTrip(out, in, "SET", "other", "v2"));
            Assert.assertEquals("OK", roundTrip(out, in, "YDRECONCILE"));
            Assert.assertEquals("OK", roundTrip(out, in, "SET", "other", "v2"));

            Assert.assertEquals("OK", roundTrip(out, in, "SELECT", "0"));
            Assert.assertEquals(new RespError(MISCONF_DEGRADED), roundTrip(out, in, "SET", "later", "no"));
            Assert.assertEquals("OK", roundTrip(out, in, "YDRECONCILE"));
            Assert.assertEquals("OK", roundTrip(out, in, "SET", "later", "yes"));

            List<Object> info = respArray(roundTrip(out, in, "COMMAND", "INFO", "YDRECONCILE"));
            Assert.assertEquals(1, info.size());
            List<Object> command = respArray(info.get(0));
            Assert.assertEquals("ydreconcile", command.get(0));
            Assert.assertEquals(1L, command.get(1));
            Assert.assertEquals(0L, command.get(3));
            Assert.assertEquals(0L, command.get(4));
            Assert.assertEquals(0L, command.get(5));
        }
    }

    @Test
    public void expireCountUnderflowDegradesUntilYdReconcileRestoresWrites() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                TestServerConfigs.config("noCleanup")
        )) {
            try (Socket socket = new Socket("127.0.0.1", server.port())) {
                socket.setSoTimeout(3000);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                Assert.assertEquals("OK", roundTrip(out, in, "SET", "k", "v", "EX", "60"));
                zeroExpireCount(server);
                // 下溢发生在 commit 之后，回复是 result-unknown：连接关闭，不返回确定的成功或失败。
                RespClientCodec.writeCommand(out, List.of(
                        "PERSIST".getBytes(StandardCharsets.US_ASCII),
                        "k".getBytes(StandardCharsets.US_ASCII)
                ));
                out.flush();
                Assert.assertEquals(-1, in.read());
            }

            try (Socket socket = new Socket("127.0.0.1", server.port())) {
                socket.setSoTimeout(3000);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                Assert.assertEquals(new RespError(MISCONF_DEGRADED), roundTrip(out, in, "SET", "k", "later"));
                Assert.assertEquals("OK", roundTrip(out, in, "YDRECONCILE"));
                Assert.assertEquals("OK", roundTrip(out, in, "SET", "k", "later"));
                Assert.assertEquals("later", roundTrip(out, in, "GET", "k"));
            }
        }
    }

    private static void degrade(YierdisServerBootstrap server, int dbIndex) throws Exception {
        server.executorForTests().executeOwnerTask(() -> {
            try {
                YierdisDb db = (YierdisDb) server.instanceForTests().engines()[dbIndex];
                Method healthMonitor = YierdisDb.class.getDeclaredMethod("healthMonitor");
                healthMonitor.setAccessible(true);
                YierdisDbHealth health = (YierdisDbHealth) healthMonitor.invoke(db);
                health.recordInvariantFailure(new IllegalStateException("derived expire count underflow"));
            } catch (ReflectiveOperationException failure) {
                throw new IllegalStateException(failure);
            }
        }).join();
    }

    private static void zeroExpireCount(YierdisServerBootstrap server) throws Exception {
        server.executorForTests().executeOwnerTask(() -> {
            try {
                YierdisDb db = (YierdisDb) server.instanceForTests().engines()[0];
                Method keyLifecycle = YierdisDb.class.getDeclaredMethod("keyLifecycle");
                keyLifecycle.setAccessible(true);
                Object lifecycle = keyLifecycle.invoke(db);
                Field expireCount = lifecycle.getClass().getDeclaredField("expireCount");
                expireCount.setAccessible(true);
                expireCount.setInt(lifecycle, 0);
            } catch (ReflectiveOperationException failure) {
                throw new IllegalStateException(failure);
            }
        }).join();
    }

    private static Object roundTrip(OutputStream out, InputStream in, String... args) throws IOException {
        RespClientCodec.writeCommand(
                out,
                Arrays.stream(args).map(value -> value.getBytes(StandardCharsets.UTF_8)).toList()
        );
        out.flush();
        return respValue(RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES));
    }

    private static Object respValue(RespClientCodec.RespReply reply) {
        return switch (reply.kind()) {
            case SIMPLE_STRING -> reply.text();
            case ERROR -> new RespError(reply.text());
            case INTEGER -> reply.integer();
            case BULK_STRING -> new String(reply.bytes(), StandardCharsets.UTF_8);
            case NULL -> null;
            case ARRAY, MAP, SET -> reply.values().stream().map(YdReconcileProtocolTest::respValue).toList();
        };
    }

    @SuppressWarnings("unchecked")
    private static List<Object> respArray(Object value) {
        Assert.assertTrue("expected RESP array, got " + value, value instanceof List<?>);
        return (List<Object>) value;
    }

    private record RespError(String message) {
    }
}
