package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespMap;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespObjectParser;
import yier.bubu.redis.protocol.netty.NettyRespFrame;
import yier.bubu.redis.protocol.netty.RespCommandDecoder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class Resp3CollectionReplyTest {
    @Test
    public void resp3HgetallMemoryStatsAndSmembersUseMapOrSetReplies() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            // Switch to RESP3.
            ch.writeInbound(Unpooled.wrappedBuffer(concat(
                    ascii("*2\r\n"),
                    bulk(ascii("HELLO")),
                    bulk(ascii("3"))
            )));
            byte[] hello = readOutbound(ch);
            Assert.assertTrue("HELLO 3 must reply with a RESP3 map", hello.length > 0 && hello[0] == '%');

            // HSET h f v
            ch.writeInbound(Unpooled.wrappedBuffer(concat(
                    ascii("*4\r\n"),
                    bulk(ascii("HSET")),
                    bulk(ascii("h")),
                    bulk(ascii("f")),
                    bulk(ascii("v"))
            )));
            Assert.assertArrayEquals(ascii(":1\r\n"), readOutbound(ch));

            // HGETALL h => RESP3 map
            ch.writeInbound(Unpooled.wrappedBuffer(concat(
                    ascii("*2\r\n"),
                    bulk(ascii("HGETALL")),
                    bulk(ascii("h"))
            )));
            Assert.assertArrayEquals(
                    concat(ascii("%1\r\n"), bulk(ascii("f")), bulk(ascii("v"))),
                    readOutbound(ch)
            );

            // SADD s a
            ch.writeInbound(Unpooled.wrappedBuffer(concat(
                    ascii("*3\r\n"),
                    bulk(ascii("SADD")),
                    bulk(ascii("s")),
                    bulk(ascii("a"))
            )));
            Assert.assertArrayEquals(ascii(":1\r\n"), readOutbound(ch));

            // SMEMBERS s => RESP3 set
            ch.writeInbound(Unpooled.wrappedBuffer(concat(
                    ascii("*2\r\n"),
                    bulk(ascii("SMEMBERS")),
                    bulk(ascii("s"))
            )));
            Assert.assertArrayEquals(
                    concat(ascii("~1\r\n"), bulk(ascii("a"))),
                    readOutbound(ch)
            );

            // MEMORY STATS => RESP3 map
            ch.writeInbound(Unpooled.wrappedBuffer(concat(
                    ascii("*2\r\n"),
                    bulk(ascii("MEMORY")),
                    bulk(ascii("STATS"))
            )));
            RespObject stats = parseReply(readOutbound(ch));
            Assert.assertTrue(stats instanceof RespMap);
            Map<String, String> map = toStringMap((RespMap) stats);
            Assert.assertTrue(map.containsKey("maxmemory_bytes"));
            Assert.assertTrue(map.containsKey("used_bytes_for_maxmemory"));
            Assert.assertTrue(map.containsKey("heap_data_bytes_estimate"));
            Assert.assertTrue(map.containsKey("offheap_used_bytes"));
            Assert.assertTrue(map.containsKey("total_estimated_bytes"));

            assertLong(map.get("maxmemory_bytes"));
            assertLong(map.get("used_bytes_for_maxmemory"));
            assertLong(map.get("heap_data_bytes_estimate"));
            assertLong(map.get("offheap_used_bytes"));
            assertLong(map.get("total_estimated_bytes"));
        }
    }

    private static final class TestEnv implements AutoCloseable {
        private final YierdisDb db;
        private final NettyCommandExecutor executor;
        private final EmbeddedChannel ch;

        private TestEnv() {
            this.db = new YierdisDb();
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            this.executor = new NettyCommandExecutor(
                    db,
                    processor,
                    ImmediateEventExecutor.INSTANCE,
                    1024,
                    0,
                    256,
                    128,
                    0,
                    0,
                    1024,
                    10
            );
            executor.start();
            this.ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
        }

        @Override
        public void close() {
            try {
                executor.close();
            } finally {
                db.shutdown();
                ch.finishAndReleaseAll();
            }
        }
    }

    private static Map<String, String> toStringMap(RespMap map) {
        Map<String, String> out = new HashMap<>();
        for (RespMap.Entry e : map.entries()) {
            Assert.assertTrue(e.key() instanceof RespBulkString);
            Assert.assertTrue(e.value() instanceof RespBulkString);
            out.put(((RespBulkString) e.key()).asString(), ((RespBulkString) e.value()).asString());
        }
        return out;
    }

    private static void assertLong(String s) {
        Assert.assertNotNull(s);
        Long.parseLong(s);
    }

    private static RespObject parseReply(byte[] replyBytes) {
        NettyRespFrame frame = new NettyRespFrame(Unpooled.wrappedBuffer(replyBytes));
        try {
            return RespObjectParser.parse(frame);
        } finally {
            frame.close();
        }
    }

    private static byte[] readOutbound(EmbeddedChannel ch) {
        Object out = ch.readOutbound();
        Assert.assertNotNull("expected reply", out);
        Assert.assertTrue(out instanceof ByteBuf);
        ByteBuf buf = (ByteBuf) out;
        try {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return bytes;
        } finally {
            buf.release();
        }
    }

    private static byte[] bulk(byte[] data) {
        if (data == null) {
            return ascii("$-1\r\n");
        }
        byte[] header = ascii("$" + data.length + "\r\n");
        return concat(header, data, ascii("\r\n"));
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) {
            total += p.length;
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }
}

