package yier.bubu.redis.protocol.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespCommand;

import java.nio.charset.StandardCharsets;

public class RespCommandDecoderStrictnessTest {
    @Test
    public void resp3TypePrefixesAreParsedAsInlineCommandInsteadOfProtocolError() {
        // Redis 兼容：top-level 非 array 会按 inline command 解析，因此这些前缀不应再触发 "expected array"。
        String[] lines = new String[]{
                "_\r\n",     // null（RESP3）
                "%0\r\n",    // map（RESP3）
                "#t\r\n",    // boolean（RESP3）
                ",3.14\r\n", // double（RESP3）
                "(123\r\n",  // big number（RESP3）
                "~0\r\n",    // set（RESP3）
                ">0\r\n",    // push（RESP3）
                "=0\r\n",    // verbatim（RESP3）
                "!0\r\n"     // blob error（RESP3）
        };

        for (String s : lines) {
            EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
            try {
                ch.writeInbound(Unpooled.wrappedBuffer(ascii(s)));
                RespCommand cmd = ch.readInbound();
                Assert.assertNotNull("expected a decoded inline command", cmd);
                Assert.assertEquals(1, cmd.argc());
                Assert.assertArrayEquals(ascii(s.trim()), cmd.toByteArray(0));
                cmd.recycle();
            } finally {
                finishQuietly(ch);
            }
        }
    }

    @Test
    public void resp3AttributesPrefixAloneWaitsForMoreDataInsteadOfThrowing() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
        try {
            boolean produced = ch.writeInbound(Unpooled.wrappedBuffer(new byte[]{'|'}));
            Assert.assertFalse(produced);
            Assert.assertNull(ch.readInbound());
        } finally {
            finishQuietly(ch);
        }
    }

    @Test
    public void rejectsControlBytesAsInvalidRequest() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
        try {
            try {
                ch.writeInbound(Unpooled.wrappedBuffer(new byte[]{0x01}));
                Assert.fail("expected decoder to reject control byte");
            } catch (Throwable t) {
                String msg = unwrapMessage(t);
                Assert.assertEquals("Protocol error: invalid request", msg);
            }
        } finally {
            finishQuietly(ch);
        }
    }

    @Test
    public void inlineCommandWithLeadingSpacesIsStillAccepted() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
        try {
            ch.writeInbound(Unpooled.wrappedBuffer(ascii("  PING\r\n")));
            RespCommand cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertEquals(1, cmd.argc());
            Assert.assertEquals('P', cmd.byteAt(0, 0));
            cmd.recycle();
        } finally {
            finishQuietly(ch);
        }
    }

    private static void finishQuietly(EmbeddedChannel ch) {
        if (ch == null) {
            return;
        }
        try {
            ch.finishAndReleaseAll();
        } catch (Throwable ignored) {
            // Best-effort：移除 decoder，避免 close 阶段重复触发解码异常导致测试失败。
            try {
                ch.pipeline().remove(RespCommandDecoder.class);
            } catch (Throwable ignored2) {
                // 忽略
            }
            try {
                ch.finishAndReleaseAll();
            } catch (Throwable ignored3) {
                // 忽略
            }
        }
    }

    private static String unwrapMessage(Throwable t) {
        Throwable cur = t;
        while (cur instanceof DecoderException && cur.getCause() != null) {
            cur = cur.getCause();
        }
        return cur.getMessage();
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
