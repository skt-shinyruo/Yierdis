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
    public void rejectsResp3TypePrefixesAsProtocolErrorInsteadOfInlineCommand() {
        byte[] prefixes = new byte[]{
                '_', // null（RESP3）
                '%', // map（RESP3）
                '#', // boolean（RESP3）
                ',', // double（RESP3）
                '(', // big number（RESP3）
                '~', // set（RESP3）
                '>', // push（RESP3）
                '=', // verbatim（RESP3）
                '!'  // blob error（RESP3）
        };

        for (byte p : prefixes) {
            EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
            try {
                try {
                    ch.writeInbound(Unpooled.wrappedBuffer(new byte[]{p}));
                    Assert.fail("expected decoder to reject prefix: " + (char) p);
                } catch (Throwable t) {
                    String msg = unwrapMessage(t);
                    Assert.assertNotNull(msg);
                    Assert.assertTrue("message must be protocol error, got: " + msg, msg.startsWith("Protocol error"));
                }
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
