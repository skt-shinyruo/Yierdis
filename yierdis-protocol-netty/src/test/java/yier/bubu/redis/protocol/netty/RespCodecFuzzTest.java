package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespCommand;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/**
 * 短 fuzz（默认 mvn test 运行）：使用固定 seed + 小 corpus + 随机分片，覆盖半包/粘包恢复与资源释放路径。
 * <p>
 * 目标是兜底“不死循环、不越界、不中途产出错误 frame/command”的关键性质。
 */
public class RespCodecFuzzTest {
    private static final long SEED = 0x5EED_2026_0206L;
    private static final int ITERS = 200;

    @Test
    public void respDecoderRoundTripsCorpusWithRandomFragmentation() {
        SplittableRandom rnd = new SplittableRandom(SEED ^ 0xD3C0D3L);
        List<byte[]> corpus = replyCorpus();

        for (int i = 0; i < ITERS; i++) {
            byte[] payload = corpus.get(rnd.nextInt(corpus.size()));
            byte[] out = decodeReplyWithRandomChunks(payload, rnd);
            Assert.assertArrayEquals("seed=" + SEED + ", iter=" + i, payload, out);
        }
    }

    @Test
    public void respCommandDecoderDecodesCorpusWithRandomFragmentation() {
        SplittableRandom rnd = new SplittableRandom(SEED ^ 0xC0FF_EE02L);
        List<ReqCase> corpus = requestCorpus();

        for (int i = 0; i < ITERS; i++) {
            ReqCase c = corpus.get(rnd.nextInt(corpus.size()));
            RespCommand cmd = decodeCommandWithRandomChunks(c.payload, rnd);
            try {
                Assert.assertNotNull("seed=" + SEED + ", iter=" + i + ", case=" + c.name, cmd);
                Assert.assertEquals("seed=" + SEED + ", iter=" + i + ", case=" + c.name, c.expectedArgs.length, cmd.argc());
                for (int a = 0; a < c.expectedArgs.length; a++) {
                    byte[] expected = c.expectedArgs[a];
                    if (expected == null) {
                        Assert.assertTrue("seed=" + SEED + ", iter=" + i + ", case=" + c.name + ", arg=" + a, cmd.isNull(a));
                        Assert.assertNull("seed=" + SEED + ", iter=" + i + ", case=" + c.name + ", arg=" + a, cmd.toByteArray(a));
                        continue;
                    }
                    Assert.assertFalse("seed=" + SEED + ", iter=" + i + ", case=" + c.name + ", arg=" + a, cmd.isNull(a));
                    Assert.assertArrayEquals("seed=" + SEED + ", iter=" + i + ", case=" + c.name + ", arg=" + a, expected, cmd.toByteArray(a));
                }
            } finally {
                if (cmd != null) {
                    cmd.recycle();
                }
            }
        }
    }

    @Test
    public void partialInputsDoNotProduceFramesOrCommands() {
        SplittableRandom rnd = new SplittableRandom(SEED ^ 0xA11A_BEEFL);

        for (byte[] payload : replyCorpus()) {
            int cut = rnd.nextInt(Math.max(1, payload.length));
            EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
            try {
                ch.writeInbound(Unpooled.wrappedBuffer(payload, 0, cut));
                Assert.assertNull("reply should not produce a frame for partial input", ch.readInbound());
            } finally {
                ch.finishAndReleaseAll();
            }
        }

        for (ReqCase c : requestCorpus()) {
            int cut = rnd.nextInt(Math.max(1, c.payload.length));
            EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
            try {
                ch.writeInbound(Unpooled.wrappedBuffer(c.payload, 0, cut));
                Assert.assertNull("request should not produce a command for partial input", ch.readInbound());
            } finally {
                ch.finishAndReleaseAll();
            }
        }
    }

    private static byte[] decodeReplyWithRandomChunks(byte[] payload, SplittableRandom rnd) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        try {
            feedInRandomChunks(ch, payload, rnd);
            Object msg = ch.readInbound();
            Assert.assertNotNull(msg);
            Assert.assertTrue(msg instanceof NettyRespFrame);
            NettyRespFrame frame = (NettyRespFrame) msg;
            try {
                ByteBuf buf = frame.unwrap();
                byte[] out = new byte[buf.readableBytes()];
                buf.getBytes(buf.readerIndex(), out);
                Assert.assertNull("expected single frame", ch.readInbound());
                return out;
            } finally {
                frame.close();
            }
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static RespCommand decodeCommandWithRandomChunks(byte[] payload, SplittableRandom rnd) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
        try {
            feedInRandomChunks(ch, payload, rnd);
            RespCommand cmd = ch.readInbound();
            Assert.assertNull("expected single command", ch.readInbound());
            return cmd;
        } finally {
            try {
                ch.finishAndReleaseAll();
            } catch (Throwable ignored) {
                // best-effort
            }
        }
    }

    private static void feedInRandomChunks(EmbeddedChannel ch, byte[] payload, SplittableRandom rnd) {
        int pos = 0;
        while (pos < payload.length) {
            int remaining = payload.length - pos;
            int chunk = Math.min(remaining, 1 + rnd.nextInt(Math.min(remaining, 16)));
            ch.writeInbound(Unpooled.wrappedBuffer(payload, pos, chunk));
            pos += chunk;
        }
    }

    private static List<byte[]> replyCorpus() {
        List<byte[]> list = new ArrayList<>();
        list.add(ascii("+OK\r\n"));
        list.add(ascii(":123\r\n"));
        list.add(new byte[]{'$', '3', '\r', '\n', 0, 1, (byte) 0xFF, '\r', '\n'});
        list.add(ascii("$?\r\n;5\r\nhello\r\n;0\r\n"));
        list.add(ascii("*?\r\n:1\r\n:2\r\n.\r\n"));
        list.add(ascii("%?\r\n+key\r\n+value\r\n.\r\n"));
        list.add(ascii("~?\r\n+v\r\n.\r\n"));
        list.add(ascii("|1\r\n+meta\r\n:1\r\n$?\r\n;5\r\nhello\r\n;0\r\n"));
        return list;
    }

    private static List<ReqCase> requestCorpus() {
        List<ReqCase> list = new ArrayList<>();
        list.add(new ReqCase("resp2_bulk_array_ping",
                ascii("*1\r\n$4\r\nPING\r\n"),
                new byte[][]{ascii("PING")}
        ));
        list.add(new ReqCase("inline_ping",
                ascii("PING\r\n"),
                new byte[][]{ascii("PING")}
        ));
        list.add(new ReqCase("resp3_streamed_blob_echo",
                ascii("*2\r\n+ECHO\r\n$?\r\n;5\r\nhello\r\n;0\r\n"),
                new byte[][]{ascii("ECHO"), ascii("hello")}
        ));
        list.add(new ReqCase("resp3_streamed_command_array",
                ascii("*?\r\n+PING\r\n.\r\n"),
                new byte[][]{ascii("PING")}
        ));
        list.add(new ReqCase("resp3_attributes_chain_then_ping",
                ascii("|0\r\n|1\r\n+meta\r\n$?\r\n;3\r\nfoo\r\n;0\r\n*1\r\n+PING\r\n"),
                new byte[][]{ascii("PING")}));
        list.add(new ReqCase("resp3_scalar_args",
                ascii("*3\r\n+ECHO\r\n:42\r\n#t\r\n"),
                new byte[][]{ascii("ECHO"), ascii("42"), ascii("t")}
        ));
        list.add(new ReqCase("null_bulk_arg_is_decoded_as_null",
                ascii("*2\r\n+ECHO\r\n$-1\r\n"),
                new byte[][]{ascii("ECHO"), null}
        ));
        return list;
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class ReqCase {
        final String name;
        final byte[] payload;
        final byte[][] expectedArgs;

        private ReqCase(String name, byte[] payload, byte[][] expectedArgs) {
            this.name = name;
            this.payload = payload;
            this.expectedArgs = expectedArgs;
        }
    }
}
