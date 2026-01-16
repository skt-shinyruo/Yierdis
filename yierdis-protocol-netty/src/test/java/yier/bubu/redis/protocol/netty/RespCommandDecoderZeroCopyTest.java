package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespFrame;

import java.nio.charset.StandardCharsets;

public class RespCommandDecoderZeroCopyTest {
    @Test
    public void decodesBulkArgsAsSlicesAndReleasesOnRecycle() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());

        byte[] bin = new byte[]{0, (byte) 0xFF, 'x'};
        byte[] req = concat(
                ascii("*2\r\n"),
                bulk(ascii("ECHO")),
                bulk(bin)
        );
        ch.writeInbound(Unpooled.wrappedBuffer(req));

        RespCommand cmd = ch.readInbound();
        Assert.assertNotNull(cmd);
        Assert.assertEquals(2, cmd.argc());
        Assert.assertFalse(cmd.isNull(0));
        Assert.assertFalse(cmd.isNull(1));
        Assert.assertEquals(4, cmd.len(0));
        Assert.assertEquals(3, cmd.len(1));

        Assert.assertEquals('E', cmd.byteAt(0, 0));
        Assert.assertEquals((byte) 0xFF, cmd.byteAt(1, 1));
        Assert.assertArrayEquals(bin, cmd.toByteArray(1));

        RespFrame frame = cmd.frame();
        Assert.assertNotNull(frame);
        Assert.assertTrue(frame instanceof NettyRespFrame);
        Assert.assertTrue("retainedBytes must be a stable lower bound", frame.retainedBytes() >= frame.length());
        ByteBuf buf = ((NettyRespFrame) frame).unwrap();
        Assert.assertNotNull(buf);
        Assert.assertEquals(1, buf.refCnt());

        cmd.recycle();
        Assert.assertEquals(0, buf.refCnt());

        ch.finishAndReleaseAll();
    }

    @Test
    public void nettyRespFrameRetainedBytesCanExceedLogicalLengthForDerivedBuffers() {
        ByteBuf root = Unpooled.buffer(1024);
        try {
            root.writeZero(100);
            ByteBuf slice = root.retainedSlice(0, 100);
            NettyRespFrame frame = new NettyRespFrame(slice);
            try {
                Assert.assertEquals(100, frame.length());
                Assert.assertTrue(frame.retainedBytes() >= frame.length());
                Assert.assertTrue(frame.retainedBytes() <= root.capacity());
                Assert.assertTrue(frame.retainedBytes() > frame.length());
            } finally {
                frame.close();
            }
        } finally {
            root.release();
        }
    }

    @Test
    public void decoderDistinguishesNullBulkFromEmptyBulk() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());

        byte[] req = concat(
                ascii("*3\r\n"),
                bulk(ascii("ECHO")),
                ascii("$-1\r\n"),
                bulk(new byte[0])
        );
        ch.writeInbound(Unpooled.wrappedBuffer(req));

        RespCommand cmd = ch.readInbound();
        Assert.assertNotNull(cmd);
        Assert.assertEquals(3, cmd.argc());

        Assert.assertTrue(cmd.isNull(1));
        Assert.assertEquals(-1, cmd.len(1));
        Assert.assertNull(cmd.toByteArray(1));

        Assert.assertFalse(cmd.isNull(2));
        Assert.assertEquals(0, cmd.len(2));
        Assert.assertArrayEquals(new byte[0], cmd.toByteArray(2));

        NettyRespFrame frame = (NettyRespFrame) cmd.frame();
        ByteBuf buf = frame.unwrap();
        Assert.assertTrue("retainedBytes must be a stable lower bound", frame.retainedBytes() >= frame.length());
        cmd.recycle();
        Assert.assertEquals(0, buf.refCnt());

        ch.finishAndReleaseAll();
    }

    @Test
    public void partialFramesDoNotProduceCommandAndReleasesAfterRecycle() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());

        byte[] full = ascii("*1\r\n$4\r\nPING\r\n");
        ByteBuf part1 = Unpooled.wrappedBuffer(full, 0, 7); // "*1\r\n$4"
        ByteBuf part2 = Unpooled.wrappedBuffer(full, 7, full.length - 7);

        ch.writeInbound(part1);
        Assert.assertNull(ch.readInbound());

        ch.writeInbound(part2);
        RespCommand cmd = ch.readInbound();
        Assert.assertNotNull(cmd);
        Assert.assertEquals(1, cmd.argc());
        Assert.assertFalse(cmd.isNull(0));
        Assert.assertEquals(4, cmd.len(0));

        NettyRespFrame frame = (NettyRespFrame) cmd.frame();
        ByteBuf buf = frame.unwrap();
        Assert.assertEquals(1, buf.refCnt());
        cmd.recycle();
        Assert.assertEquals(0, buf.refCnt());

        ch.finishAndReleaseAll();
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
