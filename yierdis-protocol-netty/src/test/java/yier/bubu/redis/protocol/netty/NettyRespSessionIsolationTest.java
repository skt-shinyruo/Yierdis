package yier.bubu.redis.protocol.netty;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespProtocol;

public class NettyRespSessionIsolationTest {
    @Test
    public void protocolStateIsPerChannel() {
        EmbeddedChannel ch1 = new EmbeddedChannel();
        EmbeddedChannel ch2 = new EmbeddedChannel();
        try {
            NettyRespSession s1 = new NettyRespSession(ch1);
            NettyRespSession s2 = new NettyRespSession(ch2);

            Assert.assertEquals(RespProtocol.RESP2, s1.protocol());
            Assert.assertEquals(RespProtocol.RESP2, s2.protocol());

            s1.setProtocol(RespProtocol.RESP3);
            Assert.assertEquals(RespProtocol.RESP3, s1.protocol());
            Assert.assertEquals(RespProtocol.RESP2, s2.protocol());

            s2.setProtocol(RespProtocol.RESP3);
            Assert.assertEquals(RespProtocol.RESP3, s1.protocol());
            Assert.assertEquals(RespProtocol.RESP3, s2.protocol());
        } finally {
            ch1.close();
            ch2.close();
        }
    }

    @Test
    public void nullProtocolFallsBackToResp2() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            NettyRespSession s = new NettyRespSession(ch);
            s.setProtocol(null);
            Assert.assertEquals(RespProtocol.RESP2, s.protocol());
        } finally {
            ch.close();
        }
    }
}
