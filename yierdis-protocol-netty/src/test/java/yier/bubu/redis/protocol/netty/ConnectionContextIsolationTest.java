package yier.bubu.redis.protocol.netty;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespProtocol;

public class ConnectionContextIsolationTest {
    @Test
    public void protocolStateIsPerChannel() {
        EmbeddedChannel ch1 = new EmbeddedChannel();
        EmbeddedChannel ch2 = new EmbeddedChannel();
        try {
            ConnectionContext c1 = ConnectionContext.getOrCreate(ch1);
            ConnectionContext c2 = ConnectionContext.getOrCreate(ch2);

            Assert.assertEquals(RespProtocol.RESP2, c1.protocol());
            Assert.assertEquals(RespProtocol.RESP2, c2.protocol());

            c1.setProtocol(RespProtocol.RESP3);
            Assert.assertEquals(RespProtocol.RESP3, c1.protocol());
            Assert.assertEquals(RespProtocol.RESP2, c2.protocol());

            c2.setProtocol(RespProtocol.RESP3);
            Assert.assertEquals(RespProtocol.RESP3, c1.protocol());
            Assert.assertEquals(RespProtocol.RESP3, c2.protocol());
        } finally {
            ch1.close();
            ch2.close();
        }
    }

    @Test
    public void nullProtocolFallsBackToResp2() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ConnectionContext c = ConnectionContext.getOrCreate(ch);
            c.setProtocol(null);
            Assert.assertEquals(RespProtocol.RESP2, c.protocol());
        } finally {
            ch.close();
        }
    }
}

