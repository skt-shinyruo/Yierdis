package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

public class RespProtocolVersionTest {
    @Test
    public void mapsSupportedWireValues() {
        Assert.assertEquals(2, RespProtocolVersion.RESP2.wireValue());
        Assert.assertEquals(3, RespProtocolVersion.RESP3.wireValue());
        Assert.assertSame(RespProtocolVersion.RESP2, RespProtocolVersion.fromWireValue(2));
        Assert.assertSame(RespProtocolVersion.RESP3, RespProtocolVersion.fromWireValue(3));
    }

    @Test
    public void rejectsUnsupportedWireValuesWithRedisProtocolErrorPrefix() {
        IllegalArgumentException error = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> RespProtocolVersion.fromWireValue(4)
        );

        Assert.assertEquals("NOPROTO unsupported protocol version", error.getMessage());
    }
}
