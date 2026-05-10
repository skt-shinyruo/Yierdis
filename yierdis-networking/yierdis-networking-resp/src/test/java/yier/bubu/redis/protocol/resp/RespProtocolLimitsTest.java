package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

public class RespProtocolLimitsTest {
    @Test
    public void defaultsArePositiveAndRedisProtocolOriented() {
        Assert.assertTrue(RespProtocolLimits.DEFAULT_MAX_BULK_BYTES > 0);
        Assert.assertTrue(RespProtocolLimits.DEFAULT_MAX_ARGS > 0);
        Assert.assertTrue(RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES > 0);
        Assert.assertEquals(512 * 1024 * 1024, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
        Assert.assertEquals(1024 * 1024, RespProtocolLimits.DEFAULT_MAX_ARGS);
        Assert.assertEquals(1024 * 1024, RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES);
    }
}
