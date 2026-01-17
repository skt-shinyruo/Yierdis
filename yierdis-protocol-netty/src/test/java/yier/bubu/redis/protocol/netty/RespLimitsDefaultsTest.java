package yier.bubu.redis.protocol.netty;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespLimits;

import java.lang.reflect.Field;

public class RespLimitsDefaultsTest {
    @Test
    public void commandDecoderDefaultsMatchRespLimitsSsot() throws Exception {
        RespCommandDecoder dec = new RespCommandDecoder();
        Assert.assertEquals(RespLimits.DEFAULT_MAX_BULK_BYTES, readInt(dec, "maxBulkBytes"));
        Assert.assertEquals(RespLimits.DEFAULT_MAX_ARGS, readInt(dec, "maxArgs"));
        Assert.assertEquals(RespLimits.DEFAULT_MAX_LINE_BYTES, readInt(dec, "maxLineBytes"));
    }

    @Test
    public void replyDecoderDefaultsMatchRespLimitsSsot() throws Exception {
        RespDecoder dec = new RespDecoder();
        Assert.assertEquals(RespLimits.DEFAULT_MAX_BULK_BYTES, readInt(dec, "maxBulkBytes"));
        Assert.assertEquals(RespLimits.DEFAULT_MAX_ARRAY_LEN, readInt(dec, "maxArrayLen"));
        Assert.assertEquals(RespLimits.DEFAULT_MAX_NESTING_DEPTH, readInt(dec, "maxNestingDepth"));
        Assert.assertEquals(RespLimits.DEFAULT_MAX_LINE_BYTES, readInt(dec, "maxLineBytes"));
    }

    private static int readInt(Object obj, String fieldName) throws Exception {
        Field f = obj.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return (int) f.get(obj);
    }
}

