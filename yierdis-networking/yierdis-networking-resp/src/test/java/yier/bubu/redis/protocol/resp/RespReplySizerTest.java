package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ReplyShape;

public class RespReplySizerTest {
    @Test
    public void semanticLengthCallbacksRejectInvalidValuesAndCardinality() {
        RespReplySizer sizer = new RespReplySizer();

        Assert.assertThrows(IllegalArgumentException.class, () -> sizer.apply(
                2,
                new ReplyShape.ByteSequence(1, consumer -> consumer.accept(-2), 0)
        ));
        Assert.assertThrows(IllegalArgumentException.class, () -> sizer.apply(
                2,
                new ReplyShape.ByteMap(1, consumer -> consumer.accept(1), 0)
        ));
    }

    @Test
    public void unsupportedProtocolVersionIsRejectedBeforeSizing() {
        RespReplySizer sizer = new RespReplySizer();

        Assert.assertThrows(IllegalArgumentException.class, () -> sizer.apply(
                4,
                new ReplyShape.NullValue()
        ));
    }
}
