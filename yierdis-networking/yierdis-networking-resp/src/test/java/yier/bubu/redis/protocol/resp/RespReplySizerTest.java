package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ReplyShape;

import java.lang.reflect.Proxy;

public class RespReplySizerTest {
    @Test
    public void semanticLengthCallbacksRejectInvalidValuesAndCardinality() {
        CommandSession session = session(2);
        RespReplySizer sizer = new RespReplySizer();

        Assert.assertThrows(IllegalArgumentException.class, () -> sizer.plan(
                session,
                new ReplyShape.ByteSequence(1, consumer -> consumer.accept(-2), 0)
        ));
        Assert.assertThrows(IllegalArgumentException.class, () -> sizer.plan(
                session,
                new ReplyShape.ByteMap(1, consumer -> consumer.accept(1), 0)
        ));
    }

    private static CommandSession session(int version) {
        return (CommandSession) Proxy.newProxyInstance(
                CommandSession.class.getClassLoader(),
                new Class<?>[]{CommandSession.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("respVersion")) {
                        return version;
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }
}
