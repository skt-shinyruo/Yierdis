package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CommandSession;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;

public class RespReplyWriterSessionTest {
    @Test
    public void usesSessionRespVersionWhenAvailable() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriter writer = new RespReplyWriter(session(3), sink);
        writer.booleanValue(true);

        Assert.assertEquals("#t\r\n", sink.utf8());
    }

    @Test
    public void sessionBackedWriterObservesProtocolChangesBeforeReplyIsWritten() {
        ByteArraySink sink = new ByteArraySink();
        CommandSession session = session(2);

        RespReplyWriter writer = new RespReplyWriter(session, sink);
        session.setRespVersion(3);
        writer.mapHeader(1);
        writer.bulkString("proto".getBytes(StandardCharsets.US_ASCII));
        writer.integer(3);

        Assert.assertEquals("%1\r\n$5\r\nproto\r\n:3\r\n", sink.utf8());
    }

    @Test
    public void usesResp2FromCompleteSession() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriter writer = new RespReplyWriter(session(2), sink);
        writer.booleanValue(true);

        Assert.assertEquals(":1\r\n", sink.utf8());
    }

    private static CommandSession session(int version) {
        int[] activeVersion = {version};
        return (CommandSession) Proxy.newProxyInstance(
                CommandSession.class.getClassLoader(),
                new Class<?>[]{CommandSession.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "respVersion" -> activeVersion[0];
                    case "setRespVersion" -> {
                        activeVersion[0] = (int) args[0];
                        yield null;
                    }
                    default -> throw new UnsupportedOperationException(method.toString());
                });
    }

    private static final class ByteArraySink implements BytesSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void writeBytes(byte[] src, int off, int len) {
            out.write(src, off, len);
        }

        String utf8() {
            return out.toString(StandardCharsets.UTF_8);
        }
    }
}
