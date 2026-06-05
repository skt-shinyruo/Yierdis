package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ProtocolNegotiationSession;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class RespReplyWriterFactoryTest {
    @Test
    public void usesSessionRespVersionWhenAvailable() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriterFactory factory = new RespReplyWriterFactory();

        RedisReplyWriter writer = factory.newWriter(serverSession(3), sink);
        writer.booleanValue(true);

        Assert.assertEquals("#t\r\n", sink.utf8());
    }

    @Test
    public void sessionBackedWriterObservesProtocolChangesBeforeReplyIsWritten() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriterFactory factory = new RespReplyWriterFactory();
        MutableSession session = new MutableSession(2);

        RedisReplyWriter writer = factory.newWriter(session, sink);
        session.setRespVersion(3);
        writer.mapHeader(1);
        writer.bulkString("proto".getBytes(StandardCharsets.US_ASCII));
        writer.integer(3);

        Assert.assertEquals("%1\r\n$5\r\nproto\r\n:3\r\n", sink.utf8());
    }

    @Test
    public void fallsBackToResp2WhenSessionIsMissing() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriterFactory factory = new RespReplyWriterFactory();

        RedisReplyWriter writer = factory.newWriter(null, sink);
        writer.booleanValue(true);

        Assert.assertEquals(":1\r\n", sink.utf8());
    }

    private static ProtocolNegotiationSession serverSession(int respVersion) {
        return new MutableSession(respVersion);
    }

    private static final class MutableSession implements ProtocolNegotiationSession {
        private int respVersion;

        private MutableSession(int respVersion) {
            this.respVersion = respVersion;
        }

            @Override
            public int respVersion() {
                return respVersion;
            }

            @Override
            public void setRespVersion(int respVersion) {
                this.respVersion = respVersion;
            }
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
