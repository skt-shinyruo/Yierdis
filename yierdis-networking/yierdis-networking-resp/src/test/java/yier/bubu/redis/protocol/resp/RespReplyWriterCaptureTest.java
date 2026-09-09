package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class RespReplyWriterCaptureTest {
    // writer 的协议版本在构造时固定，渲染期间不再回读 session；
    // 与 prepare 时刻预留容量使用的是同一份捕获值，由 executor 从 ReplyPlan 传入。
    @Test
    public void writerRendersAtTheVersionFixedAtConstruction() {
        ByteArraySink sink2 = new ByteArraySink();
        RespReplyWriter writer2 = new RespReplyWriter(2, sink2);
        writer2.mapHeader(1);
        writer2.nullValue();
        writer2.setHeader(2);

        Assert.assertEquals("*2\r\n$-1\r\n*2\r\n", sink2.utf8());

        ByteArraySink sink3 = new ByteArraySink();
        RespReplyWriter writer3 = new RespReplyWriter(3, sink3);
        writer3.mapHeader(1);
        writer3.nullValue();
        writer3.setHeader(2);

        Assert.assertEquals("%1\r\n_\r\n~2\r\n", sink3.utf8());
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
