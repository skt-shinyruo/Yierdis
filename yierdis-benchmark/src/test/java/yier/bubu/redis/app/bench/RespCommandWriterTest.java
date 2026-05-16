package yier.bubu.redis.app.bench;

import com.sun.management.ThreadMXBean;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RespCommandWriterTest {
    @Test
    public void writesRespArrayBulkCommand() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        RespClientCodec.writeCommand(out, List.of(utf8("SET"), utf8("a"), utf8("1")));

        Assert.assertEquals("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n",
                out.toString(StandardCharsets.US_ASCII));
    }

    @Test
    public void writePingMatchesSharedRespEncoderBytes() throws Exception {
        Assert.assertArrayEquals(
                RespClientCodec.encodeCommand(List.of(utf8("PING"))),
                writeFrame(YierdisBench.RespCommandWriter::writePing)
        );
    }

    @Test
    public void writeSetMatchesSharedRespEncoderBytesForEscapedUtf8Payload() throws Exception {
        byte[] key = utf8("bench\"key");
        byte[] value = utf8("line1\\line2\n中文");

        Assert.assertArrayEquals(
                RespClientCodec.encodeCommand(List.of(utf8("SET"), key, value)),
                writeFrame(writer -> writer.writeSet(key, value))
        );
    }

    @Test
    public void writeAppendMatchesSharedRespEncoderBytesForEscapedUtf8Payload() throws Exception {
        byte[] key = utf8("bench\"key");
        byte[] value = utf8("line1\\line2\n中文");

        Assert.assertArrayEquals(
                RespClientCodec.encodeCommand(List.of(utf8("APPEND"), key, value)),
                writeFrame(writer -> writer.writeAppend(key, value))
        );
    }

    @Test
    public void repeatedSummaryRenderingDoesNotTouchCommandWriterPath() throws Exception {
        byte[] key = utf8("next-key");
        byte[] value = utf8("line1\\line2\n中文");

        Assert.assertArrayEquals(
                RespClientCodec.encodeCommand(List.of(utf8("SET"), key, value)),
                writeFrame(writer -> writer.writeSet(key, value))
        );
    }

    @Test
    public void writeSetThenGetOnSameWriterDoesNotLeakPreviousArgs() throws Exception {
        byte[] setKey = utf8("bench\"key");
        byte[] getKey = utf8("next-key");
        byte[] value = utf8("line1\\line2\n中文");

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        expected.writeBytes(RespClientCodec.encodeCommand(List.of(utf8("SET"), setKey, value)));
        expected.writeBytes(RespClientCodec.encodeCommand(List.of(utf8("GET"), getKey)));

        Assert.assertArrayEquals(
                expected.toByteArray(),
                writeFrames(writer -> {
                    writer.writeSet(setKey, value);
                    writer.writeGet(getKey);
                })
        );
    }

    @Test
    public void sameWriterRepeatedWritesAvoidPerCallIntermediateAllocations() throws Exception {
        ThreadMXBean threadMxBean = threadMxBeanOrNull();
        Assume.assumeNotNull(threadMxBean);

        if (!threadMxBean.isThreadAllocatedMemoryEnabled()) {
            threadMxBean.setThreadAllocatedMemoryEnabled(true);
        }

        byte[] setKey = utf8("bench\"key");
        byte[] getKey = utf8("next-key");
        byte[] value = utf8("line1\\line2\n中文");

        try (YierdisBench.RespCommandWriter writer = new YierdisBench.RespCommandWriter(OutputStream.nullOutputStream())) {
            for (int i = 0; i < 20_000; i++) {
                writer.writeSet(setKey, value);
                writer.writeGet(getKey);
            }

            long threadId = Thread.currentThread().getId();
            long before = threadMxBean.getThreadAllocatedBytes(threadId);
            for (int i = 0; i < 2_000; i++) {
                writer.writeSet(setKey, value);
                writer.writeGet(getKey);
            }
            long allocated = threadMxBean.getThreadAllocatedBytes(threadId) - before;

            Assert.assertTrue("allocated bytes for writer path: " + allocated, allocated <= 8_192L);
        }
    }

    @Test
    public void strictPingReplyValidationAcceptsSimpleStringPong() throws Exception {
        Assert.assertTrue(validateStrictReply(YierdisBench.Workload.PING, "+PONG\r\n", 0));
    }

    @Test
    public void strictSetReplyValidationAcceptsSimpleStringOk() throws Exception {
        Assert.assertTrue(validateStrictReply(YierdisBench.Workload.SET_RANDOM, "+OK\r\n", 0));
    }

    @Test
    public void strictAppendReplyValidationAcceptsIntegerReply() throws Exception {
        Assert.assertTrue(validateStrictReply(YierdisBench.Workload.APPEND, ":7\r\n", 7));
        Assert.assertFalse(validateStrictReply(YierdisBench.Workload.APPEND, "+OK\r\n", 0));
        Assert.assertFalse(validateStrictReply(YierdisBench.Workload.APPEND, "$7\r\npayload\r\n", 0));
        Assert.assertFalse(validateStrictReply(YierdisBench.Workload.APPEND, ":-1\r\n", 0));
        Assert.assertFalse(validateStrictReply(YierdisBench.Workload.APPEND, ":0\r\n", 7));
    }

    @Test
    public void strictGetReplyValidationAcceptsBulkResultWithExpectedSize() throws Exception {
        String value = "line1\\line2\n中文";

        Assert.assertTrue(validateStrictReply(YierdisBench.Workload.GET_RANDOM,
                "$" + utf8(value).length + "\r\n" + value + "\r\n", utf8(value).length));
    }

    @Test
    public void strictGetReplyValidationAcceptsNullBulkResult() throws Exception {
        Assert.assertTrue(validateStrictReply(YierdisBench.Workload.GET_RANDOM, "$-1\r\n", 2));
    }

    @Test
    public void strictReplyValidationAvoidsHeavyPerReplyAllocations() throws Exception {
        ThreadMXBean threadMxBean = threadMxBeanOrNull();
        Assume.assumeNotNull(threadMxBean);

        if (!threadMxBean.isThreadAllocatedMemoryEnabled()) {
            threadMxBean.setThreadAllocatedMemoryEnabled(true);
        }

        String value = "line1\\line2\n中文";
        int expectedDataSize = utf8(value).length;
        ReusableByteArrayInputStream reply = new ReusableByteArrayInputStream(
                ("$" + expectedDataSize + "\r\n" + value + "\r\n").getBytes(StandardCharsets.UTF_8)
        );

        for (int i = 0; i < 20_000; i++) {
            reply.reset();
            Assert.assertTrue(YierdisBench.validateStrictReply(YierdisBench.Workload.GET_RANDOM, reply, expectedDataSize));
        }

        long threadId = Thread.currentThread().getId();
        long before = threadMxBean.getThreadAllocatedBytes(threadId);
        for (int i = 0; i < 2_000; i++) {
            reply.reset();
            Assert.assertTrue(YierdisBench.validateStrictReply(YierdisBench.Workload.GET_RANDOM, reply, expectedDataSize));
        }
        long allocated = threadMxBean.getThreadAllocatedBytes(threadId) - before;

        Assert.assertTrue("allocated bytes for strict reply validation: " + allocated, allocated <= 512_000L);
    }

    private static byte[] writeFrame(WriterAction action) throws Exception {
        return writeFrames(action);
    }

    private static byte[] writeFrames(WriterAction action) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (YierdisBench.RespCommandWriter writer = new YierdisBench.RespCommandWriter(out)) {
            action.write(writer);
        }
        return out.toByteArray();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static boolean validateStrictReply(YierdisBench.Workload workload, String reply, int expectedDataSize) throws IOException {
        return YierdisBench.validateStrictReply(workload,
                new ByteArrayInputStream(reply.getBytes(StandardCharsets.UTF_8)), expectedDataSize);
    }

    private static ThreadMXBean threadMxBeanOrNull() {
        if (!(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean threadMxBean)) {
            return null;
        }
        if (!threadMxBean.isThreadAllocatedMemorySupported()) {
            return null;
        }
        return threadMxBean;
    }

    @FunctionalInterface
    private interface WriterAction {
        void write(YierdisBench.RespCommandWriter writer) throws IOException;
    }

    private static final class ReusableByteArrayInputStream extends ByteArrayInputStream {
        private ReusableByteArrayInputStream(byte[] bytes) {
            super(bytes);
        }

        @Override
        public synchronized void reset() {
            pos = 0;
        }
    }
}
