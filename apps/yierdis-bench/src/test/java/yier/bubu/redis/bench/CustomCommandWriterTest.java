package yier.bubu.redis.bench;

import com.sun.management.ThreadMXBean;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import yier.bubu.redis.protocol.v1.CustomProtocolV1RequestEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CustomCommandWriterTest {
    @Test
    public void writePingMatchesSharedEncoderBytes() throws Exception {
        Assert.assertArrayEquals(
                CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8("PING"))),
                writeFrame(YierdisBench.CustomCommandWriter::writePing)
        );
    }

    @Test
    public void writeSetMatchesSharedEncoderBytesForEscapedUtf8Payload() throws Exception {
        byte[] key = utf8("bench\"key");
        byte[] value = utf8("line1\\line2\n中文");

        Assert.assertArrayEquals(
                CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8("SET"), key, value)),
                writeFrame(writer -> writer.writeSet(key, value))
        );
    }

    @Test
    public void repeatedSummaryRenderingDoesNotTouchCommandWriterPath() throws Exception {
        byte[] key = utf8("next-key");
        byte[] value = utf8("line1\\line2\n中文");

        Assert.assertArrayEquals(
                CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8("SET"), key, value)),
                writeFrame(writer -> writer.writeSet(key, value))
        );
    }

    @Test
    public void writeSetThenGetOnSameWriterDoesNotLeakPreviousArgs() throws Exception {
        byte[] setKey = utf8("bench\"key");
        byte[] getKey = utf8("next-key");
        byte[] value = utf8("line1\\line2\n中文");

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        expected.writeBytes(CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8("SET"), setKey, value)));
        expected.writeBytes(CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8("GET"), getKey)));

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

        try (YierdisBench.CustomCommandWriter writer = new YierdisBench.CustomCommandWriter(OutputStream.nullOutputStream())) {
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
    public void strictGetReplyValidationAcceptsEscapedUtf8ResultEnvelope() throws Exception {
        String value = "line1\\line2\n中文";
        byte[] line = utf8("{\"ok\":true,\"result\":\"line1\\\\line2\\n中文\"}");

        Assert.assertTrue(validateStrictReply(YierdisBench.Workload.GET_RANDOM, line, utf8(value).length));
    }

    @Test
    public void strictGetReplyValidationAcceptsTaggedB64ResultEnvelope() throws Exception {
        byte[] line = utf8("{\"ok\":true,\"result\":{\"$b64\":\"wyg=\"}}");

        Assert.assertTrue(validateStrictReply(YierdisBench.Workload.GET_RANDOM, line, 2));
    }

    @Test
    public void strictReplyValidationAvoidsHeavyPerReplyAllocations() throws Exception {
        ThreadMXBean threadMxBean = threadMxBeanOrNull();
        Assume.assumeNotNull(threadMxBean);

        if (!threadMxBean.isThreadAllocatedMemoryEnabled()) {
            threadMxBean.setThreadAllocatedMemoryEnabled(true);
        }

        String value = "line1\\line2\n中文";
        byte[] line = utf8("{\"ok\":true,\"result\":\"line1\\\\line2\\n中文\"}");

        for (int i = 0; i < 20_000; i++) {
            Assert.assertTrue(validateStrictReply(YierdisBench.Workload.GET_RANDOM, line, utf8(value).length));
        }

        long threadId = Thread.currentThread().getId();
        long before = threadMxBean.getThreadAllocatedBytes(threadId);
        for (int i = 0; i < 2_000; i++) {
            Assert.assertTrue(validateStrictReply(YierdisBench.Workload.GET_RANDOM, line, utf8(value).length));
        }
        long allocated = threadMxBean.getThreadAllocatedBytes(threadId) - before;

        Assert.assertTrue("allocated bytes for strict reply validation: " + allocated, allocated <= 512_000L);
    }

    private static byte[] writeFrame(WriterAction action) throws Exception {
        return writeFrames(action);
    }

    private static byte[] writeFrames(WriterAction action) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (YierdisBench.CustomCommandWriter writer = new YierdisBench.CustomCommandWriter(out)) {
            action.write(writer);
        }
        return out.toByteArray();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static boolean validateStrictReply(YierdisBench.Workload workload, byte[] line, int expectedDataSize) {
        return YierdisBench.validateStrictReply(workload, line, line.length, expectedDataSize);
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
        void write(YierdisBench.CustomCommandWriter writer) throws IOException;
    }
}
