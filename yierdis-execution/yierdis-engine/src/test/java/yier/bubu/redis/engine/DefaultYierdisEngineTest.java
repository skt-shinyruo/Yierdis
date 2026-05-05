package yier.bubu.redis.engine;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.CommandDescriptor;
import yier.bubu.redis.command.CommandParsers;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultYierdisEngineTest {
    @Test
    public void executeDelegatesThroughOwnedCommandProcessor() {
        YierdisEngine engine = new DefaultYierdisEngine(
                () -> {
                },
                registration -> registration.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("LOCAL_OK")
                )
        );

        CapturingReplyWriter out = new CapturingReplyWriter();
        engine.execute(
                new EngineSession(16, 1024),
                ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()),
                out
        );

        Assert.assertEquals("LOCAL_OK", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    @Test
    public void executeRejectsNonServerSessionBeforeCommandModulesRun() {
        YierdisEngine engine = new DefaultYierdisEngine(
                () -> {
                },
                registration -> registration.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("LOCAL_OK")
                )
        );

        CapturingReplyWriter out = new CapturingReplyWriter();
        try {
            engine.execute(
                    null,
                    ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()),
                    out
            );
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("YierdisEngine requires ServerSession", e.getMessage());
        }

        Assert.assertNull(out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    @Test
    public void maintenanceTickDelegatesToOwnerThreadRuntimeHook() {
        AtomicInteger ticks = new AtomicInteger();
        YierdisEngine engine = new DefaultYierdisEngine(
                ticks::incrementAndGet
        );

        engine.maintenanceTick();
        engine.maintenanceTick();

        Assert.assertEquals(2, ticks.get());
    }

    private static final class CapturingReplyWriter implements ReplyWriter {
        private String simpleStringValue;
        private String errorValue;

        @Override
        public void requestCloseAfterReply() {
        }

        @Override
        public boolean closeAfterReplyRequested() {
            return false;
        }

        @Override
        public void simpleString(String value) {
            this.simpleStringValue = value;
        }

        @Override
        public void error(String message) {
            this.errorValue = message;
        }

        @Override
        public void integer(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void booleanValue(boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void doubleValue(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bigNumberAscii(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void verbatimString(String format, byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void blobError(String message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nullValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nullArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void arrayHeader(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkStringArray(List<byte[]> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emptyArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mapHeader(int pairs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setHeader(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pushHeader(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void attributeHeader(int pairs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkString(byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkString(BytesSlice slice) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkStringLongAscii(long value) {
            throw new UnsupportedOperationException();
        }
    }
}
