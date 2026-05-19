package yier.bubu.redis.execution.engine;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.kernel.YierdisCommandProcessorOptions;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.storage.api.DbChange;
import yier.bubu.redis.storage.api.DbChangeContext;
import yier.bubu.redis.storage.api.DbChangeKind;
import yier.bubu.redis.runtime.api.YierdisChangeEvent;
import yier.bubu.redis.runtime.api.YierdisChangeKind;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
    public void configuredChangeSinkReceivesSyntheticDbChangesFromCommandPath() {
        ArrayList<YierdisChangeEvent> events = new ArrayList<>();
        YierdisEngine engine = new DefaultYierdisEngine(
                YierdisCommandProcessorOptions.builder()
                        .changeSink(events::add)
                        .build(),
                () -> {
                },
                registration -> registration.register(
                        "SYNTHETIC_READ_DELETE",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "synthetic_read_delete"),
                        (request, ctx) -> {
                            DbChangeContext.emit(DbChange.syntheticDelete(
                                    ctx.session().dbIndex(),
                                    DbChangeKind.EXPIRED,
                                    "stale".getBytes(StandardCharsets.US_ASCII)
                            ));
                            ctx.out().simpleString("OK");
                        }
                )
        );

        EngineSession session = new EngineSession(16, 1024);
        session.setDbIndex(3);
        CapturingReplyWriter out = new CapturingReplyWriter();

        engine.execute(
                session,
                ByteArrayExecutionRequest.fromUtf8("SYNTHETIC_READ_DELETE", List.of()),
                out
        );

        Assert.assertEquals("OK", out.simpleStringValue);
        Assert.assertEquals(1, events.size());
        YierdisChangeEvent event = events.get(0);
        Assert.assertEquals(3, event.dbIndex());
        Assert.assertEquals(YierdisChangeKind.EXPIRED, event.kind());
        Assert.assertTrue(event.synthetic());
        Assert.assertEquals("DEL", arg(event, 0));
        Assert.assertEquals("stale", arg(event, 1));
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

    private static String arg(YierdisChangeEvent event, int index) {
        return new String(event.request().toByteArray(index), StandardCharsets.US_ASCII);
    }
}
