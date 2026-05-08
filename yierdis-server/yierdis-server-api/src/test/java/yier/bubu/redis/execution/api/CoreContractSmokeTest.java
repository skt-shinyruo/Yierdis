package yier.bubu.redis.execution.api;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CoreContractSmokeTest {
    @Test
    public void contractTypesCompose() {
        ReplyWriter writer = new ReplyWriter() {
            private boolean closeAfter;

            @Override
            public void requestCloseAfterReply() {
                closeAfter = true;
            }

            @Override
            public boolean closeAfterReplyRequested() {
                return closeAfter;
            }

            @Override
            public void simpleString(String value) {
            }

            @Override
            public void error(String message) {
            }

            @Override
            public void integer(long value) {
            }

            @Override
            public void booleanValue(boolean value) {
            }

            @Override
            public void doubleValue(double value) {
            }

            @Override
            public void bigNumberAscii(String value) {
            }

            @Override
            public void verbatimString(String format, byte[] data) {
            }

            @Override
            public void blobError(String message) {
            }

            @Override
            public void nullValue() {
            }

            @Override
            public void nullArray() {
            }

            @Override
            public void arrayHeader(int count) {
            }

            @Override
            public void bulkStringArray(List<byte[]> values) {
            }

            @Override
            public void emptyArray() {
            }

            @Override
            public void mapHeader(int pairs) {
            }

            @Override
            public void setHeader(int count) {
            }

            @Override
            public void pushHeader(int count) {
            }

            @Override
            public void attributeHeader(int pairs) {
            }

            @Override
            public void bulkString(byte[] data) {
            }

            @Override
            public void bulkString(byte[] data, int off, int len) {
            }

            @Override
            public void bulkString(yier.bubu.redis.bytes.BytesSlice slice) {
            }

            @Override
            public void bulkStringLongAscii(long value) {
            }
        };

        ServerSession session = new ServerSession() {
            private final TransactionState tx = new TransactionState() {
                @Override
                public boolean active() {
                    return false;
                }

                @Override
                public void begin() {
                }

                @Override
                public void discard() {
                }

                @Override
                public void enqueue(ExecutionRequest request) {
                }

                @Override
                public int size() {
                    return 0;
                }

                @Override
                public List<ExecutionRequest> drain() {
                    return List.of();
                }
            };

            @Override
            public int dbIndex() {
                return 0;
            }

            @Override
            public void setDbIndex(int dbIndex) {
            }

            @Override
            public long clientId() {
                return 1L;
            }

            @Override
            public String clientName() {
                return null;
            }

            @Override
            public void setClientName(String clientName) {
            }

            @Override
            public boolean authenticated() {
                return false;
            }

            @Override
            public void setAuthenticated(boolean authenticated) {
            }

            @Override
            public TransactionState transaction() {
                return tx;
            }

            @Override
            public ConnectionStatsView connectionStats() {
                return null;
            }
        };

        CommandContext ctx = new CommandContext(session, writer);
        Assert.assertSame(session, ctx.session());
        Assert.assertSame(writer, ctx.out());
        ctx.recordMutation(true, false);
        Assert.assertTrue(ctx.valueChanged());
        Assert.assertFalse(ctx.ttlChanged());
        Assert.assertTrue(ctx.changedAny());
        ctx.clearMutationOutcome();
        Assert.assertFalse(ctx.changedAny());
        writer.requestCloseAfterReply();
        Assert.assertTrue(writer.closeAfterReplyRequested());
    }
}
