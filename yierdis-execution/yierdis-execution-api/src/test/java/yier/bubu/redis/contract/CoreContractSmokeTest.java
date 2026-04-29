package yier.bubu.redis.contract;

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

        CommandContext ctx = new CommandContext(null, writer);
        Assert.assertSame(writer, ctx.out());
        writer.requestCloseAfterReply();
        Assert.assertTrue(writer.closeAfterReplyRequested());
    }
}

