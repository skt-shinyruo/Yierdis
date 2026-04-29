package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;

import java.util.List;

public class YierdisFastCommandProcessorModuleTest {
    @Test
    public void kernelProcessorDoesNotRegisterDefaultDataCommands() {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor();

        assertUnknownCommand(processor, "PING");
        assertUnknownCommand(processor, "GET");
        assertUnknownCommand(processor, "SET");
    }

    @Test
    public void modulesCanRegisterAdditionalCommands() {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                registrar -> registrar.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("LOCAL")
                )
        );
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of());

        CapturingReplyWriter out = new CapturingReplyWriter();
        processor.execute(request, new CommandContext(null, out));

        Assert.assertEquals("LOCAL", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    @Test
    public void modulesCanRegisterTypedCommandSpecsDirectly() {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                registrar -> registrar.register(
                        "LOCAL",
                        CommandSpec.of(
                                CommandDescriptor.of(1, 0, 0, 0),
                                CommandParsers.exact(1, "local"),
                                (args, ctx) -> ctx.out().simpleString("LOCAL_OK")
                        )
                )
        );

        CapturingReplyWriter out = new CapturingReplyWriter();
        processor.execute(ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()), new CommandContext(null, out));

        Assert.assertEquals("LOCAL_OK", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    private static void assertUnknownCommand(YierdisFastCommandProcessor processor, String commandName) {
        CapturingReplyWriter out = new CapturingReplyWriter();
        processor.execute(ByteArrayExecutionRequest.fromUtf8(commandName, List.of()), new CommandContext(null, out));

        Assert.assertNull(out.simpleStringValue);
        Assert.assertEquals("ERR unknown command '" + commandName + "'", out.errorValue);
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
            throw unsupported();
        }

        @Override
        public void booleanValue(boolean value) {
            throw unsupported();
        }

        @Override
        public void doubleValue(double value) {
            throw unsupported();
        }

        @Override
        public void bigNumberAscii(String value) {
            throw unsupported();
        }

        @Override
        public void verbatimString(String format, byte[] data) {
            throw unsupported();
        }

        @Override
        public void blobError(String message) {
            throw unsupported();
        }

        @Override
        public void nullValue() {
            throw unsupported();
        }

        @Override
        public void nullArray() {
            throw unsupported();
        }

        @Override
        public void arrayHeader(int count) {
            throw unsupported();
        }

        @Override
        public void bulkStringArray(List<byte[]> values) {
            throw unsupported();
        }

        @Override
        public void emptyArray() {
            throw unsupported();
        }

        @Override
        public void mapHeader(int pairs) {
            throw unsupported();
        }

        @Override
        public void setHeader(int count) {
            throw unsupported();
        }

        @Override
        public void pushHeader(int count) {
            throw unsupported();
        }

        @Override
        public void attributeHeader(int pairs) {
            throw unsupported();
        }

        @Override
        public void bulkString(byte[] data) {
            throw unsupported();
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            throw unsupported();
        }

        @Override
        public void bulkString(BytesSlice slice) {
            throw unsupported();
        }

        @Override
        public void bulkStringLongAscii(long value) {
            throw unsupported();
        }

        private UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("reply shape not used by this test");
        }
    }
}
