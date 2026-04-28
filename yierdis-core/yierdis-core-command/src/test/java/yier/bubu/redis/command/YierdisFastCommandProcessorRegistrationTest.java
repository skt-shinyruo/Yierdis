package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.DbIndexProvider;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.runtime.api.YierdisChangeSink;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class YierdisFastCommandProcessorRegistrationTest {
    private static final YierdisDbRouter TEST_ROUTER = new YierdisDbRouter() {
        @Override
        public DbEngine dbFor(DbIndexProvider dbIndexProvider) {
            return null;
        }

        @Override
        public int databases() {
            return 1;
        }
    };

    @Test
    public void constructorRegistersDefaultAndCallerSuppliedModules() {
        CommandModule extraModule = registration -> registration.register(
                "TRACE",
                CommandDescriptor.of(1, 0, 0, 0),
                CommandParsers.exactRequest(1, "trace"),
                (request, ctx) -> ctx.out().simpleString("TRACE-OK")
        );

        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                TEST_ROUTER,
                null,
                YierdisChangeSink.NOOP,
                null,
                extraModule
        );

        Assert.assertEquals("PONG", executeSimpleString(processor, "PING"));
        Assert.assertEquals("TRACE-OK", executeSimpleString(processor, "TRACE"));
    }

    @Test
    public void commandModuleRegistrationDoesNotExposeLegacyHandlerOverloads() {
        for (Class<?> nested : CommandModule.class.getDeclaredClasses()) {
            Assert.assertNotEquals("Handler", nested.getSimpleName());
        }
        for (Method method : CommandModule.Registration.class.getMethods()) {
            if (!"register".equals(method.getName()) && !"registerDisallowedInMulti".equals(method.getName())) {
                continue;
            }
            for (Class<?> parameterType : method.getParameterTypes()) {
                Assert.assertFalse(parameterType.getName().endsWith("CommandModule$Handler"));
            }
        }
    }

    @Test
    public void registryLookupAndHelpersMustAcceptExecutionRequest() throws Exception {
        Method spec = CommandRegistry.class.getDeclaredMethod("spec", ExecutionRequest.class);
        Assert.assertEquals(CommandSpec.class, spec.getReturnType());

        Method descriptor = CommandRegistry.class.getDeclaredMethod("descriptor", ExecutionRequest.class);
        Assert.assertEquals(CommandDescriptor.class, descriptor.getReturnType());

        Method disallowed = CommandRegistry.class.getDeclaredMethod("disallowedInMultiError", ExecutionRequest.class);
        Assert.assertEquals(String.class, disallowed.getReturnType());

        Method utf8 = CommandSupport.class.getDeclaredMethod("utf8", ExecutionRequest.class, int.class);
        Assert.assertEquals(String.class, utf8.getReturnType());

        Method asciiEqualsIgnoreCase = CommandSupport.class.getDeclaredMethod(
                "asciiEqualsIgnoreCase",
                ExecutionRequest.class,
                int.class,
                String.class
        );
        Assert.assertEquals(boolean.class, asciiEqualsIgnoreCase.getReturnType());

        Method parseLong = CommandSupport.class.getDeclaredMethod("parseLong", ExecutionRequest.class, int.class, String.class);
        Assert.assertEquals(long.class, parseLong.getReturnType());
    }

    @Test
    public void typedCommandSpecCanBeRegisteredAndExecuted() {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                TEST_ROUTER,
                null,
                YierdisChangeSink.NOOP,
                null,
                registration -> registration.register(
                        "TYPED",
                        CommandSpec.of(
                                CommandDescriptor.of(2, 0, 0, 0),
                                CommandParsers.exact(2, "typed"),
                                (args, ctx) -> ctx.out().bulkString(args.bytes(1))
                        )
                )
        );

        Assert.assertEquals("value", executeBulkString(processor, "TYPED", "value"));
    }

    @Test
    public void processorReturnsParseErrorBeforeTypedHandlerRuns() {
        final boolean[] handlerCalled = {false};
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                TEST_ROUTER,
                null,
                YierdisChangeSink.NOOP,
                null,
                registration -> registration.register(
                        "STRICT",
                        CommandSpec.of(
                                CommandDescriptor.of(2, 0, 0, 0),
                                CommandParsers.exact(2, "strict"),
                                (args, ctx) -> {
                                    handlerCalled[0] = true;
                                    ctx.out().simpleString("OK");
                                }
                        )
                )
        );

        TestReplyWriter out = new TestReplyWriter();
        processor.execute(ByteArrayExecutionRequest.fromUtf8("STRICT", List.of()), new CommandContext(null, out));

        Assert.assertFalse(handlerCalled[0]);
        Assert.assertEquals("ERR wrong number of arguments for 'strict' command", out.error());
    }

    private static String executeSimpleString(YierdisFastCommandProcessor processor, String... argv) {
        TestReplyWriter writer = new TestReplyWriter();
        processor.execute(new ArrayExecutionRequest(argv), new CommandContext(null, writer));
        if (writer.error() != null) {
            Assert.fail("expected simple string reply, got error: " + writer.error());
        }
        Assert.assertNotNull("expected simple string reply", writer.simpleString());
        return writer.simpleString();
    }

    private static String executeBulkString(YierdisFastCommandProcessor processor, String command, String arg) {
        TestReplyWriter out = new TestReplyWriter();
        processor.execute(ByteArrayExecutionRequest.fromUtf8(command, List.of(arg)), new CommandContext(null, out));
        Assert.assertNull(out.error());
        return out.bulkString();
    }

    private static final class ArrayExecutionRequest implements ExecutionRequest {
        private final byte[][] argv;

        private ArrayExecutionRequest(String... argv) {
            this.argv = new byte[argv.length][];
            for (int i = 0; i < argv.length; i++) {
                this.argv[i] = argv[i] == null ? null : argv[i].getBytes(StandardCharsets.US_ASCII);
            }
        }

        @Override
        public int argc() {
            return argv.length;
        }

        @Override
        public boolean isNull(int index) {
            return argv[index] == null;
        }

        @Override
        public int len(int index) {
            byte[] arg = argv[index];
            return arg == null ? -1 : arg.length;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return argv[index][offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            byte[] arg = argv[index];
            System.arraycopy(arg, 0, dst, dstOff, arg.length);
        }

        @Override
        public byte[] toByteArray(int index) {
            return argv[index];
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static final class TestReplyWriter implements ReplyWriter {
        private String simpleString;
        private String bulkString;
        private String error;
        private boolean closeAfterReplyRequested;

        private String simpleString() {
            return simpleString;
        }

        private String error() {
            return error;
        }

        private String bulkString() {
            return bulkString;
        }

        @Override
        public void requestCloseAfterReply() {
            closeAfterReplyRequested = true;
        }

        @Override
        public boolean closeAfterReplyRequested() {
            return closeAfterReplyRequested;
        }

        @Override
        public void simpleString(String value) {
            this.simpleString = value;
        }

        @Override
        public void error(String message) {
            this.error = message;
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
            this.bulkString = data == null ? null : new String(data, StandardCharsets.UTF_8);
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
