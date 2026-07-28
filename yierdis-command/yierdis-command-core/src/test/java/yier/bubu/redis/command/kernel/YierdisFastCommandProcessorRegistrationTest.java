package yier.bubu.redis.command.kernel;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyShapes;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class YierdisFastCommandProcessorRegistrationTest {
    @Test
    public void explicitRegistryRegistersCallerSuppliedModulesOnly() {
        CommandModule extraModule = registration -> registration.register(new CommandDefinition<>(
                syntax("TRACE", CommandArity.exact(1)),
                CommandParsers.args(),
                (request, preparation) -> PreparedCommands.fixed(
                        ReplyShapes.simpleString("TRACE-OK"),
                        execution -> execution.reply().simpleString("TRACE-OK")
                )
        ));

        CommandRegistry registry = CommandRegistries.from(extraModule);
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);

        Assert.assertEquals("ERR unknown command 'PING'", executeError(processor, "PING"));
        Assert.assertEquals("TRACE-OK", executeSimpleString(processor, "TRACE"));
    }

    @Test
    public void processorDoesNotExposeModuleAssemblyConstructors() {
        for (var constructor : YierdisFastCommandProcessor.class.getConstructors()) {
            for (Class<?> parameterType : constructor.getParameterTypes()) {
                Assert.assertNotEquals(CommandModule[].class, parameterType);
                Assert.assertNotEquals(Iterable.class, parameterType);
            }
        }
    }

    @Test
    public void commandModuleRegistrationDoesNotExposeLegacyHandlerOverloads() {
        for (Class<?> nested : CommandModule.class.getDeclaredClasses()) {
            Assert.assertNotEquals("Handler", nested.getSimpleName());
        }
        for (Method method : CommandModule.Registration.class.getMethods()) {
            if (!"register".equals(method.getName())) {
                continue;
            }
            for (Class<?> parameterType : method.getParameterTypes()) {
                Assert.assertFalse(parameterType.getName().endsWith("CommandModule$Handler"));
            }
        }
    }

    @Test
    public void registryMetadataHelpersExposeDirectAndLegacyRegistrations() throws Exception {
        Method definitionByUpperName = CommandRegistry.class.getDeclaredMethod("definitionByUpperName", String.class);
        Assert.assertEquals(CommandDefinition.class, definitionByUpperName.getReturnType());

        Method specByUpperName = CommandRegistry.class.getDeclaredMethod("specByUpperName", String.class);
        Assert.assertEquals(CommandSpec.class, specByUpperName.getReturnType());
    }

    @Test
    public void typedCommandDefinitionCanBeRegisteredAndExecuted() {
        CommandRegistry registry = CommandRegistries.from(
                registration -> registration.register(new CommandDefinition<>(
                        syntax("TYPED", CommandArity.exact(2)),
                        CommandParsers.args(),
                        (args, preparation) -> {
                            byte[] value = args.bytes(1);
                            return PreparedCommands.fixed(
                                    ReplyShapes.bulkString(value.length, 0L),
                                    execution -> execution.reply().bulkString(value)
                            );
                        }
                ))
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);

        Assert.assertEquals("value", executeBulkString(processor, "TYPED", "value"));
    }

    @Test
    public void processorReturnsParseErrorBeforeTypedHandlerRuns() {
        final boolean[] handlerCalled = {false};
        CommandRegistry registry = CommandRegistries.from(
                registration -> registration.register(new CommandDefinition<>(
                        syntax("STRICT", CommandArity.exact(2)),
                        CommandParsers.args(),
                        (args, preparation) -> {
                            handlerCalled[0] = true;
                            return PreparedCommands.fixed(
                                    ReplyShapes.simpleString("OK"),
                                    execution -> execution.reply().simpleString("OK")
                            );
                        }
                ))
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);

        TestReplyWriter out = new TestReplyWriter();
        PreparedCommandTestSupport.execute(
                processor,
                ByteArrayExecutionRequest.fromUtf8("STRICT", List.of()),
                out
        );

        Assert.assertFalse(handlerCalled[0]);
        Assert.assertEquals("ERR wrong number of arguments for 'strict' command", out.error());
    }

    @Test
    public void commandRegistriesRegistersModulesInOrder() {
        CommandRegistry registry = CommandRegistries.from(
                registration -> registration.register(new CommandDefinition<>(
                        syntax("FIRST", CommandArity.exact(1)),
                        CommandParsers.args(),
                        (request, preparation) -> PreparedCommands.fixed(
                                ReplyShapes.simpleString("FIRST"),
                                execution -> execution.reply().simpleString("FIRST")
                        )
                )),
                registration -> registration.register(new CommandDefinition<>(
                        syntax("SECOND", CommandArity.exact(1)),
                        CommandParsers.args(),
                        (request, preparation) -> PreparedCommands.fixed(
                                ReplyShapes.simpleString("SECOND"),
                                execution -> execution.reply().simpleString("SECOND")
                        )
                ))
        );

        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        Assert.assertEquals("FIRST", executeSimpleString(processor, "FIRST"));
        Assert.assertEquals("SECOND", executeSimpleString(processor, "SECOND"));
    }

    @Test
    public void commandRegistriesRejectNullModules() {
        try {
            CommandRegistries.from(
                    java.util.Arrays.asList(
                            registration -> registration.register(new CommandDefinition<>(
                                    syntax("OK", CommandArity.exact(1)),
                                    CommandParsers.args(),
                                    (request, preparation) -> PreparedCommands.fixed(
                                            ReplyShapes.simpleString("OK"),
                                            execution -> execution.reply().simpleString("OK")
                                    )
                            )),
                            null
                    )
            );
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("modules must not contain null", e.getMessage());
        }
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(
                nameUpper, arity, CommandKeySpec.NONE, TransactionPolicy.QUEUEABLE
        );
    }

    private static String executeSimpleString(YierdisFastCommandProcessor processor, String... argv) {
        TestReplyWriter writer = new TestReplyWriter();
        PreparedCommandTestSupport.execute(processor, new ArrayExecutionRequest(argv), writer);
        if (writer.error() != null) {
            Assert.fail("expected simple string reply, got error: " + writer.error());
        }
        Assert.assertNotNull("expected simple string reply", writer.simpleString());
        return writer.simpleString();
    }

    private static String executeError(YierdisFastCommandProcessor processor, String... argv) {
        TestReplyWriter writer = new TestReplyWriter();
        PreparedCommandTestSupport.execute(processor, new ArrayExecutionRequest(argv), writer);
        Assert.assertNotNull("expected error reply", writer.error());
        return writer.error();
    }

    private static String executeBulkString(YierdisFastCommandProcessor processor, String command, String arg) {
        TestReplyWriter out = new TestReplyWriter();
        PreparedCommandTestSupport.execute(
                processor,
                ByteArrayExecutionRequest.fromUtf8(command, List.of(arg)),
                out
        );
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

    private static final class TestReplyWriter implements RedisReplyWriter {
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
