package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.EvictionCoordinator;
import yier.bubu.redis.ops.ExpirationManager;
import yier.bubu.redis.ops.KeyspaceOps;
import yier.bubu.redis.ops.MemoryOps;
import yier.bubu.redis.ops.TtlOps;
import yier.bubu.redis.ops.ValueOps;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class YierdisFastCommandProcessorModuleTest {
    @Test
    public void defaultCoreModulesDoNotRegisterServerOnlyCommands() {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                new NoopDbEngine(),
                null,
                SlowCommandGovernor.DEFAULT
        );

        assertUnknownCommand(processor, "HELLO");
        assertUnknownCommand(processor, "INFO");
        assertUnknownCommand(processor, "STATS");
    }

    @Test
    public void extraModulesCanRegisterAdditionalCommands() {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                new NoopDbEngine(),
                null,
                SlowCommandGovernor.DEFAULT,
                registrar -> registrar.register("LOCAL", (cmd, ctx) -> ctx.out().simpleString("LOCAL_OK"))
        );

        CapturingReplyWriter out = new CapturingReplyWriter();
        processor.execute(new StaticCommand("LOCAL"), new CommandContext(null, out));

        Assert.assertEquals("LOCAL_OK", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    private static void assertUnknownCommand(YierdisFastCommandProcessor processor, String commandName) {
        CapturingReplyWriter out = new CapturingReplyWriter();
        processor.execute(new StaticCommand(commandName), new CommandContext(null, out));

        Assert.assertNull(out.simpleStringValue);
        Assert.assertEquals("ERR unknown command '" + commandName + "'", out.errorValue);
    }

    private static final class StaticCommand implements Command {
        private final byte[][] argv;

        private StaticCommand(String... args) {
            this.argv = new byte[args.length][];
            for (int i = 0; i < args.length; i++) {
                this.argv[i] = args[i] == null ? null : args[i].getBytes(StandardCharsets.US_ASCII);
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
            byte[] arg = argv[index];
            return arg == null ? null : arg.clone();
        }

        @Override
        public void close() {
        }
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

    private static final class NoopDbEngine implements DbEngine {
        @Override
        public DbReads reads() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbWrites writes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ValueOps values() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpirationManager expiration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EvictionCoordinator eviction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyspaceOps keyspace() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TtlOps ttl() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryOps memory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }
}
