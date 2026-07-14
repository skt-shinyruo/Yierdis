package yier.bubu.redis.common.command;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class CommandRecordScopeTest {
    @Test
    public void scopeIsNestedAndDoesNotCopyRecord() {
        ImmutableCommandRecord first = record("SET", "a", "1");
        ImmutableCommandRecord second = record("DEL", "a");

        Assert.assertNull(CommandRecordScope.current());
        try (CommandRecordScope.Scope ignored = CommandRecordScope.open(first)) {
            Assert.assertSame(first, CommandRecordScope.current());
            try (CommandRecordScope.Scope nested = CommandRecordScope.open(second)) {
                Assert.assertSame(second, CommandRecordScope.current());
            }
            Assert.assertSame(first, CommandRecordScope.current());
        }
        Assert.assertNull(CommandRecordScope.current());
        first.close();
        second.close();
    }

    @Test
    public void byteArrayRecordCopiesInputSharesRetainedViewsAndClosesIdempotently() {
        byte[] command = ascii("SET");
        ByteArrayCommandRecord first = ByteArrayCommandRecord.copyOf(command, ascii("key"), null, ascii("value"));
        long retained = first.retainedMemoryBytes();
        command[0] = (byte) 'G';

        Assert.assertArrayEquals(ascii("SET"), first.toByteArray(0));
        Assert.assertTrue(retained > 0L);
        try (ImmutableCommandRecord second = first.retain()) {
            Assert.assertEquals(retained, second.retainedMemoryBytes());
            Assert.assertArrayEquals(ascii("value"), second.toByteArray(3));
        }
        first.close();
        first.close();
    }

    @Test
    public void borrowedViewHasNoOwnershipOperations() {
        for (Method method : CommandRecordView.class.getMethods()) {
            Assert.assertNotEquals("retain", method.getName());
            Assert.assertNotEquals("close", method.getName());
        }
    }

    @Test
    public void scopeCloseRequiresItsOwnerThread() throws Exception {
        ImmutableCommandRecord record = record("PING");
        CommandRecordScope.Scope scope = CommandRecordScope.open(record);
        Throwable[] failure = new Throwable[1];
        Thread foreign = Thread.ofPlatform().start(() -> {
            try {
                scope.close();
            } catch (Throwable t) {
                failure[0] = t;
            }
        });
        foreign.join();

        Assert.assertTrue(failure[0] instanceof IllegalStateException);
        scope.close();
        record.close();
    }

    private static ImmutableCommandRecord record(String... argv) {
        byte[][] bytes = new byte[argv.length][];
        for (int i = 0; i < argv.length; i++) {
            bytes[i] = argv[i] == null ? null : ascii(argv[i]);
        }
        return ByteArrayCommandRecord.copyOf(bytes);
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
