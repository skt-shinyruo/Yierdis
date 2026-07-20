package yier.bubu.redis.common.command;

import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class MutationContextTest {
    @Test
    public void noneHasNoCommandRecord() {
        MutationContext context = MutationContext.none();

        Assert.assertFalse(context.hasCommandRecord());
        Assert.assertNull(context.commandRecord());
        Assert.assertNull(context.retainCommandRecord());
    }

    @Test
    public void contextBorrowsTheRecordAndRetainCreatesIndependentOwnership() {
        ByteArrayCommandRecord owner = ByteArrayCommandRecord.copyOf(bytes("SET"), bytes("key"), bytes("value"));
        MutationContext context = MutationContext.of(owner);

        Assert.assertTrue(context.hasCommandRecord());
        Assert.assertSame(owner, context.commandRecord());

        ImmutableCommandRecord retained = context.retainCommandRecord();
        owner.close();
        try {
            Assert.assertArrayEquals(bytes("SET"), retained.toByteArray(0));
        } finally {
            retained.close();
        }
    }

    @Test
    public void closeDropsTheBorrowWithoutClosingTheOwner() {
        ByteArrayCommandRecord owner = ByteArrayCommandRecord.copyOf(bytes("SET"), bytes("key"), bytes("value"));
        MutationContext context = MutationContext.of(owner);

        context.close();

        Assert.assertFalse(context.hasCommandRecord());
        Assert.assertNull(context.commandRecord());
        Assert.assertArrayEquals(bytes("SET"), owner.toByteArray(0));
        owner.close();
    }

    @Test
    public void ofRejectsNull() {
        Assert.assertThrows(NullPointerException.class, () -> MutationContext.of(null));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
