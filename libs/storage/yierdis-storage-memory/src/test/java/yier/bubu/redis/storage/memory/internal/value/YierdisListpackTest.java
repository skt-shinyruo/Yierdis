package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class YierdisListpackTest {
    @Test
    public void preservesNullVsEmptyAndSupportsIndexOf() {
        YierdisListpack lp = new YierdisListpack();
        lp.addLast(null);
        lp.addLast(new byte[0]);
        lp.addLast(new byte[]{1, 2});

        Assert.assertEquals(3, lp.size());
        Assert.assertEquals(2, lp.rawBytesSize());

        Assert.assertNull(lp.get(0));
        Assert.assertNotNull(lp.get(1));
        Assert.assertEquals(0, lp.get(1).length);
        Assert.assertArrayEquals(new byte[]{1, 2}, lp.get(2));

        Assert.assertEquals(0, lp.indexOf(null));
        Assert.assertEquals(1, lp.indexOf(new byte[0]));
        Assert.assertEquals(2, lp.indexOf(new byte[]{1, 2}));
        Assert.assertEquals(-1, lp.indexOf(new byte[]{9}));
    }

    @Test
    public void insertAtAndRemoveAtShiftContents() {
        YierdisListpack lp = new YierdisListpack();
        lp.addLast(b("a"));
        lp.addLast(b("c"));

        lp.insertAt(1, b("b"));
        Assert.assertEquals(3, lp.size());
        Assert.assertArrayEquals(b("a"), lp.get(0));
        Assert.assertArrayEquals(b("b"), lp.get(1));
        Assert.assertArrayEquals(b("c"), lp.get(2));

        Assert.assertArrayEquals(b("b"), lp.removeAt(1));
        Assert.assertEquals(2, lp.size());
        Assert.assertArrayEquals(b("a"), lp.get(0));
        Assert.assertArrayEquals(b("c"), lp.get(1));
    }

    @Test
    public void setUpdatesRawBytesAccounting() {
        YierdisListpack lp = new YierdisListpack();
        lp.addLast(b("a"));
        Assert.assertEquals(1, lp.rawBytesSize());

        lp.set(0, b("abc"));
        Assert.assertEquals(3, lp.rawBytesSize());

        lp.set(0, null);
        Assert.assertEquals(0, lp.rawBytesSize());
        Assert.assertNull(lp.get(0));
    }

    @Test
    public void cursorAppendToCopiesEntries() {
        YierdisListpack src = new YierdisListpack();
        src.addLast(b("x"));
        src.addLast(null);
        src.addLast(b(""));

        YierdisListpack dst = new YierdisListpack();
        YierdisListpack.Cursor c = src.cursor();
        while (c.next()) {
            c.appendTo(dst);
        }

        Assert.assertEquals(src.size(), dst.size());
        Assert.assertEquals(src.rawBytesSize(), dst.rawBytesSize());

        Assert.assertArrayEquals(b("x"), dst.get(0));
        Assert.assertNull(dst.get(1));
        Assert.assertNotNull(dst.get(2));
        Assert.assertEquals(0, dst.get(2).length);
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}

