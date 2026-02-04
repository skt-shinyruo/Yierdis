package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class KeysBudgetTest {
    @Test
    public void keysFailsFastWhenTimeBudgetExceeded() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();
        try {
            for (int i = 0; i < 64; i++) {
                byte[] key = ("k" + i).getBytes(StandardCharsets.US_ASCII);
                byte[] val = ("v" + i).getBytes(StandardCharsets.US_ASCII);
                db.setString(key, val, YierdisDb.SetMode.NORMAL, null);
            }

            try {
                db.keys("*".getBytes(StandardCharsets.US_ASCII), Integer.MAX_VALUE, 1L);
                Assert.fail("expected KEYS budget failure");
            } catch (YierdisDb.YierdisCommandException e) {
                Assert.assertTrue(e.getMessage().contains("time budget exceeded"));
            }
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void keysFailsFastWhenResultLimitExceeded() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();
        try {
            for (int i = 0; i < 4; i++) {
                byte[] key = ("k" + i).getBytes(StandardCharsets.US_ASCII);
                db.setString(key, "v".getBytes(StandardCharsets.US_ASCII), YierdisDb.SetMode.NORMAL, null);
            }

            try {
                db.keys("*".getBytes(StandardCharsets.US_ASCII), 1, 0L);
                Assert.fail("expected KEYS limit failure");
            } catch (YierdisDb.YierdisCommandException e) {
                Assert.assertTrue(e.getMessage().contains("result limit exceeded"));
            }
        } finally {
            db.shutdown();
        }
    }
}

