package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.ops.SetMode;

import java.nio.charset.StandardCharsets;

public class KeysBudgetTest {
    @Test
    public void keysReturnsPartialResultsWhenTimeBudgetExceeded() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();
        try {
            // 适当放大数据量：避免在 nanoTime 分辨率较粗时出现“1ns 预算仍然跑完”的偶发现象。
            for (int i = 0; i < 4096; i++) {
                byte[] key = ("k" + i).getBytes(StandardCharsets.US_ASCII);
                byte[] val = ("v" + i).getBytes(StandardCharsets.US_ASCII);
                db.setString(key, val, SetMode.NORMAL, null);
            }

            Assert.assertTrue(
                    "expected KEYS to return partial results under extreme time budget",
                    db.keys("*".getBytes(StandardCharsets.US_ASCII), Integer.MAX_VALUE, 1L).size() < 4096
            );
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void keysReturnsPartialResultsWhenResultLimitExceeded() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();
        try {
            for (int i = 0; i < 4; i++) {
                byte[] key = ("k" + i).getBytes(StandardCharsets.US_ASCII);
            db.setString(key, "v".getBytes(StandardCharsets.US_ASCII), SetMode.NORMAL, null);
            }

            Assert.assertEquals(
                    "expected KEYS to return at most the configured maxMatches",
                    1,
                    db.keys("*".getBytes(StandardCharsets.US_ASCII), 1, 0L).size()
            );
        } finally {
            db.shutdown();
        }
    }
}
