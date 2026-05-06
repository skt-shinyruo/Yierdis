package yier.bubu.redis.db;

import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.ScanCursorV2;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplySimpleString;
import yier.bubu.redis.runtime.TestCommandProcessors;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.cmd;

public class YierdisSnapshotTest {
    @Test
    public void snapshotReturnsKeysValuesAndExpireAtForStrings() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        try {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(cmd("SET", "a", "1")) instanceof ReplySimpleString);
                Assert.assertTrue(client.execute(cmd("SET", "b", "2")) instanceof ReplySimpleString);
                Assert.assertTrue(client.execute(cmd("EXPIRE", "a", "10")) instanceof ReplyInteger);
            }

            List<YierdisSnapshotEntry> entries = new ArrayList<>();
            YierdisSnapshot snapshot = db.introspection();
            ScanCursorV2 cursor = ScanCursorV2.start();
            int guard = 0;
            do {
                cursor = snapshot.snapshot(cursor, 10, entries);
                guard++;
                Assert.assertTrue("snapshot loop guard triggered", guard < 1000);
            } while (cursor.value() != 0);

            HashMap<String, YierdisSnapshotEntry> map = new HashMap<>();
            for (YierdisSnapshotEntry e : entries) {
                map.put(new String(e.keyBytes(), StandardCharsets.US_ASCII), e);
            }

            Assert.assertTrue(map.containsKey("a"));
            Assert.assertTrue(map.containsKey("b"));

            YierdisSnapshotEntry a = map.get("a");
            Assert.assertEquals(ValueType.STRING, a.type());
            Assert.assertArrayEquals("1".getBytes(StandardCharsets.US_ASCII), a.stringValueBytes());
            Assert.assertNotNull(a.expireAtMillis());
            Assert.assertTrue(a.expireAtMillis() > System.currentTimeMillis());

            YierdisSnapshotEntry b = map.get("b");
            Assert.assertEquals(ValueType.STRING, b.type());
            Assert.assertArrayEquals("2".getBytes(StandardCharsets.US_ASCII), b.stringValueBytes());
            Assert.assertNull(b.expireAtMillis());
        } finally {
            db.shutdown();
        }
    }
}
