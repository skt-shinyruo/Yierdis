package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class MemoryStatsAccountingConsistencyTest {
    @Test
    public void memoryStatsUsedBytesForMaxmemoryMatchesEnforcementIncludingTtlEstimate() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().pexpire(view(b("k")), 10_000).value());

            long enforcement = db.usedBytesForMaxmemory();
            long stats = db.memory().memoryStats().usedBytesForMaxmemory();
            Assert.assertEquals(enforcement, stats);
        } finally {
            db.shutdown();
        }
    }

    private static BytesView view(byte[] data) {
        return new BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                if (index < 0 || index >= data.length) {
                    throw new IndexOutOfBoundsException();
                }
                return data[index];
            }
        };
    }
}
