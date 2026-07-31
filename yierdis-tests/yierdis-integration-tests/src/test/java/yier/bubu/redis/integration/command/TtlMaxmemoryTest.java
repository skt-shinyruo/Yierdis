package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.CommandDispatcher;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.createFfmDb;

public class TtlMaxmemoryTest {
    @Test
    public void expireUpdatesDeadlineWithoutPhysicalGrowthUnderNoeviction() {
        assertTtlMutationKeepsPhysicalUsageStable(
                List.of(b("EXPIRE"), b("k"), b("60")),
                db -> db.writes().ttl().expire(view(b("k")), 60L).value()
        );
    }

    @Test
    public void pexpireUpdatesDeadlineWithoutPhysicalGrowthUnderNoeviction() {
        assertTtlMutationKeepsPhysicalUsageStable(
                List.of(b("PEXPIRE"), b("k"), b("60000")),
                db -> db.writes().ttl().pexpire(view(b("k")), 60_000L).value()
        );
    }

    @Test
    public void pexpireatUpdatesDeadlineWithoutPhysicalGrowthUnderNoeviction() {
        long unixMillis = System.currentTimeMillis() + 60_000L;
        assertTtlMutationKeepsPhysicalUsageStable(
                List.of(b("PEXPIREAT"), b("k"), b(Long.toString(unixMillis))),
                db -> db.writes().ttl().expireAtMillis(view(b("k")), unixMillis).value()
        );
    }

    @Test
    public void setWithExpireOptionIsRejectedWhenTheEntryDoesNotFitUnderNoeviction() {
        byte[] key = b("k");
        byte[] value = b("v");
        long maxmemoryBytes = minMaxmemoryThatAllowsSetWithTtl(key, value) - 1L;

        YierdisDb db = openFfm(maxmemoryBytes);
        db.bindToCurrentThread();

        CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
        try (FastTestClient client = new FastTestClient(dispatcher)) {
            ReplyObject set = client.execute(List.of(b("SET"), key, value, b("EX"), b("60")));
            Assert.assertTrue(set instanceof ReplyError);
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, ((ReplyError) set).message());

            ReplyObject get = client.execute(List.of(b("GET"), key));
            Assert.assertTrue(get instanceof ReplyNull);
        } finally {
            db.shutdown();
        }
    }

    private static void assertTtlMutationKeepsPhysicalUsageStable(
            List<byte[]> ttlCommand,
            TtlMutation ttlMutation
    ) {
        byte[] key = b("k");
        byte[] value = b("v");
        long maxmemoryBytes = minMaxmemoryThatAllowsPlainSetAndTtlMutation(key, value, ttlMutation);

        YierdisDb db = openFfm(maxmemoryBytes);
        db.bindToCurrentThread();

        CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
        try (FastTestClient client = new FastTestClient(dispatcher)) {
            Assert.assertTrue(client.execute(List.of(b("SET"), key, value)) instanceof ReplySimpleString);
            long usedBefore = db.memory().memoryStats().usedBytesForMaxmemory();
            long nativeDataBefore = db.memory().memoryStats().nativeDataCommittedBytes();

            ReplyObject expire = client.execute(ttlCommand);
            Assert.assertTrue(expire instanceof ReplyInteger);
            Assert.assertEquals(1L, ((ReplyInteger) expire).value());

            long usedAfter = db.memory().memoryStats().usedBytesForMaxmemory();
            long nativeDataAfter = db.memory().memoryStats().nativeDataCommittedBytes();
            Assert.assertEquals(usedBefore, usedAfter);
            Assert.assertEquals(nativeDataBefore, nativeDataAfter);
            Assert.assertEquals(1, db.memory().memoryStats().expireCount());
            Assert.assertTrue("physical usage must stay within the admitted maxmemory budget", usedAfter <= maxmemoryBytes);

            ReplyObject get = client.execute(List.of(b("GET"), key));
            Assert.assertTrue(get instanceof ReplyBulkString);
        } finally {
            db.shutdown();
        }
    }

    private static long minMaxmemoryThatAllowsPlainSetAndTtlMutation(
            byte[] key,
            byte[] value,
            TtlMutation ttlMutation
    ) {
        long high = 1L;
        while (!allowsPlainSetAndTtlMutation(high, key, value, ttlMutation)) {
            high = Math.multiplyExact(high, 2L);
        }

        long low = 0L;
        while (low + 1L < high) {
            long mid = low + (high - low) / 2L;
            if (allowsPlainSetAndTtlMutation(mid, key, value, ttlMutation)) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return high;
    }

    private static long minMaxmemoryThatAllowsSetWithTtl(byte[] key, byte[] value) {
        long high = 1L;
        while (!allowsSetWithTtl(high, key, value)) {
            high = Math.multiplyExact(high, 2L);
        }

        long low = 0L;
        while (low + 1L < high) {
            long mid = low + (high - low) / 2L;
            if (allowsSetWithTtl(mid, key, value)) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return high;
    }

    private static boolean allowsSetWithTtl(long maxmemoryBytes, byte[] key, byte[] value) {
        YierdisDb db = openFfm(maxmemoryBytes);
        try {
            db.bindToCurrentThread();
            return db.writes().strings().setString(key, value, SetMode.NORMAL, ExpireOption.px(60_000L)).value();
        } catch (YierdisCommandException e) {
            if (MaxmemoryErrors.OOM_ERR.equals(e.getMessage())) {
                return false;
            }
            throw e;
        } finally {
            db.shutdown();
        }
    }

    private static boolean allowsPlainSetAndTtlMutation(
            long maxmemoryBytes,
            byte[] key,
            byte[] value,
            TtlMutation ttlMutation
    ) {
        YierdisDb db = openFfm(maxmemoryBytes);
        try {
            db.bindToCurrentThread();
            return db.writes().strings().setString(key, value, SetMode.NORMAL, null).value()
                    && ttlMutation.apply(db);
        } catch (YierdisCommandException e) {
            if (MaxmemoryErrors.OOM_ERR.equals(e.getMessage())) {
                return false;
            }
            throw e;
        } finally {
            db.shutdown();
        }
    }

    private static yier.bubu.redis.bytes.BytesView view(byte[] data) {
        return new yier.bubu.redis.bytes.BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                return data[index];
            }
        };
    }

    private static YierdisDb openFfm(long maxmemoryBytes) {
        return createFfmDb(new DbEngineConfig(
                0,
                maxmemoryBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ), 0);
    }

    @FunctionalInterface
    private interface TtlMutation {
        boolean apply(YierdisDb db);
    }
}
