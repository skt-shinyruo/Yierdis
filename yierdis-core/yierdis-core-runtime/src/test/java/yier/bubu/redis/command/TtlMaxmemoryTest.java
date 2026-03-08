package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.ops.DbMemoryConstants;
import yier.bubu.redis.ops.MaxmemoryErrors;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;

public class TtlMaxmemoryTest {
    @Test
    public void expireIsRejectedWhenItWouldAddTtlMetadataUnderNoeviction() {
        byte[] key = b("k");
        byte[] value = b("v");
        long maxmemoryBytes = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + key.length + value.length;

        YierdisDb db = new YierdisDb(null, maxmemoryBytes, "noeviction", 5, 5, 5);
        db.bindToCurrentThread();

        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        try (FastTestClient client = new FastTestClient(processor)) {
            Assert.assertTrue(client.execute(List.of(b("SET"), key, value)) instanceof ReplySimpleString);

            ReplyObject expire = client.execute(List.of(b("EXPIRE"), key, b("60")));
            Assert.assertTrue(expire instanceof ReplyError);
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, ((ReplyError) expire).message());

            ReplyObject get = client.execute(List.of(b("GET"), key));
            Assert.assertTrue(get instanceof ReplyBulkString);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void pexpireIsRejectedWhenItWouldAddTtlMetadataUnderNoeviction() {
        byte[] key = b("k");
        byte[] value = b("v");
        long maxmemoryBytes = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + key.length + value.length;

        YierdisDb db = new YierdisDb(null, maxmemoryBytes, "noeviction", 5, 5, 5);
        db.bindToCurrentThread();

        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        try (FastTestClient client = new FastTestClient(processor)) {
            Assert.assertTrue(client.execute(List.of(b("SET"), key, value)) instanceof ReplySimpleString);

            ReplyObject expire = client.execute(List.of(b("PEXPIRE"), key, b("60000")));
            Assert.assertTrue(expire instanceof ReplyError);
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, ((ReplyError) expire).message());

            ReplyObject get = client.execute(List.of(b("GET"), key));
            Assert.assertTrue(get instanceof ReplyBulkString);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void pexpireatIsRejectedWhenItWouldAddTtlMetadataUnderNoeviction() {
        byte[] key = b("k");
        byte[] value = b("v");
        long maxmemoryBytes = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + key.length + value.length;

        YierdisDb db = new YierdisDb(null, maxmemoryBytes, "noeviction", 5, 5, 5);
        db.bindToCurrentThread();

        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        try (FastTestClient client = new FastTestClient(processor)) {
            Assert.assertTrue(client.execute(List.of(b("SET"), key, value)) instanceof ReplySimpleString);

            long unixMillis = System.currentTimeMillis() + 60_000L;
            ReplyObject expire = client.execute(List.of(b("PEXPIREAT"), key, b(Long.toString(unixMillis))));
            Assert.assertTrue(expire instanceof ReplyError);
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, ((ReplyError) expire).message());

            ReplyObject get = client.execute(List.of(b("GET"), key));
            Assert.assertTrue(get instanceof ReplyBulkString);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void setWithExpireOptionIsRejectedWhenItWouldAddTtlMetadataUnderNoeviction() {
        byte[] key = b("k");
        byte[] value = b("v");
        long maxmemoryBytes = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + key.length + value.length;

        YierdisDb db = new YierdisDb(null, maxmemoryBytes, "noeviction", 5, 5, 5);
        db.bindToCurrentThread();

        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        try (FastTestClient client = new FastTestClient(processor)) {
            ReplyObject set = client.execute(List.of(b("SET"), key, value, b("EX"), b("60")));
            Assert.assertTrue(set instanceof ReplyError);
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, ((ReplyError) set).message());

            ReplyObject get = client.execute(List.of(b("GET"), key));
            Assert.assertTrue(get instanceof ReplyNull);
        } finally {
            db.shutdown();
        }
    }
}
