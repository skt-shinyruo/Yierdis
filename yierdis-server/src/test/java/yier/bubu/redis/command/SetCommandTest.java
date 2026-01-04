package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespSimpleString;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class SetCommandTest {
    @Test
    public void upgradeFromIntsetKeepsExistingMembers() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = new byte[]{'s', 0, 1};

            RespInteger added = (RespInteger) client.execute(Arrays.asList(
                    b("SADD"),
                    key,
                    b("1"),
                    b("2"),
                    b("3")
            ));
            Assert.assertEquals(3, added.value());

            byte[] binaryMember = new byte[]{0, (byte) 0xFF, 'x'};
            RespInteger added2 = (RespInteger) client.execute(Arrays.asList(
                    b("SADD"),
                    key,
                    binaryMember
            ));
            Assert.assertEquals(1, added2.value());

            RespInteger is2 = (RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("2")));
            Assert.assertEquals(1, is2.value());

            RespInteger isBin = (RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, binaryMember));
            Assert.assertEquals(1, isBin.value());

            RespArray members = (RespArray) client.execute(Arrays.asList(b("SMEMBERS"), key));
            Assert.assertEquals(4, members.values().size());
            Assert.assertTrue(containsBytes(members, b("1")));
            Assert.assertTrue(containsBytes(members, b("2")));
            Assert.assertTrue(containsBytes(members, b("3")));
            Assert.assertTrue(containsBytes(members, binaryMember));
            }
        });
    }

    @Test
    public void setMembersAreBinarySafeEvenWhenIntegerLike() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("s");

            RespInteger added = (RespInteger) client.execute(Arrays.asList(
                    b("SADD"),
                    key,
                    b("1"),
                    b("01"),
                    b("+1"),
                    b("0"),
                    b("-0")
            ));
            Assert.assertEquals(5, added.value());

            RespInteger card = (RespInteger) client.execute(Arrays.asList(b("SCARD"), key));
            Assert.assertEquals(5, card.value());

            Assert.assertEquals(1, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("1")))).value());
            Assert.assertEquals(1, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("01")))).value());
            Assert.assertEquals(1, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("+1")))).value());
            Assert.assertEquals(1, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("0")))).value());
            Assert.assertEquals(1, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("-0")))).value());

            RespInteger removed = (RespInteger) client.execute(Arrays.asList(b("SREM"), key, b("01")));
            Assert.assertEquals(1, removed.value());

            RespInteger card2 = (RespInteger) client.execute(Arrays.asList(b("SCARD"), key));
            Assert.assertEquals(4, card2.value());

            Assert.assertEquals(1, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("1")))).value());
            Assert.assertEquals(0, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("01")))).value());
            Assert.assertEquals(1, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("+1")))).value());
            Assert.assertEquals(1, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("0")))).value());
            Assert.assertEquals(1, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("-0")))).value());
            }
        });
    }

    @Test
    public void sremDeletesKeyWhenEmpty() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = new byte[]{0, 's'};
            byte[] member = new byte[]{(byte) 0xFF};

            RespInteger added = (RespInteger) client.execute(Arrays.asList(b("SADD"), key, member));
            Assert.assertEquals(1, added.value());

            RespInteger removed = (RespInteger) client.execute(Arrays.asList(b("SREM"), key, member));
            Assert.assertEquals(1, removed.value());

            RespInteger card = (RespInteger) client.execute(Arrays.asList(b("SCARD"), key));
            Assert.assertEquals(0, card.value());

            RespArray members = (RespArray) client.execute(Arrays.asList(b("SMEMBERS"), key));
            Assert.assertTrue(members.values().isEmpty());

            RespInteger exists = (RespInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(0, exists.value());

            RespSimpleString type = (RespSimpleString) client.execute(Arrays.asList(b("TYPE"), key));
            Assert.assertEquals("none", type.value());
            }
        });
    }

    private static boolean containsBytes(RespArray array, byte[] expected) {
        for (Object o : array.values()) {
            if (o instanceof RespBulkString && Arrays.equals(expected, ((RespBulkString) o).data())) {
                return true;
            }
        }
        return false;
    }
}
