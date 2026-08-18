package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class SetCommandTest {
    @Test
    public void upgradeFromIntsetKeepsExistingMembers() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = new byte[]{'s', 0, 1};

            ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(
                    b("SADD"),
                    key,
                    b("1"),
                    b("2"),
                    b("3")
            ));
            Assert.assertEquals(3, added.value());

            byte[] binaryMember = new byte[]{0, (byte) 0xFF, 'x'};
            ReplyInteger added2 = (ReplyInteger) client.execute(Arrays.asList(
                    b("SADD"),
                    key,
                    binaryMember
            ));
            Assert.assertEquals(1, added2.value());

            ReplyInteger is2 = (ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("2")));
            Assert.assertEquals(1, is2.value());

            ReplyInteger isBin = (ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, binaryMember));
            Assert.assertEquals(1, isBin.value());

            ReplyArray members = (ReplyArray) client.execute(Arrays.asList(b("SMEMBERS"), key));
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
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("s");

            ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(
                    b("SADD"),
                    key,
                    b("1"),
                    b("01"),
                    b("+1"),
                    b("0"),
                    b("-0")
            ));
            Assert.assertEquals(5, added.value());

            ReplyInteger card = (ReplyInteger) client.execute(Arrays.asList(b("SCARD"), key));
            Assert.assertEquals(5, card.value());

            Assert.assertEquals(1, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("1")))).value());
            Assert.assertEquals(1, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("01")))).value());
            Assert.assertEquals(1, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("+1")))).value());
            Assert.assertEquals(1, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("0")))).value());
            Assert.assertEquals(1, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("-0")))).value());

            ReplyInteger removed = (ReplyInteger) client.execute(Arrays.asList(b("SREM"), key, b("01")));
            Assert.assertEquals(1, removed.value());

            ReplyInteger card2 = (ReplyInteger) client.execute(Arrays.asList(b("SCARD"), key));
            Assert.assertEquals(4, card2.value());

            Assert.assertEquals(1, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("1")))).value());
            Assert.assertEquals(0, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("01")))).value());
            Assert.assertEquals(1, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("+1")))).value());
            Assert.assertEquals(1, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("0")))).value());
            Assert.assertEquals(1, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("-0")))).value());
            }
        });
    }

    @Test
    public void sremDeletesKeyWhenEmpty() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = new byte[]{0, 's'};
            byte[] member = new byte[]{(byte) 0xFF};

            ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(b("SADD"), key, member));
            Assert.assertEquals(1, added.value());

            ReplyInteger removed = (ReplyInteger) client.execute(Arrays.asList(b("SREM"), key, member));
            Assert.assertEquals(1, removed.value());

            ReplyInteger card = (ReplyInteger) client.execute(Arrays.asList(b("SCARD"), key));
            Assert.assertEquals(0, card.value());

            ReplyArray members = (ReplyArray) client.execute(Arrays.asList(b("SMEMBERS"), key));
            Assert.assertTrue(members.values().isEmpty());

            ReplyInteger exists = (ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(0, exists.value());

            ReplySimpleString type = (ReplySimpleString) client.execute(Arrays.asList(b("TYPE"), key));
            Assert.assertEquals("none", type.value());
            }
        });
    }

    private static boolean containsBytes(ReplyArray array, byte[] expected) {
        for (Object o : array.values()) {
            if (o instanceof ReplyBulkString && Arrays.equals(expected, ((ReplyBulkString) o).data())) {
                return true;
            }
        }
        return false;
    }
}
