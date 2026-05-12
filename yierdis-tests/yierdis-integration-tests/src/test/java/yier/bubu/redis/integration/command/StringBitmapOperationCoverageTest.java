package yier.bubu.redis.integration.command;

import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.testutil.FastTestClient;

import java.util.Arrays;

import static yier.bubu.redis.testutil.ReplyAssertions.assertBulkString;
import static yier.bubu.redis.testutil.ReplyAssertions.assertInteger;
import static yier.bubu.redis.testutil.ReplyAssertions.assertSimpleString;
import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class StringBitmapOperationCoverageTest {
    @Test
    public void stringTemplateCoversBinarySafeSetGetStrlenAppendIncrAndDecr() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
                byte[] value = new byte[]{0, (byte) 0xFE, 'v'};

                assertSimpleString("OK", client.execute(Arrays.asList(b("SET"), key, value)));
                assertBulkString(value, client.execute(Arrays.asList(b("GET"), key)));
                assertInteger(value.length, client.execute(Arrays.asList(b("STRLEN"), key)));

                assertInteger(value.length + 2, client.execute(Arrays.asList(b("APPEND"), key, new byte[]{1, 2})));
                assertBulkString(new byte[]{0, (byte) 0xFE, 'v', 1, 2}, client.execute(Arrays.asList(b("GET"), key)));

                assertSimpleString("OK", client.execute(cmd("SET", "counter", "11")));
                assertInteger(12, client.execute(cmd("INCR", "counter")));
                assertInteger(11, client.execute(cmd("DECR", "counter")));
                assertBulkString("11", client.execute(cmd("GET", "counter")));
            }
        });
    }

    @Test
    public void bitmapTemplateCoversMutationReadsAndByteRanges() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] key = b("bitmap");

                assertInteger(0, client.execute(Arrays.asList(b("GETBIT"), key, b("0"))));
                assertInteger(0, client.execute(Arrays.asList(b("SETBIT"), key, b("0"), b("1"))));
                assertInteger(1, client.execute(Arrays.asList(b("GETBIT"), key, b("0"))));

                assertInteger(0, client.execute(Arrays.asList(b("SETBIT"), key, b("15"), b("1"))));
                assertInteger(2, client.execute(Arrays.asList(b("BITCOUNT"), key)));
                assertInteger(1, client.execute(Arrays.asList(b("BITCOUNT"), key, b("0"), b("0"))));
                assertInteger(1, client.execute(Arrays.asList(b("BITCOUNT"), key, b("1"), b("1"))));
                assertInteger(1, client.execute(Arrays.asList(b("BITCOUNT"), key, b("-1"), b("-1"))));
            }
        });
    }
}
