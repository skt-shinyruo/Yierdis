package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

/**
 * 约 5 万个活跃对象之后，命令层 LPOP 中位数不超过空 DB 基线的 10 倍。
 * 样本含预热；seam 是 FastTestClient 经 CommandDispatcher。断言只看回复内容。
 */
public class ListPopLatencyRegressionTest {
    private static final int ACTIVE_OBJECTS = 50_000;
    private static final int WARMUP = 8;
    private static final int SAMPLES = 21;
    private static final long MAX_RATIO = 10L;

    @Test
    public void lpopMedianStaysWithinTenTimesEmptyBaselineAfterFiftyThousandObjects() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);
            byte[] listKey = b("pop-latency");
            byte[] value = b("x");

            long baselineNanos = medianLpopNanos(client, listKey, value);
            fillActiveObjects(client, ACTIVE_OBJECTS);
            long loadedNanos = medianLpopNanos(client, listKey, value);

            Assert.assertTrue(
                    "LPOP median after " + ACTIVE_OBJECTS + " objects was " + loadedNanos
                            + " ns, empty baseline " + baselineNanos + " ns",
                    loadedNanos <= baselineNanos * MAX_RATIO
            );
        });
    }

    private static void fillActiveObjects(FastTestClient client, int count) {
        for (int i = 0; i < count; i++) {
            client.execute(List.of(b("SET"), b("k" + i), b("v")));
        }
    }

    private static long medianLpopNanos(FastTestClient client, byte[] listKey, byte[] value) {
        for (int i = 0; i < WARMUP; i++) {
            pushAndPop(client, listKey, value);
        }
        long[] samples = new long[SAMPLES];
        for (int i = 0; i < SAMPLES; i++) {
            client.execute(List.of(b("RPUSH"), listKey, value));
            long started = System.nanoTime();
            ReplyObject reply = client.execute(List.of(b("LPOP"), listKey));
            samples[i] = System.nanoTime() - started;
            Assert.assertTrue(reply instanceof ReplyBulkString);
            Assert.assertArrayEquals(value, ((ReplyBulkString) reply).data());
        }
        Arrays.sort(samples);
        return Math.max(1L, samples[SAMPLES / 2]);
    }

    private static void pushAndPop(FastTestClient client, byte[] listKey, byte[] value) {
        client.execute(List.of(b("RPUSH"), listKey, value));
        ReplyObject reply = client.execute(List.of(b("LPOP"), listKey));
        Assert.assertTrue(reply instanceof ReplyBulkString);
        Assert.assertArrayEquals(value, ((ReplyBulkString) reply).data());
    }
}
