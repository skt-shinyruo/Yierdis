package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

/**
 * #165：同步 FLUSHDB 的每 key 耗时，以及已有大量 key 之后再写一批 SET 的耗时，
 * 都不能随 key 数超线性上涨。阈值是 4 倍数据量下每 key 成本不超过 2 倍。
 * seam 是 FastTestClient 经 CommandDispatcher。
 */
public class IssueDraftPerfRegressionTest {
    private static final int SMALL = 25_000;
    private static final int LARGE = 100_000;
    private static final double MAX_PER_KEY_RATIO = 2.0;

    @Test
    public void syncFlushdbPerKeyCostDoesNotGrowWithKeyCount() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);
            fill(client, 2_000, "warm:");
            flushNanos(client);
            double small = (double) median(3, () -> {
                fill(client, SMALL, "s:");
                return flushNanos(client);
            }) / SMALL;
            double large = (double) median(3, () -> {
                fill(client, LARGE, "s:");
                return flushNanos(client);
            }) / LARGE;
            String flushDetail = String.format(
                    "FLUSHDB per-key cost %.0f ns at %d keys vs %.0f ns at %d keys (ratio %.2f)",
                    large,
                    LARGE,
                    small,
                    SMALL,
                    large / small
            );
            System.out.println(flushDetail);
            Assert.assertTrue(flushDetail, large <= small * MAX_PER_KEY_RATIO);
        });
    }

    @Test
    public void setBatchCostDoesNotGrowWithKeyCount() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);
            fill(client, 2_000, "warm:");
            // 三次取样的 key 会累加，中位数取排序后的中间一次，和 #165 的草稿一致。
            long empty = median(3, () -> timedFill(client, 10_000, "e"));
            fill(client, LARGE, "bulk:");
            long loaded = median(3, () -> timedFill(client, 10_000, "l"));
            String setDetail = "10k SETs took " + loaded + " ns after " + LARGE + " keys vs " + empty
                    + " ns near-empty (ratio " + String.format("%.2f", (double) loaded / empty) + ")";
            System.out.println(setDetail);
            Assert.assertTrue(setDetail, loaded <= empty * MAX_PER_KEY_RATIO);
        });
    }

    private interface Sample {
        long run();
    }

    private static int round;

    private static long median(int n, Sample sample) {
        long[] values = new long[n];
        for (int i = 0; i < n; i++) {
            values[i] = sample.run();
        }
        Arrays.sort(values);
        return values[n / 2];
    }

    private static long timedFill(FastTestClient client, int count, String prefix) {
        String uniquePrefix = prefix + (round++) + ":";
        long started = System.nanoTime();
        fill(client, count, uniquePrefix);
        return System.nanoTime() - started;
    }

    private static void fill(FastTestClient client, int count, String prefix) {
        for (int i = 0; i < count; i++) {
            ReplySimpleString reply = (ReplySimpleString) client.execute(List.of(b("SET"), b(prefix + i), b("v" + i)));
            Assert.assertEquals("OK", reply.value());
        }
    }

    private static long flushNanos(FastTestClient client) {
        long started = System.nanoTime();
        ReplySimpleString reply = (ReplySimpleString) client.execute(cmd("FLUSHDB"));
        long elapsed = System.nanoTime() - started;
        Assert.assertEquals("OK", reply.value());
        return elapsed;
    }
}
