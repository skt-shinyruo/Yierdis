package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

/**
 * 命令注册护栏测试：确保命令处理器至少注册一组跨 domain 的最小命令集。
 * <p>
 * 该列表刻意保持“小且稳定”，用于防止新增/拆分命令模块时出现“忘记注册导致静默退化”为 unknown command。
 */
public class CommandRegistryGuardTest {
    @Test
    public void minimalCommandSetIsRegistered() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                assertNotUnknown(client.execute(cmd("PING")));

                assertNotUnknown(client.execute(cmd("SET", "k", "v")));
                assertNotUnknown(client.execute(cmd("GET", "k")));

                assertNotUnknown(client.execute(cmd("LPUSH", "l", "a")));
                assertNotUnknown(client.execute(cmd("HSET", "h", "f", "v")));
                assertNotUnknown(client.execute(cmd("SADD", "s", "m")));
                assertNotUnknown(client.execute(cmd("ZADD", "z", "1", "m")));

                assertNotUnknown(client.execute(cmd("PFADD", "hll", "a")));

                // 子命令风格（例如 MEMORY STATS）。
                assertNotUnknown(client.execute(Arrays.asList(b("MEMORY"), b("STATS"))));
            }
        });
    }

    private static void assertNotUnknown(ReplyObject reply) {
        if (!(reply instanceof ReplyError)) {
            return;
        }
        String message = ((ReplyError) reply).message();
        Assert.assertNotEquals("ERR unknown command", message);
    }
}
