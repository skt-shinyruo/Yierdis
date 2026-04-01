package yier.bubu.redis;

// 连接关闭清理回归：MULTI 期间关闭连接必须清空事务队列，避免大请求数据长期驻留。

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.TransactionState;

import java.nio.charset.StandardCharsets;

public class TransactionQueueCleanupTest {
    @Test
    public void closingConnectionDiscardsTransactionState() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch, 1, 16);
            ServerSessionState session = context.session();
            ServerRuntimeState runtime = context.runtime();
            TransactionState tx = session.transaction();

            tx.begin();
            Assert.assertTrue(tx.active());
            Assert.assertFalse(tx.aborted());
            Assert.assertEquals(0, tx.size());

            Assert.assertNull(tx.tryEnqueue(new byte[][]{b("SET"), b("k"), b("v")}));
            Assert.assertEquals(1, tx.size());

            // 触发一次入队失败，确保 aborted 标记也能被清理。
            Assert.assertNotNull(tx.tryEnqueue(new byte[][]{b("GET"), b("k")}));
            Assert.assertTrue(tx.aborted());

            runtime.markClosing(session);
            Assert.assertFalse(tx.active());
            Assert.assertFalse(tx.aborted());
            Assert.assertEquals(0, tx.size());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
