package yier.bubu.redis.app.server;

// 连接关闭清理回归：MULTI 期间关闭连接必须清空事务队列，避免大请求数据长期驻留。

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.TransactionState;

import java.util.Arrays;

public class TransactionQueueCleanupTest {
    @Test
    public void closingConnectionDiscardsTransactionState() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(ch, 1, 16);
            TransactionState tx = connection.session().transaction();

            tx.begin();
            Assert.assertTrue(tx.active());
            Assert.assertFalse(tx.aborted());
            Assert.assertEquals(0, tx.size());

            Assert.assertNull(tx.tryEnqueue(request("SET", "k", "v")));
            Assert.assertEquals(1, tx.size());

            // 触发一次入队失败，确保 aborted 标记也能被清理。
            Assert.assertNotNull(tx.tryEnqueue(request("GET", "k")));
            Assert.assertTrue(tx.aborted());

            Assert.assertTrue(connection.markClosing());
            Assert.assertFalse(tx.active());
            Assert.assertFalse(tx.aborted());
            Assert.assertEquals(0, tx.size());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static ByteArrayExecutionRequest request(String... args) {
        return ByteArrayExecutionRequest.fromUtf8(args[0], Arrays.asList(Arrays.copyOfRange(args, 1, args.length)));
    }
}
