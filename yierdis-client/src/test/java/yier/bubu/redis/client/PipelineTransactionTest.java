package yier.bubu.redis.client;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

public class PipelineTransactionTest {
    @Test
    public void repeatedBatchesReleaseReadRepliesWhileSavedHandlesRemainUsable() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server);
             Pipeline pipeline = connection.pipeline()) {
            Field replies = Pipeline.class.getDeclaredField("replies");
            replies.setAccessible(true);
            Reply<String> saved = pipeline.echo("saved");
            for (int i = 0; i < 100; i++) {
                Reply<String> current = pipeline.echo(Integer.toString(i));
                if (i % 2 == 0) {
                    pipeline.sync();
                }
                Assert.assertEquals(Integer.toString(i), current.get());
                // 直接检查保留数量，避免用 GC 时机或耗时断言掩盖历史回复的线性积累。
                Assert.assertTrue(((Collection<?>) replies.get(pipeline)).isEmpty());
            }
            Assert.assertEquals("saved", saved.get());
        }
    }

    @Test
    public void rawMultiInAPipelineIsRejectedBeforeWrite() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            try (Pipeline pipeline = connection.pipeline()) {
                Reply<String> before = pipeline.set("before", "stored");
                Assert.assertThrows(IllegalStateException.class, () -> pipeline.command("mUlTi"));
                Reply<String> after = pipeline.set("after", "stored");
                Assert.assertEquals("OK", before.get());
                Assert.assertEquals("OK", after.get());
            }
            Assert.assertEquals("stored", connection.get("before"));
            Assert.assertEquals("stored", connection.get("after"));
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void rawConnectionTransactionUsesTheSameModeAndCanFinishNormally() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", connection.command("mUlTi"));
            Assert.assertThrows(IllegalStateException.class, connection::pipeline);
            Assert.assertThrows(IllegalStateException.class, connection::multi);
            Assert.assertThrows(IllegalStateException.class, connection::ping);
            Assert.assertThrows(IllegalStateException.class, () -> connection.command("MULTI"));
            Assert.assertThrows(IllegalArgumentException.class, () -> connection.command(0, "SET", "k", "bad"));
            Assert.assertEquals("QUEUED", connection.command(1_000, "SET", "k", "kept"));
            Assert.assertEquals(List.of("OK"), connection.command(1_000, "eXeC"));
            Assert.assertEquals("kept", connection.get("k"));
            Assert.assertEquals("OK", connection.command("MULTI"));
            Assert.assertEquals("QUEUED", connection.command("SET", "k", "discarded"));
            Assert.assertEquals("OK", connection.command("dIsCaRd"));
            Assert.assertEquals("kept", connection.get("k"));
        }
    }

    @Test
    public void rawExecPreservesRespShapesAndRawDiscardFinishesTheObject() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            connection.hset("hash", "field", "value");
            Transaction executed = connection.multi();
            executed.hgetall("hash");
            executed.set("n", "abc");
            executed.incr("n");
            Assert.assertThrows(IllegalStateException.class, () -> executed.command("MULTI"));
            Assert.assertEquals(List.of(List.of("field", "value"), "OK", "ERR value is not an integer or out of range"),
                    executed.command("EXEC"));
            Assert.assertThrows(IllegalStateException.class, () -> executed.command("PING"));
            Assert.assertEquals("PONG", connection.ping());

            Transaction discarded = connection.multi();
            discarded.set("n", "discarded");
            Assert.assertEquals("OK", discarded.command("DISCARD"));
            Assert.assertThrows(IllegalStateException.class, discarded::exec);
            Assert.assertEquals("abc", connection.get("n"));
        }
    }

    @Test
    public void rawExecAbortEndsTheTransactionAndSuccessfulSelectUpdatesTheDatabase() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction aborted = connection.multi();
            aborted.set("k", "discarded");
            Assert.assertThrows(ServerException.class, () -> aborted.command("SET", "only-key"));
            Assert.assertThrows(ServerException.class, () -> aborted.command("EXEC"));
            Assert.assertThrows(IllegalStateException.class, aborted::exec);
            Assert.assertNull(connection.get("k"));

            Assert.assertEquals("OK", connection.command("MULTI"));
            Assert.assertEquals("QUEUED", connection.command("SELECT", "1"));
            Assert.assertEquals(0, connection.database());
            Assert.assertEquals(List.of("OK"), connection.command("EXEC"));
            Assert.assertEquals(1, connection.database());
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void pipelinedSetsReturnInSendOrderAndAServerErrorStaysOnItsHandle() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Pipeline pipeline = connection.pipeline();
            Reply<String> first = pipeline.set("a", "1");
            Reply<String> second = pipeline.set("b", "2");
            Reply<Object> bad = pipeline.command("NO_SUCH");
            Reply<String> third = pipeline.set("c", "3");

            Assert.assertEquals("OK", first.get());
            Assert.assertEquals("OK", second.get());
            try {
                bad.get();
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR unknown command 'NO_SUCH'", e.getMessage());
            }
            Assert.assertEquals("OK", first.get());
            Assert.assertEquals("OK", third.get());

            pipeline.close();
            Assert.assertEquals("1", connection.get("a"));
            Assert.assertEquals("2", connection.get("b"));
            Assert.assertEquals("3", connection.get("c"));
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void syncLeavesTheSamePipelineAbleToWriteAnotherBatch() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Pipeline pipeline = connection.pipeline();
            Reply<String> first = pipeline.set("a", "1");
            pipeline.sync();
            Assert.assertSame(pipeline, connection.pipeline());
            Reply<String> second = pipeline.set("b", "2");
            Assert.assertEquals("OK", first.get());
            Assert.assertEquals("OK", second.get());
            pipeline.close();
            Assert.assertEquals("1", connection.get("a"));
            Assert.assertEquals("2", connection.get("b"));
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void unreadPipelineRejectsANormalCommandAndClosingItClosesTheConnection() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Pipeline pipeline = connection.pipeline();
            Reply<String> set = pipeline.set("k", "v");
            Reply<Long> incr = pipeline.incr("n");
            try {
                incr.get(0);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("commandTimeoutMillis"));
            }
            try {
                connection.ping();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            try {
                connection.multi();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            Assert.assertEquals("OK", set.get());
            Assert.assertEquals(Long.valueOf(1), incr.get());
            pipeline.close();
            Assert.assertEquals("v", connection.get("k"));
            Assert.assertEquals("1", connection.get("n"));

            Pipeline unread = connection.pipeline();
            unread.set("k", "later");
            unread.close();
            try {
                connection.ping();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
        }
    }

    @Test
    public void closingAfterAReadServerErrorLeavesTheConnectionOpen() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Pipeline pipeline = connection.pipeline();
            Reply<String> ok = pipeline.set("k", "v");
            Reply<Object> bad = pipeline.command("NO_SUCH");
            Reply<String> later = pipeline.set("k2", "v2");
            Assert.assertEquals("OK", later.get());
            pipeline.close();
            Assert.assertEquals("PONG", connection.ping());
            try {
                bad.get();
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR unknown command 'NO_SUCH'", e.getMessage());
            }
            Assert.assertEquals("OK", ok.get());
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void gettingTheFirstPipelineReplyReadsTheRestSoCloseKeepsTheConnection() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Pipeline pipeline = connection.pipeline();
            Reply<String> first = pipeline.set("a", "1");
            pipeline.set("b", "2");
            pipeline.set("c", "3");
            Assert.assertEquals("OK", first.get());
            pipeline.close();
            Assert.assertEquals("PONG", connection.ping());
            Assert.assertEquals("1", connection.get("a"));
            Assert.assertEquals("2", connection.get("b"));
            Assert.assertEquals("3", connection.get("c"));
        }
    }

    @Test
    public void multiSetDoesNotReturnOkAndExecListsResultsInOrder() throws Exception {
        Method set = Transaction.class.getMethod("set", String.class, String.class);
        Assert.assertEquals(void.class, set.getReturnType());
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction transaction = connection.multi();
            transaction.set("a", "1");
            Assert.assertEquals("QUEUED", transaction.command("SET", "b", "2"));
            transaction.set("n", "abc");
            transaction.incr("n");
            transaction.set("m", "ok");
            List<Object> results = transaction.exec();
            Assert.assertEquals(5, results.size());
            Assert.assertEquals("OK", results.get(0));
            Assert.assertEquals("OK", results.get(1));
            Assert.assertEquals("OK", results.get(2));
            Assert.assertTrue(results.get(3) instanceof ServerException);
            Assert.assertEquals(
                    "ERR value is not an integer or out of range",
                    ((ServerException) results.get(3)).getMessage()
            );
            Assert.assertEquals("OK", results.get(4));
            Assert.assertEquals("1", connection.get("a"));
            Assert.assertEquals("2", connection.get("b"));
            Assert.assertEquals("abc", connection.get("n"));
            Assert.assertEquals("ok", connection.get("m"));
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void queueTimeErrorKeepsTransactionModeUntilDiscardOrExec() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction discarded = connection.multi();
            discarded.set("k", "v");
            try {
                discarded.command("SET", "only-key");
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR wrong number of arguments for 'set' command", e.getMessage());
            }
            try {
                connection.ping();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            discarded.discard();
            Assert.assertNull(connection.get("k"));
            try {
                discarded.exec();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            try {
                discarded.set("k", "again");
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            Assert.assertEquals("PONG", connection.ping());

            Transaction aborted = connection.multi();
            aborted.set("k", "v");
            try {
                aborted.command("SET", "only-key");
                Assert.fail("expected ServerException");
            } catch (ServerException expected) {
            }
            try {
                connection.get("k");
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            try {
                aborted.exec();
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals(
                        "EXECABORT Transaction discarded because of previous errors.",
                        e.getMessage()
                );
            }
            try {
                aborted.exec();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            try {
                aborted.command("PING");
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            Assert.assertNull(connection.get("k"));
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void emptyExecReturnsAnEmptyList() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction transaction = connection.multi();
            Assert.assertEquals(List.of(), transaction.exec());
            Assert.assertEquals("PONG", connection.ping());
            try {
                transaction.exec();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
        }
    }

    @Test
    public void pipelineOrTypedCommandDuringTransactionIsRejectedLocally() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", connection.set("k", "old"));
            Transaction transaction = connection.multi();
            transaction.set("k", "new");
            try {
                connection.pipeline();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            try {
                connection.set("k", "from-connection");
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            try {
                connection.multi();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            Assert.assertEquals(List.of("OK"), transaction.exec());
            Assert.assertEquals("new", connection.get("k"));
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void closingMidTransactionDoesNotExec() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (Connection connection = connect(server)) {
                Assert.assertEquals("OK", connection.set("k", "kept"));
                Transaction transaction = connection.multi();
                transaction.set("k", "lost");
                connection.close();
                try {
                    transaction.exec();
                    Assert.fail("expected IllegalStateException");
                } catch (IllegalStateException expected) {
                }
            }
            try (Connection again = connect(server)) {
                Assert.assertEquals("kept", again.get("k"));
            }
        }
    }

    @Test
    public void unencodableArgumentDoesNotLeaveTransactionMode() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Transaction transaction = connection.multi();
            try {
                transaction.set("k", "\uD800");
                Assert.fail("expected DecodeException");
            } catch (DecodeException expected) {
            }
            try {
                connection.ping();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
            transaction.discard();
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    private static Connection connect(TestServer server) {
        return Connection.connect("127.0.0.1", server.port());
    }
}
