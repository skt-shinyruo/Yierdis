package yier.bubu.redis.client;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;

public class PipelineTransactionTest {
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
    public void closingAfterSyncStillClosesWhenAServerErrorWasNotTaken() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Pipeline pipeline = connection.pipeline();
            Reply<String> ok = pipeline.set("k", "v");
            pipeline.command("NO_SUCH");
            pipeline.sync();
            Assert.assertEquals("OK", ok.get());
            pipeline.close();
            try {
                connection.ping();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
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
