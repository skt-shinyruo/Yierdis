package yier.bubu.redis.client;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.client.exception.BorrowException;
import yier.bubu.redis.client.exception.CommandTimeoutException;
import yier.bubu.redis.client.exception.ConnectionException;
import yier.bubu.redis.client.exception.ServerException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectionPoolTest {
    @Test
    public void rawMultiIsClosedOnReturnInsteadOfLeakingToTheNextBorrower() throws Exception {
        try (TestServer server = TestServer.start();
             ConnectionPool pool = new ConnectionPool(settings(server), 1, 1_000)) {
            Connection borrowed = pool.borrow();
            Assert.assertEquals("OK", Await.join(borrowed.command("MULTI")));
            borrowed.command("SET", "k", "uncommitted");
            pool.returnConnection(borrowed);
            assertClosed(borrowed);
            Connection next = pool.borrow();
            Assert.assertNotSame(borrowed, next);
            Assert.assertEquals("PONG", Await.join(next.ping()));
            Assert.assertNull(Await.join(next.get("k")));
            pool.returnConnection(next);
        }
    }

    @Test
    public void rawExecWithSelectCannotReturnAConnectionOnTheWrongDatabase() throws Exception {
        try (TestServer server = TestServer.start();
             ConnectionPool pool = new ConnectionPool(settings(server), 1, 1_000)) {
            Connection borrowed = pool.borrow();
            borrowed.command("MULTI");
            borrowed.command("SELECT", "1");
            borrowed.command("SET", "k", "on-one");
            Assert.assertEquals(java.util.List.of("OK", "OK"), Await.join(borrowed.command("EXEC")));
            Assert.assertEquals(1, borrowed.database());
            pool.returnConnection(borrowed);
            assertClosed(borrowed);
            Connection next = pool.borrow();
            Assert.assertNotSame(borrowed, next);
            Assert.assertEquals(0, next.database());
            Assert.assertNull(Await.join(next.get("k")));
            pool.returnConnection(next);
        }
    }

    @Test
    public void rejectsNonPositiveMaximumAndNegativeBorrowWaitBeforeConnect() {
        Assert.assertFalse(CommandTimeoutException.class.isAssignableFrom(BorrowException.class));
        Assert.assertFalse(ConnectionException.class.isAssignableFrom(BorrowException.class));
        ConnectionSettings settings = ConnectionSettings.defaults();
        try {
            new ConnectionPool(settings, 0, 0);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("maximumSize"));
        }
        try {
            new ConnectionPool(settings, -1, 1_000);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("maximumSize"));
        }
        try {
            new ConnectionPool(settings, 1, -1);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("borrowWaitMillis"));
        }
    }

    @Test
    public void secondBorrowFailsWhenTheOnlyConnectionIsOutAndThatConnectionStillRuns() throws Exception {
        try (TestServer server = TestServer.start();
             ConnectionPool pool = new ConnectionPool(settings(server), 1, 400)) {
            Connection first = pool.borrow();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread second = new Thread(() -> {
                try {
                    pool.borrow();
                    failure.set(new AssertionError("expected BorrowException"));
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            });
            second.setDaemon(true);
            long start = System.nanoTime();
            second.start();
            second.join(3_000);
            Assert.assertFalse(second.isAlive());
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assert.assertTrue(failure.get() instanceof BorrowException);
            Assert.assertFalse(failure.get() instanceof CommandTimeoutException);
            Assert.assertFalse(failure.get() instanceof ConnectionException);
            Assert.assertTrue("elapsed " + elapsedMillis, elapsedMillis >= 300);
            Assert.assertTrue("elapsed " + elapsedMillis, elapsedMillis < 2_000);
            Assert.assertEquals("PONG", Await.join(first.ping()));
            Assert.assertEquals("OK", Await.join(first.set("k", "still-held")));
            Assert.assertEquals("still-held", Await.join(first.get("k")));
            pool.returnConnection(first);
        }
    }

    @Test
    public void zeroBorrowWaitFailsImmediatelyWhenNothingIsIdle() throws Exception {
        try (TestServer server = TestServer.start();
             ConnectionPool pool = new ConnectionPool(settings(server), 1, 0)) {
            Connection first = pool.borrow();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread second = new Thread(() -> {
                try {
                    pool.borrow();
                    failure.set(new AssertionError("expected BorrowException"));
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            });
            second.setDaemon(true);
            long start = System.nanoTime();
            second.start();
            second.join(1_000);
            Assert.assertFalse(second.isAlive());
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assert.assertTrue(failure.get() instanceof BorrowException);
            Assert.assertTrue("elapsed " + elapsedMillis, elapsedMillis < 500);
            Assert.assertEquals("OK", Await.join(first.set("k", "held")));
            Assert.assertEquals("held", Await.join(first.get("k")));
            pool.returnConnection(first);
        }
    }

    @Test
    public void waitersReceiveConnectionsInArrivalOrderAndRepliesDoNotCross() throws Exception {
        try (TestServer server = TestServer.start();
             ConnectionPool pool = new ConnectionPool(settings(server), 1, 10_000)) {
            Connection held = pool.borrow();
            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch secondStarted = new CountDownLatch(1);
            CountDownLatch firstMayUse = new CountDownLatch(1);
            AtomicReference<Connection> firstConnection = new AtomicReference<>();
            AtomicReference<Connection> secondConnection = new AtomicReference<>();
            AtomicReference<String> firstValue = new AtomicReference<>();
            AtomicReference<String> secondValue = new AtomicReference<>();
            AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            AtomicReference<Throwable> secondFailure = new AtomicReference<>();

            Thread first = new Thread(() -> {
                firstStarted.countDown();
                try {
                    Connection connection = pool.borrow();
                    firstConnection.set(connection);
                    Assert.assertTrue(firstMayUse.await(5, TimeUnit.SECONDS));
                    Assert.assertEquals("OK", Await.join(connection.set("owner", "first")));
                    firstValue.set(Await.join(connection.get("owner")));
                    pool.returnConnection(connection);
                } catch (Throwable thrown) {
                    firstFailure.set(thrown);
                }
            });
            Thread second = new Thread(() -> {
                secondStarted.countDown();
                try {
                    Connection connection = pool.borrow();
                    secondConnection.set(connection);
                    Assert.assertEquals("OK", Await.join(connection.set("owner", "second")));
                    secondValue.set(Await.join(connection.get("owner")));
                    pool.returnConnection(connection);
                } catch (Throwable thrown) {
                    secondFailure.set(thrown);
                }
            });
            first.setDaemon(true);
            second.setDaemon(true);

            first.start();
            Assert.assertTrue(firstStarted.await(2, TimeUnit.SECONDS));
            // 等前一个线程已经堵在借出等待里，再启动下一个，到达顺序才是启动顺序。
            awaitBlocked(first);
            second.start();
            Assert.assertTrue(secondStarted.await(2, TimeUnit.SECONDS));
            awaitBlocked(second);

            pool.returnConnection(held);
            Assert.assertTrue(awaitReference(firstConnection));
            Assert.assertSame(held, firstConnection.get());
            Assert.assertNull(secondConnection.get());
            firstMayUse.countDown();

            first.join(5_000);
            second.join(5_000);
            Assert.assertFalse(first.isAlive());
            Assert.assertFalse(second.isAlive());
            rethrow(firstFailure.get());
            rethrow(secondFailure.get());
            Assert.assertEquals("first", firstValue.get());
            Assert.assertEquals("second", secondValue.get());
            Assert.assertSame(held, secondConnection.get());
        }
    }

    @Test
    public void returnedConnectionStaysOnThePoolDatabase() throws Exception {
        try (TestServer server = TestServer.start()) {
            ConnectionSettings pooled = ConnectionSettings.defaults()
                    .withPort(server.port())
                    .withDatabase(1);
            try (ConnectionPool pool = new ConnectionPool(pooled, 1, 1_000)) {
                Connection first = pool.borrow();
                Assert.assertEquals(1, first.database());
                Assert.assertEquals("OK", Await.join(first.set("k", "on-pool-db")));
                pool.returnConnection(first);

                Connection second = pool.borrow();
                Assert.assertSame(first, second);
                Assert.assertEquals(1, second.database());
                Assert.assertEquals("on-pool-db", Await.join(second.get("k")));
                pool.returnConnection(second);
            }
            try (Connection other = Connection.connect(ConnectionSettings.defaults().withPort(server.port()))) {
                Assert.assertEquals(0, other.database());
                Assert.assertNull(Await.join(other.get("k")));
            }
        }
    }

    @Test
    public void selectAwayFromThePoolDatabaseIsNotVisibleToTheNextBorrow() throws Exception {
        try (TestServer server = TestServer.start();
             ConnectionPool pool = new ConnectionPool(settings(server), 1, 1_000)) {
            Connection borrowed = pool.borrow();
            Assert.assertEquals("OK", Await.join(borrowed.select(1)));
            Assert.assertEquals(1, borrowed.database());
            Assert.assertEquals("OK", Await.join(borrowed.set("k", "on-one")));
            pool.returnConnection(borrowed);
            assertClosed(borrowed);

            Connection next = pool.borrow();
            Assert.assertNotSame(borrowed, next);
            Assert.assertEquals(0, next.database());
            Assert.assertNull(Await.join(next.get("k")));
            pool.returnConnection(next);

            try (Connection db1 = Connection.connect(ConnectionSettings.defaults()
                    .withPort(server.port())
                    .withDatabase(1))) {
                Assert.assertEquals("on-one", Await.join(db1.get("k")));
            }
        }
    }

    @Test
    public void unfinishedTransactionIsNotCommittedWhenReturned() throws Exception {
        try (TestServer server = TestServer.start();
             ConnectionPool pool = new ConnectionPool(settings(server), 1, 1_000)) {
            Connection borrowed = pool.borrow();
            Transaction transaction = borrowed.multi();
            transaction.set("k", "queued");
            pool.returnConnection(borrowed);
            assertClosed(borrowed);

            Connection next = pool.borrow();
            Assert.assertNotSame(borrowed, next);
            Assert.assertNull(Await.join(next.get("k")));
            Assert.assertEquals("PONG", Await.join(next.ping()));
            pool.returnConnection(next);
        }
    }

    @Test
    public void unpairedCommandIsNotHandedOutAgain() throws Exception {
        try (TestServer server = TestServer.start();
             ConnectionPool pool = new ConnectionPool(settings(server), 1, 1_000)) {
            Connection borrowed = pool.borrow();
            CompletableFuture<String> pending = borrowed.set("k", "v");
            pool.returnConnection(borrowed);
            try {
                Await.join(pending);
                Assert.fail("expected ConnectionException");
            } catch (ConnectionException expected) {
            }
            assertClosed(borrowed);

            Connection next = pool.borrow();
            Assert.assertNotSame(borrowed, next);
            Assert.assertEquals("PONG", Await.join(next.ping()));
            Assert.assertEquals(Long.valueOf(1), Await.join(next.incr("n")));
            pool.returnConnection(next);
        }
    }

    @Test
    public void connectionRemainsBorrowableAfterAServerError() throws Exception {
        try (TestServer server = TestServer.start();
             ConnectionPool pool = new ConnectionPool(settings(server), 1, 1_000)) {
            Connection borrowed = pool.borrow();
            try {
                Await.join(borrowed.command("NO_SUCH"));
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR unknown command 'NO_SUCH'", e.getMessage());
            }
            pool.returnConnection(borrowed);

            Connection again = pool.borrow();
            Assert.assertSame(borrowed, again);
            Assert.assertEquals("PONG", Await.join(again.ping()));
            Assert.assertEquals("OK", Await.join(again.set("k", "after-error")));
            Assert.assertEquals("after-error", Await.join(again.get("k")));
            pool.returnConnection(again);
        }
    }

    @Test
    public void closeRefusesNewBorrowsAndClosesAConnectionWhenItIsReturned() throws Exception {
        try (TestServer server = TestServer.start();
             ConnectionPool pool = new ConnectionPool(settings(server), 1, 1_000)) {
            Connection borrowed = pool.borrow();
            pool.close();
            try {
                pool.borrow();
                Assert.fail("expected BorrowException");
            } catch (BorrowException expected) {
            }
            Assert.assertEquals("OK", Await.join(borrowed.set("k", "in-flight")));
            Assert.assertEquals("in-flight", Await.join(borrowed.get("k")));
            pool.returnConnection(borrowed);
            assertClosed(borrowed);
            try {
                pool.borrow();
                Assert.fail("expected BorrowException");
            } catch (BorrowException expected) {
            }
        }
    }

    @Test
    public void failedSetupSelectDoesNotStickASlot() throws Exception {
        try (TestServer server = TestServer.start()) {
            ConnectionSettings settings = ConnectionSettings.defaults()
                    .withPort(server.port())
                    .withDatabase(16);
            try (ConnectionPool pool = new ConnectionPool(settings, 1, 1_000)) {
                for (int attempt = 0; attempt < 2; attempt++) {
                    long start = System.nanoTime();
                    try {
                        pool.borrow();
                        Assert.fail("expected ServerException");
                    } catch (ServerException e) {
                        Assert.assertEquals("ERR DB index is out of range", e.getMessage());
                    }
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                    Assert.assertTrue("elapsed " + elapsedMillis, elapsedMillis < 800);
                }
            }
        }
    }

    private static ConnectionSettings settings(TestServer server) {
        return ConnectionSettings.defaults().withPort(server.port());
    }

    private static void assertClosed(Connection connection) {
        try {
            Await.join(connection.ping());
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }
    }

    private static void awaitBlocked(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.sleep(5);
        }
        Assert.fail("thread did not block in borrow: " + thread.getState());
    }

    private static boolean awaitReference(AtomicReference<?> reference) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (reference.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        return reference.get() != null;
    }

    private static void rethrow(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw new AssertionError(failure);
    }
}
