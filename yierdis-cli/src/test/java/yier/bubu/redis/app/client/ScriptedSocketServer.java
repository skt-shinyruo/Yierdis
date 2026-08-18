package yier.bubu.redis.app.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicReference;

final class ScriptedSocketServer implements AutoCloseable {
    @FunctionalInterface
    interface Script {
        void run(Socket socket) throws Exception;
    }

    private final ServerSocket server;
    private final AtomicReference<Throwable> failure;
    private final Thread worker;

    private ScriptedSocketServer(ServerSocket server, AtomicReference<Throwable> failure, Thread worker) {
        this.server = server;
        this.failure = failure;
        this.worker = worker;
    }

    static ScriptedSocketServer start(Script script) throws IOException {
        ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = Thread.ofPlatform().start(() -> {
            try (Socket socket = server.accept()) {
                script.run(socket);
            } catch (Throwable t) {
                if (!server.isClosed()) {
                    failure.set(t);
                }
            }
        });
        return new ScriptedSocketServer(server, failure, worker);
    }

    int port() {
        return server.getLocalPort();
    }

    void assertSucceeded() throws InterruptedException {
        worker.join(3_000L);
        if (worker.isAlive()) {
            throw new AssertionError("scripted socket server did not finish");
        }
        if (failure.get() != null) {
            throw new AssertionError("scripted socket server failed", failure.get());
        }
    }

    @Override
    public void close() throws InterruptedException, IOException {
        server.close();
        worker.join(3_000L);
    }
}
