package yier.bubu.redis.app.client;

import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Blocking RESP client for Yierdis.
 * <p>
 * The client intentionally keeps a one-at-a-time request/response model. A timeout or parse failure closes the
 * connection because Redis-style replies are FIFO and cannot be safely resynchronized.
 */
public final class YierdisClient implements AutoCloseable {
    private static final int CONNECT_TIMEOUT_MILLIS = 5000;

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final Object requestLock = new Object();
    private volatile boolean closed;

    private YierdisClient(Socket socket) throws IOException {
        this.socket = Objects.requireNonNull(socket, "socket");
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
    }

    public static YierdisClient connect(String host, int port) throws IOException {
        Objects.requireNonNull(host, "host");
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
            return new YierdisClient(socket);
        } catch (Throwable t) {
            try {
                socket.close();
            } catch (Throwable closeFailure) {
                t.addSuppressed(closeFailure);
            }
            throw t;
        }
    }

    public RespClientCodec.RespReply execute(List<byte[]> args, long timeoutMillis) throws InterruptedException {
        Objects.requireNonNull(args, "args");
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be > 0");
        }

        synchronized (requestLock) {
            if (closed) {
                throw new IllegalStateException("Client is closed");
            }
            if (socket.isClosed() || !socket.isConnected()) {
                closed = true;
                throw new IllegalStateException("Connection is closed");
            }

            try {
                socket.setSoTimeout(toSocketTimeoutMillis(timeoutMillis));
                RespClientCodec.writeCommand(out, args);
                out.flush();
                return RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            } catch (SocketTimeoutException e) {
                closeSilently();
                throw new IllegalStateException("Timeout waiting for response (connection closed to prevent response desync)", e);
            } catch (IOException | RuntimeException e) {
                closeSilently();
                String message = String.valueOf(e.getMessage()).toLowerCase();
                if (message.contains("eof") || message.contains("closed")) {
                    throw new IllegalStateException("Connection closed", e);
                }
                throw new IllegalStateException("Invalid RESP reply (connection closed to prevent desync)", e);
            }
        }
    }

    public RespClientCodec.RespReply executeUtf8(List<String> args, long timeoutMillis) throws InterruptedException {
        Objects.requireNonNull(args, "args");
        List<byte[]> out = new ArrayList<>(args.size());
        for (String a : args) {
            out.add(a == null ? null : a.getBytes(StandardCharsets.UTF_8));
        }
        return execute(out, timeoutMillis);
    }

    @Override
    public void close() {
        closeSilently();
    }

    private void closeSilently() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            socket.close();
        } catch (Throwable ignored) {
            // ignore
        }
    }

    private static int toSocketTimeoutMillis(long timeoutMillis) {
        return timeoutMillis > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) timeoutMillis;
    }

}
