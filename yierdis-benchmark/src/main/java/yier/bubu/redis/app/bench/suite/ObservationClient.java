package yier.bubu.redis.app.bench.suite;

import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ObservationClient {
    private static final int CONNECT_TIMEOUT_MILLIS = 1000;
    private static final int READ_TIMEOUT_MILLIS = 1000;

    public ObservationSnapshot capture(String host, int port) {
        validateEndpoint(host, port);

        Map<String, String> values = new LinkedHashMap<>();
        values.put("STATS", captureCommand(host, port, "STATS"));
        values.put("MEMORY STATS", captureCommand(host, port, "MEMORY", "STATS"));
        values.put("INFO", captureCommand(host, port, "INFO"));
        return new ObservationSnapshot(values);
    }

    static String formatReply(RespClientCodec.RespReply reply) {
        if (reply == null) {
            return "";
        }
        return switch (reply.kind()) {
            case SIMPLE_STRING, ERROR -> reply.text() == null ? "" : reply.text();
            case INTEGER -> reply.integer() == null ? "" : Long.toString(reply.integer());
            case BULK_STRING -> reply.bytes() == null ? "" : new String(reply.bytes(), StandardCharsets.UTF_8);
            case NULL -> "null";
            case ARRAY -> formatArray(reply.values());
        };
    }

    private static String captureCommand(String host, int port, String... args) {
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(READ_TIMEOUT_MILLIS);
            RespClientCodec.writeCommand(socket.getOutputStream(), utf8Args(args));
            RespClientCodec.RespReply reply = RespClientCodec.readReply(socket.getInputStream(),
                    RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            return formatReply(reply);
        } catch (IOException | RuntimeException e) {
            return formatError(e);
        }
    }

    private static String formatArray(List<RespClientCodec.RespReply> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }

        StringBuilder out = new StringBuilder();
        for (int i = 0; i < values.size(); i += 2) {
            if (out.length() > 0) {
                out.append("; ");
            }
            out.append(formatReply(values.get(i))).append('=');
            RespClientCodec.RespReply value = i + 1 < values.size() ? values.get(i + 1) : null;
            out.append(formatReply(value));
        }
        return out.toString();
    }

    private static List<byte[]> utf8Args(String... args) {
        byte[][] encoded = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            encoded[i] = args[i].getBytes(StandardCharsets.UTF_8);
        }
        return List.of(encoded);
    }

    private static void validateEndpoint(String host, int port) {
        Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be in 1..65535");
        }
    }

    private static String formatError(Exception e) {
        String message = e.getMessage();
        if (message == null || message.isBlank()) {
            return "ERROR " + e.getClass().getSimpleName();
        }
        return "ERROR " + e.getClass().getSimpleName() + ": " + message;
    }
}
