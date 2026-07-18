package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

public final class RedisBenchmarkCommandTemplate {
    private static final byte[] RANDOM_MARKER = asciiBytes("__rand_int__", "random marker");
    private static final byte[] FIXED_SCORE = new byte[]{'0'};

    public enum WireMode {
        INLINE,
        RESP
    }

    public enum ArgumentKind {
        LITERAL,
        PAYLOAD,
        RANDOM_SCORE
    }

    public record Argument(ArgumentKind kind, byte[] literal) {
        public Argument {
            kind = Objects.requireNonNull(kind, "kind");
            literal = Objects.requireNonNull(literal, "literal").clone();
        }

        @Override
        public byte[] literal() {
            return literal.clone();
        }

        public static Argument literal(String value) {
            return new Argument(ArgumentKind.LITERAL, asciiBytes(value, "literal"));
        }

        public static Argument payload() {
            return new Argument(ArgumentKind.PAYLOAD, new byte[0]);
        }

        public static Argument randomScore() {
            return new Argument(ArgumentKind.RANDOM_SCORE, new byte[0]);
        }
    }

    private final WireMode wireMode;
    private final byte[] inlineFrame;
    private final List<Argument> arguments;

    private RedisBenchmarkCommandTemplate(
            WireMode wireMode,
            byte[] inlineFrame,
            List<Argument> arguments
    ) {
        this.wireMode = Objects.requireNonNull(wireMode, "wireMode");
        this.inlineFrame = Objects.requireNonNull(inlineFrame, "inlineFrame").clone();
        this.arguments = List.copyOf(Objects.requireNonNull(arguments, "arguments"));
    }

    public static RedisBenchmarkCommandTemplate inline(String frame) {
        Objects.requireNonNull(frame, "frame");
        if (!frame.endsWith("\r\n")) {
            throw new IllegalArgumentException("inline frame must end with CRLF");
        }
        if (frame.substring(0, frame.length() - 2).isBlank()) {
            throw new IllegalArgumentException("inline frame requires a command");
        }
        return new RedisBenchmarkCommandTemplate(
                WireMode.INLINE,
                asciiBytes(frame, "frame"),
                List.of()
        );
    }

    public static RedisBenchmarkCommandTemplate resp(Argument... arguments) {
        Objects.requireNonNull(arguments, "arguments");
        if (arguments.length == 0) {
            throw new IllegalArgumentException("RESP template requires a command");
        }
        return new RedisBenchmarkCommandTemplate(
                WireMode.RESP,
                new byte[0],
                List.of(arguments.clone())
        );
    }

    public WireMode wireMode() {
        return wireMode;
    }

    public byte[] inlineFrame() {
        return inlineFrame.clone();
    }

    public List<Argument> arguments() {
        return arguments;
    }

    PreparedPipeline prepare(int pipeline, byte[] payload, OptionalLong keyspace) {
        if (pipeline <= 0) {
            throw new IllegalArgumentException("pipeline must be > 0");
        }
        Objects.requireNonNull(payload, "payload");
        OptionalLong requiredKeyspace = Objects.requireNonNull(keyspace, "keyspace");
        if (requiredKeyspace.isPresent()) {
            long value = requiredKeyspace.getAsLong();
            if (value < 0) {
                throw new IllegalArgumentException("keyspace must be >= 0");
            }
            if (value > BenchmarkRandom.TWELVE_DIGIT_LIMIT) {
                throw new IllegalArgumentException("keyspace values must fit in 12 digits");
            }
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        if (wireMode == WireMode.INLINE) {
            for (int copy = 0; copy < pipeline; copy++) {
                output.writeBytes(inlineFrame);
            }
        } else {
            List<byte[]> resolvedArguments = resolveArguments(payload, requiredKeyspace);
            for (int copy = 0; copy < pipeline; copy++) {
                output.writeBytes(RespClientCodec.encodeCommand(resolvedArguments));
            }
        }

        byte[] bytes = output.toByteArray();
        int[] randomOffsets = requiredKeyspace.isPresent()
                ? replaceMarkersWithZeroes(bytes)
                : new int[0];
        return new PreparedPipeline(bytes, randomOffsets, requiredKeyspace);
    }

    private List<byte[]> resolveArguments(byte[] payload, OptionalLong keyspace) {
        List<byte[]> resolved = new ArrayList<>(arguments.size());
        for (Argument argument : arguments) {
            resolved.add(switch (argument.kind()) {
                case LITERAL -> argument.literal();
                case PAYLOAD -> payload;
                case RANDOM_SCORE -> keyspace.isPresent() ? RANDOM_MARKER : FIXED_SCORE;
            });
        }
        return resolved;
    }

    private static int[] replaceMarkersWithZeroes(byte[] bytes) {
        int markerCount = 0;
        for (int offset = 0; offset <= bytes.length - RANDOM_MARKER.length; offset++) {
            if (markerAt(bytes, offset)) {
                markerCount++;
            }
        }

        int[] offsets = new int[markerCount];
        int markerIndex = 0;
        for (int offset = 0; offset <= bytes.length - RANDOM_MARKER.length; offset++) {
            if (!markerAt(bytes, offset)) {
                continue;
            }
            offsets[markerIndex++] = offset;
            for (int digit = 0; digit < RANDOM_MARKER.length; digit++) {
                bytes[offset + digit] = '0';
            }
        }
        return offsets;
    }

    private static boolean markerAt(byte[] bytes, int offset) {
        for (int markerIndex = 0; markerIndex < RANDOM_MARKER.length; markerIndex++) {
            if (bytes[offset + markerIndex] != RANDOM_MARKER[markerIndex]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] asciiBytes(String value, String name) {
        String required = Objects.requireNonNull(value, name);
        byte[] bytes = required.getBytes(StandardCharsets.US_ASCII);
        if (!required.equals(new String(bytes, StandardCharsets.US_ASCII))) {
            throw new IllegalArgumentException(name + " must contain only ASCII characters");
        }
        return bytes;
    }
}
