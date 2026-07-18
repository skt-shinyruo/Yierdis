package yier.bubu.redis.app.bench.redis;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public final class RedisBenchmarkCommandTemplate {
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

    private static byte[] asciiBytes(String value, String name) {
        String required = Objects.requireNonNull(value, name);
        byte[] bytes = required.getBytes(StandardCharsets.US_ASCII);
        if (!required.equals(new String(bytes, StandardCharsets.US_ASCII))) {
            throw new IllegalArgumentException(name + " must contain only ASCII characters");
        }
        return bytes;
    }
}
