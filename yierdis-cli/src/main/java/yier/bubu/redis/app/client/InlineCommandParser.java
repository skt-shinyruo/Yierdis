package yier.bubu.redis.app.client;

import java.util.List;

/**
 * CLI compatibility wrapper for the shared inline command parser.
 */
public final class InlineCommandParser {
    private InlineCommandParser() {
    }

    public static Decoded parse(byte[] input, int off, int len, int maxArgs) {
        if (maxArgs <= 0) {
            throw new IllegalArgumentException("maxArgs must be > 0");
        }
        return new Decoded(yier.bubu.redis.protocol.resp.InlineCommandParser.parse(input, off, len, maxArgs));
    }

    public static List<byte[]> splitUtf8(String line, int maxArgs) {
        if (maxArgs <= 0) {
            throw new IllegalArgumentException("maxArgs must be > 0");
        }
        return yier.bubu.redis.protocol.resp.InlineCommandParser.splitUtf8(line, maxArgs);
    }

    public static final class Decoded {
        private final yier.bubu.redis.protocol.resp.InlineCommandParser.Decoded delegate;

        private Decoded(yier.bubu.redis.protocol.resp.InlineCommandParser.Decoded delegate) {
            this.delegate = delegate;
        }

        public byte[] decoded() {
            return delegate.decoded();
        }

        public int decodedLen() {
            return delegate.decodedLen();
        }

        public int argc() {
            return delegate.argc();
        }

        public int offset(int arg) {
            return delegate.offset(arg);
        }

        public int length(int arg) {
            return delegate.length(arg);
        }
    }
}
