package yier.bubu.redis.storage.api.result;

import java.util.Objects;

public final class ByteMapSources {
    @FunctionalInterface
    public interface LengthVisitor {
        void visit(PayloadLengthSink out);
    }

    @FunctionalInterface
    public interface Emitter {
        void emit(ByteValueSink out);
    }

    private ByteMapSources() {
    }

    public static ByteMapSource empty() {
        return of(0, 0L, ignored -> { }, ignored -> { });
    }

    public static ByteMapSource of(
            int pairCount,
            long retainedMemoryBytes,
            LengthVisitor lengthVisitor,
            Emitter emitter
    ) {
        return new Source(pairCount, retainedMemoryBytes, lengthVisitor, emitter);
    }

    private static final class Source extends ByteSourceSupport implements ByteMapSource {
        private final LengthVisitor lengthVisitor;
        private final Emitter emitter;

        private Source(
                int pairCount,
                long retainedMemoryBytes,
                LengthVisitor lengthVisitor,
                Emitter emitter
        ) {
            super(pairCount, "pairCount", retainedMemoryBytes, "byte map source");
            this.lengthVisitor = Objects.requireNonNull(lengthVisitor, "lengthVisitor");
            this.emitter = Objects.requireNonNull(emitter, "emitter");
        }

        @Override
        public int pairCount() {
            return count();
        }

        @Override
        public void visitPairLengths(PayloadLengthSink out) {
            visitLengths(out, 2, lengthVisitor::visit);
        }

        @Override
        public void emitPairsTo(ByteValueSink out) {
            emitter.emit(emitterSink(out));
        }
    }
}
