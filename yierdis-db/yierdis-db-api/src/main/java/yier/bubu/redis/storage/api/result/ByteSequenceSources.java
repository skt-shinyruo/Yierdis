package yier.bubu.redis.storage.api.result;

import java.util.Objects;

public final class ByteSequenceSources {
    @FunctionalInterface
    public interface LengthVisitor {
        void visit(PayloadLengthSink out);
    }

    @FunctionalInterface
    public interface Emitter {
        void emit(ByteValueSink out);
    }

    private ByteSequenceSources() {
    }

    public static ByteSequenceSource empty() {
        return of(0, 0L, ignored -> { }, ignored -> { });
    }

    public static ByteSequenceSource of(
            int elementCount,
            long retainedMemoryBytes,
            LengthVisitor lengthVisitor,
            Emitter emitter
    ) {
        return new Source(elementCount, retainedMemoryBytes, lengthVisitor, emitter);
    }

    private static final class Source extends ByteSourceSupport implements ByteSequenceSource {
        private final LengthVisitor lengthVisitor;
        private final Emitter emitter;

        private Source(
                int elementCount,
                long retainedMemoryBytes,
                LengthVisitor lengthVisitor,
                Emitter emitter
        ) {
            super(elementCount, "elementCount", retainedMemoryBytes, "byte sequence source");
            this.lengthVisitor = Objects.requireNonNull(lengthVisitor, "lengthVisitor");
            this.emitter = Objects.requireNonNull(emitter, "emitter");
        }

        @Override
        public int elementCount() {
            return count();
        }

        @Override
        public void visitElementLengths(PayloadLengthSink out) {
            visitLengths(out, 1, lengthVisitor::visit);
        }

        @Override
        public void emitTo(ByteValueSink out) {
            emitter.emit(emitterSink(out));
        }
    }
}
