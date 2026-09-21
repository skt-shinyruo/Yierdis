package yier.bubu.redis.storage.api.result;

import java.util.function.IntConsumer;

import java.util.Objects;
import java.util.function.Consumer;

public final class ByteSequenceSources {
    private ByteSequenceSources() {
    }

    public static ByteSequenceSource empty() {
        return of(0, 0L, ignored -> { }, ignored -> { });
    }

    /**
     * 把 live emitter 的一次遍历拷成独立 source。后续 emit/length 只回放这份快照。
     */
    public static ByteSequenceSource copiedFrom(Consumer<ByteValueSink> emitter) {
        CapturedByteItems items = CapturedByteItems.capture(emitter);
        return of(items.size(), items.retainedMemoryBytes(), items::visitLengths, items::emitTo);
    }

    public static ByteSequenceSource of(
            int elementCount,
            long retainedMemoryBytes,
            Consumer<IntConsumer> lengthVisitor,
            Consumer<ByteValueSink> emitter
    ) {
        return new Source(elementCount, retainedMemoryBytes, lengthVisitor, emitter);
    }

    private static final class Source extends ByteSourceSupport implements ByteSequenceSource {
        private final Consumer<IntConsumer> lengthVisitor;
        private final Consumer<ByteValueSink> emitter;

        private Source(
                int elementCount,
                long retainedMemoryBytes,
                Consumer<IntConsumer> lengthVisitor,
                Consumer<ByteValueSink> emitter
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
        public void visitElementLengths(IntConsumer out) {
            visitLengths(out, 1, lengthVisitor);
        }

        @Override
        public void emitTo(ByteValueSink out) {
            emitter.accept(emitterSink(out));
        }
    }
}
