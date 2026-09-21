package yier.bubu.redis.storage.api.result;

import java.util.function.IntConsumer;

import java.util.Objects;
import java.util.function.Consumer;

public final class ByteMapSources {
    private ByteMapSources() {
    }

    public static ByteMapSource empty() {
        return of(0, 0L, ignored -> { }, ignored -> { });
    }

    /**
     * 把 live field/value emit 拷成独立 source。条目数必须成对，后续 emit/length 只回放这份快照。
     */
    public static ByteMapSource copiedFrom(Consumer<ByteValueSink> emitter) {
        CapturedByteItems items = CapturedByteItems.capture(emitter);
        if ((items.size() & 1) != 0) {
            throw new IllegalStateException("captured map source emitted an odd number of values");
        }
        return of(items.size() / 2, items.retainedMemoryBytes(), items::visitLengths, items::emitTo);
    }

    public static ByteMapSource of(
            int pairCount,
            long retainedMemoryBytes,
            Consumer<IntConsumer> lengthVisitor,
            Consumer<ByteValueSink> emitter
    ) {
        return new Source(pairCount, retainedMemoryBytes, lengthVisitor, emitter);
    }

    private static final class Source extends ByteSourceSupport implements ByteMapSource {
        private final Consumer<IntConsumer> lengthVisitor;
        private final Consumer<ByteValueSink> emitter;

        private Source(
                int pairCount,
                long retainedMemoryBytes,
                Consumer<IntConsumer> lengthVisitor,
                Consumer<ByteValueSink> emitter
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
        public void visitPairLengths(IntConsumer out) {
            visitLengths(out, 2, lengthVisitor);
        }

        @Override
        public void emitPairsTo(ByteValueSink out) {
            emitter.accept(emitterSink(out));
        }
    }
}
