package yier.bubu.redis.storage.api.result;

import java.util.function.IntConsumer;

public interface ByteSequenceSource extends AutoCloseable {
    int elementCount();

    long retainedMemoryBytes();

    void visitElementLengths(IntConsumer out);

    void emitTo(ByteValueSink out);

    @Override
    void close();
}
