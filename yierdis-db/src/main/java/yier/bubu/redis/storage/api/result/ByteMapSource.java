package yier.bubu.redis.storage.api.result;

import java.util.function.IntConsumer;

public interface ByteMapSource extends AutoCloseable {
    int pairCount();

    long retainedMemoryBytes();

    void visitPairLengths(IntConsumer out);

    void emitPairsTo(ByteValueSink out);

    @Override
    void close();
}
