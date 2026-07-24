package yier.bubu.redis.storage.api.result;

public interface ByteMapSource extends AutoCloseable {
    int pairCount();

    long retainedMemoryBytes();

    void visitPairLengths(PayloadLengthSink out);

    void emitPairsTo(ByteValueSink out);

    @Override
    void close();
}
