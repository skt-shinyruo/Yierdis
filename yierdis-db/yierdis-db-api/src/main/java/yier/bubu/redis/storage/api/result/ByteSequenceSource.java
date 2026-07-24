package yier.bubu.redis.storage.api.result;

public interface ByteSequenceSource extends AutoCloseable {
    int elementCount();

    long retainedMemoryBytes();

    void visitElementLengths(PayloadLengthSink out);

    void emitTo(ByteValueSink out);

    @Override
    void close();
}
