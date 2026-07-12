package yier.bubu.redis.storage.api.result;

public interface PoppedValueSequence extends BulkStringSequence, AutoCloseable {
    boolean isNull();

    long encodedElementBytes();

    long retainedMemoryBytes();

    @Override
    void close();
}
