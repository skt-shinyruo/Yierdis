package yier.bubu.redis.storage.api.result;

@FunctionalInterface
public interface PayloadLengthSink {
    void payloadLength(int length);
}
