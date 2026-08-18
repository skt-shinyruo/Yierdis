package yier.bubu.redis.storage.api.result;

import yier.bubu.redis.bytes.BytesSlice;

public interface ByteValueSink {
    void value(byte[] data);

    void value(byte[] data, int offset, int length);

    void value(BytesSlice slice);

    void longAscii(long value);

    void nullValue();
}
