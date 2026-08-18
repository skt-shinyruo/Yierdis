package yier.bubu.redis.storage.testkit;

import java.nio.charset.StandardCharsets;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.result.ByteValueSink;

public abstract class MaterializingByteValueSink implements ByteValueSink {
    @Override
    public void value(byte[] data, int offset, int length) {
        if (data == null) {
            value((byte[]) null);
            return;
        }
        byte[] copy = new byte[length];
        System.arraycopy(data, offset, copy, 0, length);
        value(copy);
    }

    @Override
    public void value(BytesSlice slice) {
        if (slice == null) {
            value((byte[]) null);
            return;
        }
        byte[] copy = new byte[slice.length()];
        slice.getBytes(0, copy, 0, copy.length);
        value(copy);
    }

    @Override
    public void longAscii(long value) {
        value(Long.toString(value).getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public void nullValue() {
        value((byte[]) null);
    }
}
