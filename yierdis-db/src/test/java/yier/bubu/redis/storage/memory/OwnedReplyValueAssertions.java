package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.StringOps;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.testkit.MaterializingByteValueSink;

import static yier.bubu.redis.storage.testkit.TestBytes.view;

public final class OwnedReplyValueAssertions {
    private OwnedReplyValueAssertions() {
    }

    static boolean isNull(ByteValue value) {
        try (value) {
            return value.payloadLength() < 0;
        }
    }

    public static byte[] stringValue(StringOps operations, byte[] key) {
        return bytes(operations.getStringValue(view(key)));
    }

    static byte[] bytes(ByteValue value) {
        try (value) {
            byte[][] captured = new byte[1][];
            value.emitTo(new MaterializingByteValueSink() {
                @Override
                public void value(byte[] data) {
                    captured[0] = data == null ? null : data.clone();
                }

            });
            return captured[0];
        }
    }

}
