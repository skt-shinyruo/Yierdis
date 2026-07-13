package yier.bubu.redis.storage.memory.internal.hash;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeObjectView;

public final class SipHash24 {
    private SipHash24() {
    }

    public static long hash(HashSeed seed, byte[] value) {
        Objects.requireNonNull(value, "value");
        return hash(seed, new ByteSource() {
            @Override
            public int length() {
                return value.length;
            }

            @Override
            public byte byteAt(int index) {
                return value[index];
            }
        });
    }

    public static long hash(HashSeed seed, BytesView value) {
        Objects.requireNonNull(value, "value");
        return hash(seed, new ByteSource() {
            @Override
            public int length() {
                return value.length();
            }

            @Override
            public byte byteAt(int index) {
                return value.getByte(index);
            }
        });
    }

    public static long hash(HashSeed seed, NativeObjectView value) {
        Objects.requireNonNull(value, "value");
        return hash(seed, new ByteSource() {
            @Override
            public int length() {
                return value.size();
            }

            @Override
            public byte byteAt(int index) {
                return value.getByte(index);
            }
        });
    }

    public static int foldToInt(long hash) {
        return (int) (hash ^ (hash >>> 32));
    }

    private static long hash(HashSeed seed, ByteSource source) {
        Objects.requireNonNull(seed, "seed");
        int length = source.length();
        if (length < 0) {
            throw new IllegalArgumentException("value length < 0: " + length);
        }

        long v0 = 0x736f6d6570736575L ^ seed.key0();
        long v1 = 0x646f72616e646f6dL ^ seed.key1();
        long v2 = 0x6c7967656e657261L ^ seed.key0();
        long v3 = 0x7465646279746573L ^ seed.key1();

        int offset = 0;
        int fullLength = length & ~7;
        while (offset < fullLength) {
            long message = readLittleEndian(source, offset, Long.BYTES);
            v3 ^= message;
            for (int round = 0; round < 2; round++) {
                v0 += v1;
                v1 = Long.rotateLeft(v1, 13);
                v1 ^= v0;
                v0 = Long.rotateLeft(v0, 32);
                v2 += v3;
                v3 = Long.rotateLeft(v3, 16);
                v3 ^= v2;
                v0 += v3;
                v3 = Long.rotateLeft(v3, 21);
                v3 ^= v0;
                v2 += v1;
                v1 = Long.rotateLeft(v1, 17);
                v1 ^= v2;
                v2 = Long.rotateLeft(v2, 32);
            }
            v0 ^= message;
            offset += Long.BYTES;
        }

        long last = ((long) length) << 56;
        last |= readLittleEndian(source, offset, length - offset);
        v3 ^= last;
        for (int round = 0; round < 2; round++) {
            v0 += v1;
            v1 = Long.rotateLeft(v1, 13);
            v1 ^= v0;
            v0 = Long.rotateLeft(v0, 32);
            v2 += v3;
            v3 = Long.rotateLeft(v3, 16);
            v3 ^= v2;
            v0 += v3;
            v3 = Long.rotateLeft(v3, 21);
            v3 ^= v0;
            v2 += v1;
            v1 = Long.rotateLeft(v1, 17);
            v1 ^= v2;
            v2 = Long.rotateLeft(v2, 32);
        }
        v0 ^= last;
        v2 ^= 0xffL;
        for (int round = 0; round < 4; round++) {
            v0 += v1;
            v1 = Long.rotateLeft(v1, 13);
            v1 ^= v0;
            v0 = Long.rotateLeft(v0, 32);
            v2 += v3;
            v3 = Long.rotateLeft(v3, 16);
            v3 ^= v2;
            v0 += v3;
            v3 = Long.rotateLeft(v3, 21);
            v3 ^= v0;
            v2 += v1;
            v1 = Long.rotateLeft(v1, 17);
            v1 ^= v2;
            v2 = Long.rotateLeft(v2, 32);
        }
        return v0 ^ v1 ^ v2 ^ v3;
    }

    private static long readLittleEndian(ByteSource source, int offset, int length) {
        long value = 0L;
        for (int i = 0; i < length; i++) {
            value |= ((long) source.byteAt(offset + i) & 0xffL) << (i * Byte.SIZE);
        }
        return value;
    }

    private interface ByteSource {
        int length();

        byte byteAt(int index);
    }
}
