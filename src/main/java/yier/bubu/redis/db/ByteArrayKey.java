package yier.bubu.redis.db;

import java.util.Arrays;

final class ByteArrayKey implements Comparable<ByteArrayKey> {
    private final byte[] bytes;
    private final int hash;

    ByteArrayKey(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes must not be null");
        }
        this.bytes = bytes;
        this.hash = Arrays.hashCode(bytes);
    }

    byte[] bytes() {
        return bytes;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ByteArrayKey)) {
            return false;
        }
        ByteArrayKey other = (ByteArrayKey) o;
        return Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int compareTo(ByteArrayKey other) {
        return compareLex(bytes, other.bytes);
    }

    static int compareLex(byte[] a, byte[] b) {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int av = a[i] & 0xFF;
            int bv = b[i] & 0xFF;
            if (av != bv) {
                return Integer.compare(av, bv);
            }
        }
        return Integer.compare(a.length, b.length);
    }
}

