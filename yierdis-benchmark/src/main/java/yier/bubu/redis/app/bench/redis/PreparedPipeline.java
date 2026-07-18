package yier.bubu.redis.app.bench.redis;

import java.util.Objects;
import java.util.OptionalLong;

final class PreparedPipeline {
    private final byte[] bytes;
    private final int[] randomOffsets;
    private final OptionalLong keyspace;

    PreparedPipeline(byte[] bytes, int[] randomOffsets, OptionalLong keyspace) {
        this.bytes = Objects.requireNonNull(bytes, "bytes");
        this.randomOffsets = Objects.requireNonNull(randomOffsets, "randomOffsets");
        this.keyspace = Objects.requireNonNull(keyspace, "keyspace");
        if (randomOffsets.length > 0 && keyspace.isEmpty()) {
            throw new IllegalArgumentException("random offsets require a keyspace");
        }
        for (int offset : randomOffsets) {
            if (offset < 0 || offset > bytes.length - BenchmarkRandom.TWELVE_DIGITS) {
                throw new IndexOutOfBoundsException("random offset must leave room for 12 digits");
            }
        }
    }

    byte[] bytesForWrite(BenchmarkRandom random) {
        Objects.requireNonNull(random, "random");
        if (randomOffsets.length == 0) {
            return bytes;
        }

        long bound = keyspace.orElseThrow();
        for (int offset : randomOffsets) {
            long value = bound == 0 ? 0 : random.nextLong(bound);
            BenchmarkRandom.writeTwelveDigits(bytes, offset, value);
        }
        return bytes;
    }

    PreparedPipeline copyForClient() {
        return new PreparedPipeline(bytes.clone(), randomOffsets.clone(), keyspace);
    }
}
