package yier.bubu.redis.storage.memory.internal.hash;

import java.security.SecureRandom;

public record HashSeed(long key0, long key1) {
    private static final SecureRandom RANDOM = new SecureRandom();

    public static HashSeed random() {
        return new HashSeed(RANDOM.nextLong(), RANDOM.nextLong());
    }
}
