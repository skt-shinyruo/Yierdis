package yier.bubu.redis.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

final class Await {
    private Await() {
    }

    static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException failure) {
            Throwable cause = Connection.unwrap(failure);
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw failure;
        }
    }
}
