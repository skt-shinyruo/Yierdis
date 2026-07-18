package yier.bubu.redis.app.bench.redis;

@FunctionalInterface
public interface BenchmarkClock {
    long nanoTime();

    static BenchmarkClock system() {
        return System::nanoTime;
    }
}
