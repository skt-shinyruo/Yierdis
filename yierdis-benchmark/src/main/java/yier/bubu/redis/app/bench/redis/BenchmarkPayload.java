package yier.bubu.redis.app.bench.redis;

final class BenchmarkPayload {
    private BenchmarkPayload() {
    }

    static byte[] generate(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be >= 0");
        }

        byte[] data = new byte[size];
        int state = 1234;
        for (int index = 0; index < size; index++) {
            state = state * 1103515245 + 12345;
            data[index] = (byte) ('0' + ((state >>> 16) & 63));
        }
        return data;
    }
}
