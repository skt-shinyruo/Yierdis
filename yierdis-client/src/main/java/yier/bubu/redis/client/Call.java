package yier.bubu.redis.client;

import java.util.List;
import java.util.function.Function;

/**
 * 一次命令的参数和回复转换。普通模式、管道、事务共用这一份，避免三处各自排参数。
 */
final class Call<T> {
    final String[] args;
    private final Function<Object, T> decode;

    private Call(String[] args, Function<Object, T> decode) {
        this.args = args;
        this.decode = decode;
    }

    static <T> Call<T> of(Function<Object, T> decode, String... args) {
        return new Call<>(args.clone(), decode);
    }

    static <T> Call<T> of(Function<Object, T> decode, List<String> args) {
        return new Call<>(args.toArray(String[]::new), decode);
    }

    static Call<Object> raw(String... args) {
        return of(reply -> reply, args);
    }

    T decode(Object reply) {
        return decode.apply(reply);
    }
}
