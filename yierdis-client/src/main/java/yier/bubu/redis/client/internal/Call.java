package yier.bubu.redis.client.internal;

import java.util.List;
import java.util.function.Function;

/**
 * 一次命令的参数和回复转换。普通提交和事务共用这一份，避免两处各自排参数。
 */
public final class Call<T> {
    public final String[] args;
    private final Function<Object, T> decode;

    private Call(String[] args, Function<Object, T> decode) {
        this.args = args;
        this.decode = decode;
    }

    public static <T> Call<T> of(Function<Object, T> decode, String... args) {
        return new Call<>(args.clone(), decode);
    }

    public static <T> Call<T> of(Function<Object, T> decode, List<String> args) {
        return new Call<>(args.toArray(String[]::new), decode);
    }

    public static Call<Object> raw(String... args) {
        if (args == null) {
            throw new IllegalArgumentException("command is required");
        }
        return of(reply -> reply, args);
    }

    public T decode(Object reply) {
        return decode.apply(reply);
    }
}
