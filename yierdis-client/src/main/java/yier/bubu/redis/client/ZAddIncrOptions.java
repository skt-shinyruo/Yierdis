package yier.bubu.redis.client;

import java.util.List;

/**
 * 带 INCR 的 ZADD 选项。回复是新分数的文本，或条件没写上时的 null。
 */
public final class ZAddIncrOptions {
    private final ZAddOptionState state;

    private ZAddIncrOptions(ZAddOptionState state) {
        this.state = state;
    }

    public static ZAddIncrOptions nx() {
        return new ZAddIncrOptions(ZAddOptionState.incr().nx());
    }

    public static ZAddIncrOptions xx() {
        return new ZAddIncrOptions(ZAddOptionState.incr().xx());
    }

    public static ZAddIncrOptions gt() {
        return new ZAddIncrOptions(ZAddOptionState.incr().gt());
    }

    public static ZAddIncrOptions lt() {
        return new ZAddIncrOptions(ZAddOptionState.incr().lt());
    }

    public static ZAddIncrOptions ch() {
        return new ZAddIncrOptions(ZAddOptionState.incr().ch());
    }

    public ZAddIncrOptions andNx() {
        return new ZAddIncrOptions(state.nx());
    }

    public ZAddIncrOptions andXx() {
        return new ZAddIncrOptions(state.xx());
    }

    public ZAddIncrOptions andGt() {
        return new ZAddIncrOptions(state.gt());
    }

    public ZAddIncrOptions andLt() {
        return new ZAddIncrOptions(state.lt());
    }

    public ZAddIncrOptions andCh() {
        return new ZAddIncrOptions(state.ch());
    }

    List<String> tokens() {
        return state.tokens();
    }
}
