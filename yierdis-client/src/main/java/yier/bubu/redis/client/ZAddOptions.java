package yier.bubu.redis.client;

import java.util.List;

/**
 * 返回 long 的 ZADD 选项。{@link #withIncr()} 在本地拒绝，因为 INCR 的回复是分数文本。
 */
public final class ZAddOptions {
    private final ZAddOptionState state;

    private ZAddOptions(ZAddOptionState state) {
        this.state = state;
    }

    public static ZAddOptions nx() {
        return new ZAddOptions(ZAddOptionState.empty().nx());
    }

    public static ZAddOptions xx() {
        return new ZAddOptions(ZAddOptionState.empty().xx());
    }

    public static ZAddOptions gt() {
        return new ZAddOptions(ZAddOptionState.empty().gt());
    }

    public static ZAddOptions lt() {
        return new ZAddOptions(ZAddOptionState.empty().lt());
    }

    public static ZAddOptions ch() {
        return new ZAddOptions(ZAddOptionState.empty().ch());
    }

    public ZAddOptions andNx() {
        return new ZAddOptions(state.nx());
    }

    public ZAddOptions andXx() {
        return new ZAddOptions(state.xx());
    }

    public ZAddOptions andGt() {
        return new ZAddOptions(state.gt());
    }

    public ZAddOptions andLt() {
        return new ZAddOptions(state.lt());
    }

    public ZAddOptions andCh() {
        return new ZAddOptions(state.ch());
    }

    /**
     * INCR 不属于这条回复。调用在组选项时抛出，不会发出 ZADD。
     */
    public ZAddOptions withIncr() {
        throw new IllegalArgumentException("INCR changes the ZADD reply; use zaddIncr");
    }

    List<String> tokens() {
        if (state.incrFlag()) {
            throw new IllegalArgumentException("INCR changes the ZADD reply; use zaddIncr");
        }
        return state.tokens();
    }
}
