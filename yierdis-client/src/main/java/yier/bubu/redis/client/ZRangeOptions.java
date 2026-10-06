package yier.bubu.redis.client;

import java.util.ArrayList;
import java.util.List;

/**
 * ZRANGE 的 REV。{@link #withScores()} 在本地拒绝，WITHSCORES 是另一组返回成员和分数的方法。
 */
public final class ZRangeOptions {
    private final boolean rev;

    private ZRangeOptions(boolean rev) {
        this.rev = rev;
    }

    public static ZRangeOptions rev() {
        return new ZRangeOptions(true);
    }

    /**
     * WITHSCORES 会在每个成员后面带上分数文本。调用在组选项时抛出，不会发出 ZRANGE。
     */
    public ZRangeOptions withScores() {
        throw new IllegalArgumentException("WITHSCORES changes the reply; use zrangeWithScores");
    }

    List<String> tokens(boolean withScores) {
        ArrayList<String> tokens = new ArrayList<>();
        if (rev) {
            tokens.add("REV");
        }
        if (withScores) {
            tokens.add("WITHSCORES");
        }
        return tokens;
    }
}
