package yier.bubu.redis.client.command;

import java.util.ArrayList;
import java.util.List;

/**
 * 按分数取范围时的 LIMIT。{@link #withScores()} 在本地拒绝，WITHSCORES 是另一组方法。
 */
public final class ScoreRangeOptions {
    private final long offset;
    private final long count;

    private ScoreRangeOptions(long offset, long count) {
        this.offset = offset;
        this.count = count;
    }

    public static ScoreRangeOptions limit(long offset, long count) {
        return new ScoreRangeOptions(offset, count);
    }

    /**
     * WITHSCORES 会把回复从成员列表改成成员和分数。调用在组选项时抛出，不会发出命令。
     */
    public ScoreRangeOptions withScores() {
        throw new IllegalArgumentException("WITHSCORES changes the reply; use the withScores method");
    }

    public List<String> tokens() {
        ArrayList<String> tokens = new ArrayList<>(3);
        tokens.add("LIMIT");
        tokens.add(Long.toString(offset));
        tokens.add(Long.toString(count));
        return tokens;
    }
}
