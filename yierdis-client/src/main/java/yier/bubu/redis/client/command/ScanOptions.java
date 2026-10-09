package yier.bubu.redis.client.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * SCAN 系列的 MATCH 和 COUNT。{@link #withNoValues()} 在本地拒绝，NOVALUES 只属于 {@code hscanNoValues}。
 */
public final class ScanOptions {
    private final String match;
    private final Long count;

    private ScanOptions(String match, Long count) {
        this.match = match;
        this.count = count;
    }

    public static ScanOptions match(String pattern) {
        return new ScanOptions(Objects.requireNonNull(pattern, "pattern"), null);
    }

    public static ScanOptions count(long count) {
        return new ScanOptions(null, count);
    }

    public ScanOptions andMatch(String pattern) {
        return new ScanOptions(Objects.requireNonNull(pattern, "pattern"), count);
    }

    public ScanOptions andCount(long count) {
        return new ScanOptions(match, count);
    }

    /**
     * NOVALUES 会让 HSCAN 只回复 field。调用在组选项时抛出，不会发出扫描。
     */
    public ScanOptions withNoValues() {
        throw new IllegalArgumentException("NOVALUES changes the HSCAN reply; use hscanNoValues");
    }

    public List<String> tokens() {
        ArrayList<String> tokens = new ArrayList<>();
        if (match != null) {
            tokens.add("MATCH");
            tokens.add(match);
        }
        if (count != null) {
            tokens.add("COUNT");
            tokens.add(Long.toString(count));
        }
        return tokens;
    }
}
