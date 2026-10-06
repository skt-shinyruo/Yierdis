package yier.bubu.redis.client;

import java.util.ArrayList;
import java.util.List;

/**
 * EXPIRE 系列的条件。NX 与 XX、GT、LT 互斥；GT 与 LT 互斥。XX 可以和 GT 或 LT 一起用。
 */
public final class ExpireOptions {
    private final boolean nx;
    private final boolean xx;
    private final boolean gt;
    private final boolean lt;

    private ExpireOptions(boolean nx, boolean xx, boolean gt, boolean lt) {
        this.nx = nx;
        this.xx = xx;
        this.gt = gt;
        this.lt = lt;
    }

    public static ExpireOptions nx() {
        return new ExpireOptions(false, false, false, false).andNx();
    }

    public static ExpireOptions xx() {
        return new ExpireOptions(false, false, false, false).andXx();
    }

    public static ExpireOptions gt() {
        return new ExpireOptions(false, false, false, false).andGt();
    }

    public static ExpireOptions lt() {
        return new ExpireOptions(false, false, false, false).andLt();
    }

    public ExpireOptions andNx() {
        if (xx || gt || lt) {
            throw exclusiveNx();
        }
        return new ExpireOptions(true, xx, gt, lt);
    }

    public ExpireOptions andXx() {
        if (nx) {
            throw exclusiveNx();
        }
        return new ExpireOptions(nx, true, gt, lt);
    }

    public ExpireOptions andGt() {
        if (nx) {
            throw exclusiveNx();
        }
        if (lt) {
            throw exclusiveGtLt();
        }
        return new ExpireOptions(nx, xx, true, lt);
    }

    public ExpireOptions andLt() {
        if (nx) {
            throw exclusiveNx();
        }
        if (gt) {
            throw exclusiveGtLt();
        }
        return new ExpireOptions(nx, xx, gt, true);
    }

    List<String> tokens() {
        ArrayList<String> tokens = new ArrayList<>();
        if (nx) {
            tokens.add("NX");
        }
        if (xx) {
            tokens.add("XX");
        }
        if (gt) {
            tokens.add("GT");
        }
        if (lt) {
            tokens.add("LT");
        }
        return tokens;
    }

    private static IllegalArgumentException exclusiveNx() {
        return new IllegalArgumentException(
                "NX and XX, GT or LT options at the same time are not compatible");
    }

    private static IllegalArgumentException exclusiveGtLt() {
        return new IllegalArgumentException("GT and LT options at the same time are not compatible");
    }
}
