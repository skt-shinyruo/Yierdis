package yier.bubu.redis.client.command;

import java.util.ArrayList;
import java.util.List;

/**
 * ZADD 的关键字。NX 与 XX 互斥，GT、LT、NX 也不能组合。INCR 只出现在返回分数文本的路径上。
 */
final class ZAddOptionState {
    private final boolean nx;
    private final boolean xx;
    private final boolean gt;
    private final boolean lt;
    private final boolean ch;
    private final boolean incr;

    private ZAddOptionState(boolean nx, boolean xx, boolean gt, boolean lt, boolean ch, boolean incr) {
        this.nx = nx;
        this.xx = xx;
        this.gt = gt;
        this.lt = lt;
        this.ch = ch;
        this.incr = incr;
    }

    static ZAddOptionState empty() {
        return new ZAddOptionState(false, false, false, false, false, false);
    }

    static ZAddOptionState incr() {
        return new ZAddOptionState(false, false, false, false, false, true);
    }

    ZAddOptionState nx() {
        if (xx) {
            throw new IllegalArgumentException("XX and NX options at the same time are not compatible");
        }
        if (gt || lt) {
            throw new IllegalArgumentException("GT, LT, and/or NX options at the same time are not compatible");
        }
        return new ZAddOptionState(true, xx, gt, lt, ch, incr);
    }

    ZAddOptionState xx() {
        if (nx) {
            throw new IllegalArgumentException("XX and NX options at the same time are not compatible");
        }
        return new ZAddOptionState(nx, true, gt, lt, ch, incr);
    }

    ZAddOptionState gt() {
        if (nx || lt) {
            throw new IllegalArgumentException("GT, LT, and/or NX options at the same time are not compatible");
        }
        return new ZAddOptionState(nx, xx, true, lt, ch, incr);
    }

    ZAddOptionState lt() {
        if (nx || gt) {
            throw new IllegalArgumentException("GT, LT, and/or NX options at the same time are not compatible");
        }
        return new ZAddOptionState(nx, xx, gt, true, ch, incr);
    }

    ZAddOptionState ch() {
        return new ZAddOptionState(nx, xx, gt, lt, true, incr);
    }

    boolean incrFlag() {
        return incr;
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
        if (ch) {
            tokens.add("CH");
        }
        if (incr) {
            tokens.add("INCR");
        }
        return tokens;
    }
}
