package yier.bubu.redis.client;

import java.util.ArrayList;
import java.util.List;

/**
 * SET 的关键字。同一关键字再写一次只覆盖数值；NX/XX 互斥，不同的过期关键字互斥。
 * GET 只属于返回旧值的那条路径。
 */
final class SetOptionState {
    private final Condition condition;
    private final String expireKeyword;
    private final Long expireAmount;
    private final boolean includeGet;

    private SetOptionState(Condition condition, String expireKeyword, Long expireAmount, boolean includeGet) {
        this.condition = condition;
        this.expireKeyword = expireKeyword;
        this.expireAmount = expireAmount;
        this.includeGet = includeGet;
    }

    static SetOptionState ok() {
        return new SetOptionState(null, null, null, false);
    }

    static SetOptionState get() {
        return new SetOptionState(null, null, null, true);
    }

    SetOptionState nx() {
        return condition(Condition.NX);
    }

    SetOptionState xx() {
        return condition(Condition.XX);
    }

    SetOptionState ex(long seconds) {
        return expire("EX", seconds);
    }

    SetOptionState px(long milliseconds) {
        return expire("PX", milliseconds);
    }

    SetOptionState exAt(long unixSeconds) {
        return expire("EXAT", unixSeconds);
    }

    SetOptionState pxAt(long unixMilliseconds) {
        return expire("PXAT", unixMilliseconds);
    }

    SetOptionState keepTtl() {
        return expire("KEEPTTL", null);
    }

    List<String> tokens() {
        ArrayList<String> tokens = new ArrayList<>();
        if (condition == Condition.NX) {
            tokens.add("NX");
        } else if (condition == Condition.XX) {
            tokens.add("XX");
        }
        if (expireKeyword != null) {
            tokens.add(expireKeyword);
            if (expireAmount != null) {
                tokens.add(Long.toString(expireAmount));
            }
        }
        if (includeGet) {
            tokens.add("GET");
        }
        return tokens;
    }

    private SetOptionState condition(Condition next) {
        if (condition != null && condition != next) {
            throw new IllegalArgumentException("NX and XX are mutually exclusive");
        }
        return new SetOptionState(next, expireKeyword, expireAmount, includeGet);
    }

    private SetOptionState expire(String keyword, Long amount) {
        if (expireKeyword != null && !expireKeyword.equals(keyword)) {
            throw new IllegalArgumentException(expireKeyword + " and " + keyword + " are mutually exclusive");
        }
        if ("KEEPTTL".equals(keyword)) {
            return new SetOptionState(condition, keyword, null, includeGet);
        }
        return new SetOptionState(condition, keyword, amount, includeGet);
    }

    private enum Condition {
        NX,
        XX
    }
}
