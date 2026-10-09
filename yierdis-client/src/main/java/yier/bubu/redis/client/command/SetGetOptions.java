package yier.bubu.redis.client.command;

import java.util.List;

/**
 * 带 GET 的 SET 选项。回复是旧值或 null，关键字组合与 {@link SetOptions} 相同。
 */
public final class SetGetOptions {
    private final SetOptionState state;

    private SetGetOptions(SetOptionState state) {
        this.state = state;
    }

    public static SetGetOptions nx() {
        return new SetGetOptions(SetOptionState.get().nx());
    }

    public static SetGetOptions xx() {
        return new SetGetOptions(SetOptionState.get().xx());
    }

    public static SetGetOptions ex(long seconds) {
        return new SetGetOptions(SetOptionState.get().ex(seconds));
    }

    public static SetGetOptions px(long milliseconds) {
        return new SetGetOptions(SetOptionState.get().px(milliseconds));
    }

    public static SetGetOptions exAt(long unixSeconds) {
        return new SetGetOptions(SetOptionState.get().exAt(unixSeconds));
    }

    public static SetGetOptions pxAt(long unixMilliseconds) {
        return new SetGetOptions(SetOptionState.get().pxAt(unixMilliseconds));
    }

    public static SetGetOptions keepTtl() {
        return new SetGetOptions(SetOptionState.get().keepTtl());
    }

    public SetGetOptions andNx() {
        return new SetGetOptions(state.nx());
    }

    public SetGetOptions andXx() {
        return new SetGetOptions(state.xx());
    }

    public SetGetOptions andEx(long seconds) {
        return new SetGetOptions(state.ex(seconds));
    }

    public SetGetOptions andPx(long milliseconds) {
        return new SetGetOptions(state.px(milliseconds));
    }

    public SetGetOptions andExAt(long unixSeconds) {
        return new SetGetOptions(state.exAt(unixSeconds));
    }

    public SetGetOptions andPxAt(long unixMilliseconds) {
        return new SetGetOptions(state.pxAt(unixMilliseconds));
    }

    public SetGetOptions andKeepTtl() {
        return new SetGetOptions(state.keepTtl());
    }

    public List<String> tokens() {
        return state.tokens();
    }
}
