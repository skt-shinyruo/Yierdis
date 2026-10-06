package yier.bubu.redis.client;

import java.util.List;

/**
 * 返回 {@code "OK"} 或 null 的 SET 选项。{@link #withGet()} 在本地拒绝，因为 GET 会改成旧值回复。
 */
public final class SetOptions {
    private final SetOptionState state;

    private SetOptions(SetOptionState state) {
        this.state = state;
    }

    public static SetOptions nx() {
        return new SetOptions(SetOptionState.ok().nx());
    }

    public static SetOptions xx() {
        return new SetOptions(SetOptionState.ok().xx());
    }

    public static SetOptions ex(long seconds) {
        return new SetOptions(SetOptionState.ok().ex(seconds));
    }

    public static SetOptions px(long milliseconds) {
        return new SetOptions(SetOptionState.ok().px(milliseconds));
    }

    public static SetOptions exAt(long unixSeconds) {
        return new SetOptions(SetOptionState.ok().exAt(unixSeconds));
    }

    public static SetOptions pxAt(long unixMilliseconds) {
        return new SetOptions(SetOptionState.ok().pxAt(unixMilliseconds));
    }

    public static SetOptions keepTtl() {
        return new SetOptions(SetOptionState.ok().keepTtl());
    }

    public SetOptions andNx() {
        return new SetOptions(state.nx());
    }

    public SetOptions andXx() {
        return new SetOptions(state.xx());
    }

    public SetOptions andEx(long seconds) {
        return new SetOptions(state.ex(seconds));
    }

    public SetOptions andPx(long milliseconds) {
        return new SetOptions(state.px(milliseconds));
    }

    public SetOptions andExAt(long unixSeconds) {
        return new SetOptions(state.exAt(unixSeconds));
    }

    public SetOptions andPxAt(long unixMilliseconds) {
        return new SetOptions(state.pxAt(unixMilliseconds));
    }

    public SetOptions andKeepTtl() {
        return new SetOptions(state.keepTtl());
    }

    /**
     * GET 不属于这条回复。调用在组选项时抛出，不会发出 SET。
     */
    public SetOptions withGet() {
        throw new IllegalArgumentException("GET changes the SET reply; use setGet");
    }

    List<String> tokens() {
        return state.tokens();
    }
}
