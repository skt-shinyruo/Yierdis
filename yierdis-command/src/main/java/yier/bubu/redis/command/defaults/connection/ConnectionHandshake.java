package yier.bubu.redis.command.defaults.connection;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * {@code AUTH}、{@code HELLO AUTH}、{@code CLIENT SETNAME} 与 {@code HELLO SETNAME} 共用的连接握手规则。
 *
 * <p>Yierdis 没有认证配置面，规则等同 Redis 未设置 {@code requirepass} 时的 nopass {@code default} 用户。</p>
 */
public final class ConnectionHandshake {
    public static final String INVALID_CLIENT_NAME =
            "ERR Client names cannot contain spaces, newlines or special characters.";
    public static final String WRONGPASS =
            "WRONGPASS invalid username-password pair or user is disabled.";

    private static final byte[] DEFAULT_USER = "default".getBytes(StandardCharsets.US_ASCII);

    private ConnectionHandshake() {
    }

    /** 空名字表示清空连接名，总是合法；非空名字只接受 ASCII {@code '!'..'~'}。 */
    public static boolean validClientName(byte[] rawName) {
        for (byte value : rawName) {
            int code = value & 0xff;
            if (code < '!' || code > '~') {
                return false;
            }
        }
        return true;
    }

    /**
     * nopass {@code default} 用户接受任意密码；其他用户名（按字节比较，区分大小写）都不存在，
     * 调用方应回复 {@link #WRONGPASS}。
     */
    public static boolean acceptsCredentials(byte[] username) {
        return Arrays.equals(DEFAULT_USER, username);
    }
}
