package yier.bubu.redis.app.server;

// 读取构建注入的版本等 server 应用元信息。

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public final class YierdisBuildInfo {
    private static final String VERSION_RESOURCE = "/yierdis-version.properties";
    private static final String VERSION_KEY = "version";

    private static final String VERSION = loadVersion();
    private static final byte[] VERSION_ASCII = VERSION.getBytes(StandardCharsets.US_ASCII);

    private YierdisBuildInfo() {
    }

    public static String version() {
        return VERSION;
    }

    /**
     * 返回 US-ASCII 编码的版本字节数组（用于协议写出路径，例如 HELLO/INFO 等的版本展示）。
     * <p>
     * 说明：该数组为共享常量，调用方不得修改其内容。
     */
    public static byte[] versionAsciiBytes() {
        return VERSION_ASCII;
    }

    private static String loadVersion() {
        String version = "unknown";
        try (InputStream in = YierdisBuildInfo.class.getResourceAsStream(VERSION_RESOURCE)) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                String v = props.getProperty(VERSION_KEY);
                if (v != null && !v.isBlank()) {
                    version = v.trim();
                }
            }
        } catch (IOException ignored) {
            // ignore
        }
        return version;
    }
}
