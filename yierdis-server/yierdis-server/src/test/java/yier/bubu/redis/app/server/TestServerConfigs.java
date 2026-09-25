package yier.bubu.redis.app.server;

import yier.bubu.redis.app.server.args.YierdisServerFileConfig;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;

import java.util.Properties;

/**
 * 测试专用：把旧 argv 形态的键值对（"--name value" / "--flag"）转成运行时配置。
 * 基础默认：port=0（临时端口）、maxmemoryBytes=0；调用方传的键覆盖默认值。
 */
final class TestServerConfigs {
    private TestServerConfigs() {
    }

    static YierdisServerRuntimeConfig config(String... kv) {
        Properties props = new Properties();
        props.setProperty("port", "0");
        props.setProperty("maxmemoryBytes", "0");
        for (int i = 0; i < kv.length; i++) {
            String key = kv[i];
            if (key.startsWith("--")) {
                key = key.substring(2);
            }
            if (key.equals("noCleanup") || key.equals("nativeDefragEnabled")) {
                props.setProperty(key, "true");
            } else {
                props.setProperty(key, kv[++i]);
            }
        }
        YierdisServerFileConfig config = YierdisServerFileConfig.fromProperties(props);
        config.normalizeAndValidate();
        return config.toRuntimeConfig();
    }
}
