package yier.bubu.redis.integration;

import yier.bubu.redis.app.server.args.YierdisServerFileConfig;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;

import java.util.Properties;

/**
 * 集成测试的服务配置：把旧 argv 形态的键值对（"--name value" / "--flag"）转成运行时配置。
 * 基础默认：port=0（临时端口）、ioThreads=1、noCleanup=true；maxmemoryBytes 由调用方给出。
 */
public final class TestServerConfig {
    private TestServerConfig() {
    }

    public static YierdisServerRuntimeConfig config(String... kv) {
        Properties props = new Properties();
        props.setProperty("port", "0");
        props.setProperty("ioThreads", "1");
        props.setProperty("noCleanup", "true");
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
