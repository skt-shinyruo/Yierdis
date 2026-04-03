package yier.bubu.redis;

import yier.bubu.redis.args.YierdisCliException;
import yier.bubu.redis.args.YierdisServerRuntimeConfig;

/**
 * 当用户显式选择 {@code --offheapBackend foreign} 时，校验当前 JVM 是否提供正式 FFM API。
 * <p>
 * 从 JDK 25 起，Yierdis 的 foreign 后端基于 {@code java.lang.foreign}，不再需要额外的 incubator 模块参数。
 */
final class ForeignMemoryAutoModules {
    private ForeignMemoryAutoModules() {
    }

    /**
     * @return 永远返回 null；保留原签名以维持 server 启动路径稳定。
     */
    static Integer maybeRelaunchIfNeeded(YierdisServerRuntimeConfig config, String[] appArgs) {
        if (config == null) {
            return null;
        }
        if (config.offheapBackend() != YierdisServerRuntimeConfig.OffheapBackend.FOREIGN) {
            return null;
        }
        try {
            Class.forName("java.lang.foreign.Arena");
            return null;
        } catch (ClassNotFoundException e) {
            throw YierdisCliException.userError(
                    "当前 JVM 不支持 java.lang.foreign，无法启用 --offheapBackend foreign。"
                            + "请使用 JDK 25 运行，或改用 --offheapBackend unsafe/netty。",
                    e);
        }
    }
}
