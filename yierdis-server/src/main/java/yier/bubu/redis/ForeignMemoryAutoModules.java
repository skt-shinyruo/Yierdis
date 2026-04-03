package yier.bubu.redis;

import yier.bubu.redis.args.YierdisCliException;

/**
 * Validates that the current JVM exposes the JDK 25 Foreign Function and Memory API.
 */
final class ForeignMemoryAutoModules {
    private ForeignMemoryAutoModules() {
    }

    static void ensureFfmAvailable() {
        try {
            Class.forName("java.lang.foreign.Arena");
        } catch (ClassNotFoundException e) {
            throw YierdisCliException.userError(
                    "当前 JVM 不支持 java.lang.foreign。Yierdis 现在要求使用 JDK 25 运行。",
                    e
            );
        }
    }
}
