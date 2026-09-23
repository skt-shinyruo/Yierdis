package yier.bubu.redis.integration.runtime;

import java.util.Arrays;

final class ServerArgs {
    private ServerArgs() {
    }

    static String[] of(String... extraArgs) {
        String[] base = new String[]{
                "--port", "0",
                "--ioThreads", "1",
                "--noCleanup"
        };
        String[] argv = Arrays.copyOf(base, base.length + extraArgs.length);
        System.arraycopy(extraArgs, 0, argv, base.length, extraArgs.length);
        return argv;
    }
}
