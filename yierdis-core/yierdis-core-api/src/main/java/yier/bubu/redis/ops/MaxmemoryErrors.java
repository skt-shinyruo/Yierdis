package yier.bubu.redis.ops;

/**
 * Stable error strings for maxmemory enforcement.
 */
public final class MaxmemoryErrors {
    public static final String OOM_ERR = "OOM command not allowed when used memory > 'maxmemory'.";

    private MaxmemoryErrors() {
    }
}

