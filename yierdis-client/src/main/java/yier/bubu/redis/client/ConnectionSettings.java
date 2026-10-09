package yier.bubu.redis.client;

/**
 * 打开一条连接时用的地址、超时、DB 下标和事件循环线程数。超时必须大于 0，DB 下标不能为负，线程数必须大于 0。
 */
public record ConnectionSettings(
        String host,
        int port,
        long connectTimeoutMillis,
        long commandTimeoutMillis,
        int database,
        int ioThreadCount
) {
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 6378;
    public static final long DEFAULT_CONNECT_TIMEOUT_MILLIS = 5_000L;
    public static final long DEFAULT_COMMAND_TIMEOUT_MILLIS = 5_000L;
    public static final int DEFAULT_DATABASE = 0;
    public static final int DEFAULT_IO_THREAD_COUNT = 1;

    public ConnectionSettings {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("host is required");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        if (connectTimeoutMillis <= 0) {
            throw new IllegalArgumentException("connectTimeoutMillis must be > 0");
        }
        if (commandTimeoutMillis <= 0) {
            throw new IllegalArgumentException("commandTimeoutMillis must be > 0");
        }
        if (database < 0) {
            throw new IllegalArgumentException("database must be >= 0");
        }
        if (ioThreadCount <= 0) {
            throw new IllegalArgumentException("ioThreadCount must be > 0");
        }
    }

    public static ConnectionSettings defaults() {
        return new ConnectionSettings(
                DEFAULT_HOST,
                DEFAULT_PORT,
                DEFAULT_CONNECT_TIMEOUT_MILLIS,
                DEFAULT_COMMAND_TIMEOUT_MILLIS,
                DEFAULT_DATABASE,
                DEFAULT_IO_THREAD_COUNT
        );
    }

    public ConnectionSettings withHost(String host) {
        return new ConnectionSettings(host, port, connectTimeoutMillis, commandTimeoutMillis, database, ioThreadCount);
    }

    public ConnectionSettings withPort(int port) {
        return new ConnectionSettings(host, port, connectTimeoutMillis, commandTimeoutMillis, database, ioThreadCount);
    }

    public ConnectionSettings withConnectTimeoutMillis(long connectTimeoutMillis) {
        return new ConnectionSettings(host, port, connectTimeoutMillis, commandTimeoutMillis, database, ioThreadCount);
    }

    public ConnectionSettings withCommandTimeoutMillis(long commandTimeoutMillis) {
        return new ConnectionSettings(host, port, connectTimeoutMillis, commandTimeoutMillis, database, ioThreadCount);
    }

    public ConnectionSettings withDatabase(int database) {
        return new ConnectionSettings(host, port, connectTimeoutMillis, commandTimeoutMillis, database, ioThreadCount);
    }

    public ConnectionSettings withIoThreadCount(int ioThreadCount) {
        return new ConnectionSettings(host, port, connectTimeoutMillis, commandTimeoutMillis, database, ioThreadCount);
    }
}
