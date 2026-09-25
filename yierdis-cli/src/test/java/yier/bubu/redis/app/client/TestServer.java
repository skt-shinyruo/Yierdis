package yier.bubu.redis.app.client;

import yier.bubu.redis.app.server.YierdisServerBootstrap;
import yier.bubu.redis.app.server.args.YierdisServerFileConfig;

import java.util.Properties;

final class TestServer implements AutoCloseable {
    private final YierdisServerBootstrap server;

    private TestServer(YierdisServerBootstrap server) {
        this.server = server;
    }

    static TestServer start() throws Exception {
        // Bind port=0 for ephemeral port (avoids conflicts on CI/dev machines).
        Properties props = new Properties();
        props.setProperty("port", "0");
        props.setProperty("maxmemoryBytes", "0");
        props.setProperty("ioThreads", "1");
        props.setProperty("noCleanup", "true");
        return new TestServer(YierdisServerBootstrap.start(
                YierdisServerFileConfig.fromProperties(props).toRuntimeConfig()
        ));
    }

    int port() {
        return server.port();
    }

    @Override
    public void close() {
        server.close();
    }
}
