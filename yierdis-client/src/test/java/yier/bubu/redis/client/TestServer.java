package yier.bubu.redis.client;

import yier.bubu.redis.app.server.YierdisServerBootstrap;
import yier.bubu.redis.app.server.args.YierdisServerFileConfig;

import java.util.Properties;

final class TestServer implements AutoCloseable {
    private final YierdisServerBootstrap server;

    private TestServer(YierdisServerBootstrap server) {
        this.server = server;
    }

    static TestServer start() throws Exception {
        return start(0);
    }

    static TestServer start(int port) throws Exception {
        Properties props = new Properties();
        // port 0 交给系统选空闲端口。默认地址测试另行绑定 6378。
        props.setProperty("port", Integer.toString(port));
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
