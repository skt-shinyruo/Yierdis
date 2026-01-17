package yier.bubu.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class YierdisServer {
    private static final Logger log = LoggerFactory.getLogger(YierdisServer.class);

    public static void main(String[] args) throws Exception {
        final ServerConfig config;
        try {
            config = ServerConfig.fromArgs(args);
        } catch (IllegalArgumentException e) {
            // fromArgs already printed usage; keep startup path clean.
            System.exit(2);
            return; // keep compiler happy
        }
        if (config == null) {
            return;
        }

        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config)) {
            log.info("yierdis started on 0.0.0.0:{} (RESP2 default; supports HELLO 3 / RESP3 + inline)", server.port());
            server.awaitClose();
        }
    }

    private YierdisServer() {
    }
}
