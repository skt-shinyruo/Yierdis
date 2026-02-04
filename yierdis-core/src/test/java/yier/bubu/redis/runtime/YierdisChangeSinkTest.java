package yier.bubu.redis.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.testutil.FastTestClient;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.cmd;

public class YierdisChangeSinkTest {
    @Test
    public void emitsEventsForWriteCommandsOnly() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        List<YierdisChangeEvent> events = new ArrayList<>();
        YierdisChangeSink sink = events::add;

        try {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db, null, sink);
            try (FastTestClient client = new FastTestClient(processor)) {
                events.clear();
                client.execute(cmd("GET", "missing"));
                Assert.assertEquals(0, events.size());

                Assert.assertTrue(client.execute(cmd("SET", "k", "v")) instanceof RespSimpleString);
                Assert.assertEquals(1, events.size());

                YierdisChangeEvent e = events.get(0);
                Assert.assertEquals(0, e.dbIndex());
                Assert.assertEquals("SET", new String(e.argv()[0], StandardCharsets.US_ASCII));
                Assert.assertEquals("k", new String(e.argv()[1], StandardCharsets.US_ASCII));
                Assert.assertEquals("v", new String(e.argv()[2], StandardCharsets.US_ASCII));
            }
        } finally {
            db.shutdown();
        }
    }
}

