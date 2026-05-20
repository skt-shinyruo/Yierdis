package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandChangeObserver;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.runtime.api.YierdisChangeEvent;
import yier.bubu.redis.runtime.api.YierdisChangeKind;
import yier.bubu.redis.storage.api.DbChange;
import yier.bubu.redis.storage.api.DbChangeContext;
import yier.bubu.redis.storage.api.DbChangeKind;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RuntimeChangeSinkCommandChangeObserverTest {
    @Test
    public void onCommandChangeEmitsUserCommandEvent() {
        ArrayList<YierdisChangeEvent> events = new ArrayList<>();
        CommandChangeObserver observer = RuntimeChangeSinkCommandChangeObserver.fromSink(events::add);

        observer.onCommandChange(4, ByteArrayExecutionRequest.fromUtf8("SET", List.of("k", "v")));

        Assert.assertEquals(1, events.size());
        YierdisChangeEvent event = events.get(0);
        Assert.assertEquals(4, event.dbIndex());
        Assert.assertEquals(YierdisChangeKind.USER_COMMAND, event.kind());
        Assert.assertFalse(event.synthetic());
        Assert.assertEquals("SET", arg(event, 0));
        Assert.assertEquals("k", arg(event, 1));
        Assert.assertEquals("v", arg(event, 2));
    }

    @Test
    public void observeExecutionBridgesSyntheticDbChanges() {
        ArrayList<YierdisChangeEvent> events = new ArrayList<>();
        CommandChangeObserver observer = RuntimeChangeSinkCommandChangeObserver.fromSink(events::add);

        observer.observeExecution(() -> DbChangeContext.emit(DbChange.syntheticDelete(
                2,
                DbChangeKind.EXPIRED,
                "dead".getBytes(StandardCharsets.UTF_8)
        )));

        Assert.assertEquals(1, events.size());
        YierdisChangeEvent event = events.get(0);
        Assert.assertEquals(2, event.dbIndex());
        Assert.assertEquals(YierdisChangeKind.EXPIRED, event.kind());
        Assert.assertTrue(event.synthetic());
        Assert.assertEquals("DEL", arg(event, 0));
        Assert.assertEquals("dead", arg(event, 1));
    }

    @Test
    public void noopSinkUsesNoopObserver() {
        Assert.assertSame(CommandChangeObserver.NOOP, RuntimeChangeSinkCommandChangeObserver.fromSink(null));
    }

    private static String arg(YierdisChangeEvent event, int index) {
        return new String(event.request().toByteArray(index), StandardCharsets.UTF_8);
    }
}
