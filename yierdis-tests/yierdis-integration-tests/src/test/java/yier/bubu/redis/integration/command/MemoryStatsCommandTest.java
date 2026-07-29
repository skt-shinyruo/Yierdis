package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandRegistries;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyMap;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class MemoryStatsCommandTest {
    private static final List<String> EXPECTED_KEYS = List.of(
            "maxmemory_bytes",
            "used_bytes_for_maxmemory",
            "effective_used_bytes_for_maxmemory",
            "ledger_used_bytes",
            "offheap_used_bytes",
            "ledger_reserved_bytes",
            "offheap_included_in_maxmemory",
            "keyspace_table_overhead_bytes_estimate",
            "expire_table_overhead_bytes_estimate",
            "expire_value_objects_bytes_estimate",
            "total_estimated_bytes",
            "keys_stored_offheap",
            "key_count",
            "expire_count",
            "expired_entries_awaiting_physical_deletion",
            "keyspace_rehashing",
            "keyspace_table0_capacity",
            "keyspace_table1_capacity",
            "expire_rehashing",
            "expire_table0_capacity",
            "expire_table1_capacity"
    );

    @Test
    public void memoryStatsReturnsStableKeyValuePairs() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                ReplyObject resp = client.execute(cmd("MEMORY", "STATS"));
                Assert.assertTrue(resp instanceof ReplyMap);
                List<ReplyMap.Entry> entries = ((ReplyMap) resp).entries();
                Assert.assertNotNull(entries);
                Assert.assertEquals(EXPECTED_KEYS.size(), entries.size());
                for (int i = 0; i < EXPECTED_KEYS.size(); i++) {
                    ReplyMap.Entry entry = entries.get(i);
                    Assert.assertTrue(entry.key() instanceof ReplyBulkString);
                    Assert.assertEquals(EXPECTED_KEYS.get(i), ((ReplyBulkString) entry.key()).asString());
                    Assert.assertTrue(
                            EXPECTED_KEYS.get(i) + " must use integer reply form",
                            entry.value() instanceof ReplyInteger
                    );
                }
            }
        });
    }

    @Test
    public void memoryStatsUsesGlobalProviderOnlyWhenItSuppliesAGlobalSnapshot() {
        forEachDb(db -> {
            YierdisMemoryStats global = YierdisMemoryStats.empty(31_337L, false);
            CommandDispatcher globalDispatcher = CommandRegistries.dispatcher(
                    DefaultCommandModules.create(db, new MemoryStatsProvider(global)));
            CommandDispatcher perDbDispatcher = CommandRegistries.dispatcher(
                    DefaultCommandModules.create(db, new MemoryStatsProvider(null)));

            Assert.assertEquals(31_337L, maxmemoryBytes(globalDispatcher));
            Assert.assertEquals(db.memory().memoryStats().maxmemoryBytes(), maxmemoryBytes(perDbDispatcher));
        });
    }

    private static long maxmemoryBytes(CommandDispatcher dispatcher) {
        try (FastTestClient client = new FastTestClient(dispatcher)) {
            ReplyMap map = (ReplyMap) client.execute(cmd("MEMORY", "STATS"));
            for (ReplyMap.Entry entry : map.entries()) {
                if ("maxmemory_bytes".equals(((ReplyBulkString) entry.key()).asString())) {
                    return ((ReplyInteger) entry.value()).value();
                }
            }
            throw new AssertionError("MEMORY STATS did not include maxmemory_bytes");
        }
    }

    private record MemoryStatsProvider(YierdisMemoryStats global) implements ServerInfoProvider {
        @Override
        public RedisReply info(CommandArgs args, CommandSession session) {
            return RedisReplies.error("ERR INFO unavailable");
        }

        @Override
        public RedisReply stats(CommandSession session) {
            return RedisReplies.error("ERR STATS unavailable");
        }

        @Override
        public YierdisMemoryStats memoryStats(CommandSession session) {
            return global;
        }
    }
}
