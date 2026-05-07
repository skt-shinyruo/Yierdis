package yier.bubu.redis.storage.memory.internal.ledger;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.internal.ledger.InMemoryLedger;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryReservation;

public class MemoryLedgerContractTest {
    @Test
    public void reserveCommitRollbackMaintainInvariants() {
        InMemoryLedger ledger = new InMemoryLedger(10);
        Assert.assertEquals(0, ledger.usedBytes());
        Assert.assertEquals(0, ledger.reservedBytes());

        MemoryReservation r1 = ledger.reserve(7);
        Assert.assertEquals(0, ledger.usedBytes());
        Assert.assertEquals(7, ledger.reservedBytes());

        try {
            ledger.reserve(4);
            Assert.fail("expected OOM");
        } catch (MemoryLedgerOutOfMemoryException e) {
            Assert.assertEquals(MemoryLedgerOutOfMemoryException.REDIS_OOM_MESSAGE, e.getMessage());
        }

        ledger.rollback(r1);
        Assert.assertEquals(0, ledger.usedBytes());
        Assert.assertEquals(0, ledger.reservedBytes());

        MemoryReservation r2 = ledger.reserve(4);
        ledger.commit(r2, 3);
        Assert.assertEquals(3, ledger.usedBytes());
        Assert.assertEquals(0, ledger.reservedBytes());

        MemoryReservation r3 = ledger.reserve(7);
        ledger.commit(r3, 7);
        Assert.assertEquals(10, ledger.usedBytes());

        try {
            ledger.reserve(1);
            Assert.fail("expected OOM");
        } catch (MemoryLedgerOutOfMemoryException ignored) {
            // expected
        }
    }
}

