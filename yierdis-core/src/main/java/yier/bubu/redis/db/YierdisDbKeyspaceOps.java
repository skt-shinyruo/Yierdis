package yier.bubu.redis.db;

import yier.bubu.redis.ops.KeyspaceOps;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

final class YierdisDbKeyspaceOps implements KeyspaceOps {
    private final YierdisDb db;

    YierdisDbKeyspaceOps(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    @Override
    public ValueType typeOf(YierdisBytesView keyView) {
        return db.typeOf(keyView);
    }

    @Override
    public long del(Collection<byte[]> keys) {
        return db.del(keys);
    }

    @Override
    public boolean existsKey(YierdisBytesView keyView) {
        return db.existsKey(keyView);
    }

    @Override
    public List<byte[]> keys(byte[] globPattern, int maxMatches, long timeBudgetNanos) {
        return db.keys(globPattern, maxMatches, timeBudgetNanos);
    }

    @Override
    public ScanCursorV2 scan(ScanCursorV2 cursor, byte[] globPattern, int count, List<byte[]> out) {
        return db.scan(cursor, globPattern, count, out);
    }
}

