package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.ScanCursorV2;

import java.util.List;
import java.util.Objects;

final class YierdisDbIntrospection implements YierdisSnapshot {
    private final YierdisDbKernel kernel;

    YierdisDbIntrospection(YierdisDbKernel kernel) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
    }

    String objectEncoding(BytesView keyView) {
        return kernel.execute(DbUse.inspect(scope -> scope.objectEncoding(keyView)));
    }

    String objectEncoding(byte[] keyBytes) {
        return kernel.execute(DbUse.inspect(scope -> scope.objectEncoding(keyBytes)));
    }

    @Override
    public ScanCursorV2 snapshot(ScanCursorV2 cursor, int count, List<YierdisSnapshotEntry> out) {
        return kernel.execute(DbUse.inspect(scope -> scope.snapshot(cursor, count, out)));
    }
}
