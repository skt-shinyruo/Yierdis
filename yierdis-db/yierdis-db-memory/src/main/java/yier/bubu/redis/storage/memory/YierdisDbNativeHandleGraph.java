package yier.bubu.redis.storage.memory;

import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;

import java.util.Objects;

final class YierdisDbNativeHandleGraph {
    private YierdisDbNativeHandleGraph() {
    }

    enum Role {
        KEY_BYTES,
        ENTRY_RECORD,
        STRING_VALUE,
        COLLECTION_ROOT
    }

    @FunctionalInterface
    interface Visitor {
        void visit(Role role, NativeHandle handle, EntryRecord record);
    }

    static void visitReachable(YierdisDbKeyLifecycle lifecycle, Visitor visitor) {
        Objects.requireNonNull(lifecycle, "lifecycle");
        Objects.requireNonNull(visitor, "visitor");
        NativeAllocator allocator = lifecycle.nativeAllocator();
        // 以 keyDirectory 为根遍历当前可达 native 对象；未从目录发布的半创建对象不会出现在这张图里。
        lifecycle.keyDirectory().forEachEntry((keyHandle, entryHandle) -> {
            EntryRecord record = lifecycle.entryTable().get(entryHandle);
            NativeHandle keyNativeHandle = KeyHandleAccess.allocatorNativeHandleOrNull(keyHandle);
            if (keyNativeHandle != null) {
                visitResolved(allocator, keyNativeHandle, Role.KEY_BYTES, record, visitor);
            }
            visitResolved(allocator, entryHandle.nativeHandle(), Role.ENTRY_RECORD, record, visitor);
            visitValueHandle(allocator, record, visitor);
        });
    }

    private static void visitValueHandle(NativeAllocator allocator, EntryRecord record, Visitor visitor) {
        if (record == null) {
            return;
        }
        ValueHandle valueHandle = record.valueHandle();
        if (valueHandle == null || valueHandle.isNull()) {
            return;
        }
        Role role = valueRole(record.type());
        if (role == null) {
            return;
        }
        visitResolved(allocator, valueHandle.nativeHandle(), role, record, visitor);
    }

    private static Role valueRole(ValueType type) {
        if (type == null) {
            return null;
        }
        return switch (type) {
            case STRING -> Role.STRING_VALUE;
            case LIST, HASH, SET, ZSET -> Role.COLLECTION_ROOT;
        };
    }

    private static void visitResolved(
            NativeAllocator allocator,
            NativeHandle handle,
            Role role,
            EntryRecord record,
            Visitor visitor
    ) {
        // visitor 只接收通过 allocator resolve 验证过的 handle，stale handle 会在遍历边界暴露出来。
        try (NativeObjectView ignored = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            visitor.visit(role, handle, record);
        }
    }
}
