package yier.bubu.redis.storage.api.result;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import yier.bubu.redis.bytes.BytesSlice;

/**
 * 把一次 live emit 立刻拷成独立条目。prepare 持有 source 期间 storage 仍可能 in-place 变更，
 * 所以 count/length/emit 必须来自同一份快照，而不能再次读 live handle。
 */
final class CapturedByteItems {
    private final List<Item> items;
    private final long retainedMemoryBytes;

    private CapturedByteItems(List<Item> items, long retainedMemoryBytes) {
        this.items = items;
        this.retainedMemoryBytes = retainedMemoryBytes;
    }

    static CapturedByteItems capture(Consumer<ByteValueSink> emitter) {
        Objects.requireNonNull(emitter, "emitter");
        CapturingSink sink = new CapturingSink();
        emitter.accept(sink);
        return new CapturedByteItems(sink.items, sink.retainedMemoryBytes);
    }

    int size() {
        return items.size();
    }

    long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    void visitLengths(IntConsumer out) {
        for (Item item : items) {
            out.accept(item.payloadLength);
        }
    }

    void emitTo(ByteValueSink out) {
        Objects.requireNonNull(out, "out");
        for (Item item : items) {
            item.emitTo(out);
        }
    }

    private static long addPayload(long current, int length) {
        if (length <= 0) {
            return current;
        }
        long extra = length;
        return Long.MAX_VALUE - current < extra ? Long.MAX_VALUE : current + extra;
    }

    private static final class CapturingSink implements ByteValueSink {
        private final List<Item> items = new ArrayList<>();
        private long retainedMemoryBytes;

        @Override
        public void value(byte[] data) {
            if (data == null) {
                nullValue();
                return;
            }
            addBytes(data.clone(), data.length);
        }

        @Override
        public void value(byte[] data, int offset, int length) {
            if (data == null) {
                nullValue();
                return;
            }
            Objects.checkFromIndexSize(offset, length, data.length);
            byte[] copy = new byte[length];
            System.arraycopy(data, offset, copy, 0, length);
            addBytes(copy, length);
        }

        @Override
        public void value(BytesSlice slice) {
            if (slice == null) {
                nullValue();
                return;
            }
            int length = slice.length();
            byte[] copy = new byte[length];
            slice.getBytes(0, copy, 0, length);
            addBytes(copy, length);
        }

        @Override
        public void longAscii(long value) {
            int length = Long.toString(value).length();
            items.add(Item.longAscii(value, length));
            retainedMemoryBytes = addPayload(retainedMemoryBytes, length);
        }

        @Override
        public void nullValue() {
            items.add(Item.nullItem());
        }

        private void addBytes(byte[] copy, int length) {
            items.add(Item.bytes(copy, length));
            retainedMemoryBytes = addPayload(retainedMemoryBytes, length);
        }
    }

    private static final class Item {
        private final Kind kind;
        private final byte[] bytes;
        private final long longValue;
        private final int payloadLength;

        private Item(Kind kind, byte[] bytes, long longValue, int payloadLength) {
            this.kind = kind;
            this.bytes = bytes;
            this.longValue = longValue;
            this.payloadLength = payloadLength;
        }

        private static Item bytes(byte[] copy, int length) {
            return new Item(Kind.BYTES, copy, 0L, length);
        }

        private static Item longAscii(long value, int length) {
            return new Item(Kind.LONG_ASCII, null, value, length);
        }

        private static Item nullItem() {
            return new Item(Kind.NULL, null, 0L, -1);
        }

        private void emitTo(ByteValueSink out) {
            switch (kind) {
                case BYTES -> out.value(bytes, 0, payloadLength);
                case LONG_ASCII -> out.longAscii(longValue);
                case NULL -> out.nullValue();
            }
        }
    }

    private enum Kind {
        BYTES,
        LONG_ASCII,
        NULL
    }
}
