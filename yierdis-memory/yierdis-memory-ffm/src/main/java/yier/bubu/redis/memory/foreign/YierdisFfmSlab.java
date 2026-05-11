package yier.bubu.redis.memory.foreign;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

record YierdisFfmSlab(
        YierdisFfmRegion region,
        int size,
        ArrayList<Block> freeBlocks,
        AtomicBoolean closed
) {
    YierdisFfmSlab {
        Objects.requireNonNull(region, "region");
        Objects.requireNonNull(freeBlocks, "freeBlocks");
        Objects.requireNonNull(closed, "closed");
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
    }

    YierdisFfmSlab(YierdisFfmMemoryRuntime runtime, int size) {
        this(runtime.allocateRegion("slab", size), size, new ArrayList<>(), new AtomicBoolean(false));
        freeBlocks.add(new Block(0, size));
    }

    synchronized Allocation allocate(int bytes) {
        ensureOpen();
        for (int i = 0; i < freeBlocks.size(); i++) {
            Block block = freeBlocks.get(i);
            if (block.length() < bytes) {
                continue;
            }

            int offset = block.offset();
            int remaining = block.length() - bytes;
            if (remaining == 0) {
                freeBlocks.remove(i);
            } else {
                freeBlocks.set(i, new Block(offset + bytes, remaining));
            }
            return new Allocation(offset, bytes);
        }
        return null;
    }

    synchronized void free(int offset, int bytes) {
        if (closed.get()) {
            return;
        }
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        freeBlocks.add(new Block(offset, bytes));
        freeBlocks.sort(Comparator.comparingInt(Block::offset));
        coalesce();
    }

    synchronized YierdisFfmSpan span(int offset, int length) {
        ensureOpen();
        return region.span(offset, length);
    }

    synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        region.close();
        freeBlocks.clear();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("slab is closed");
        }
        region.ensureOpen();
    }

    private void coalesce() {
        if (freeBlocks.isEmpty()) {
            return;
        }
        ArrayList<Block> merged = new ArrayList<>(freeBlocks.size());
        Block current = freeBlocks.get(0);
        for (int i = 1; i < freeBlocks.size(); i++) {
            Block next = freeBlocks.get(i);
            if (current.offset() + current.length() == next.offset()) {
                current = new Block(current.offset(), current.length() + next.length());
            } else {
                merged.add(current);
                current = next;
            }
        }
        merged.add(current);
        freeBlocks.clear();
        freeBlocks.addAll(merged);
    }

    record Allocation(int offset, int length) {
        Allocation {
            if (offset < 0) {
                throw new IllegalArgumentException("offset must be >= 0");
            }
            if (length <= 0) {
                throw new IllegalArgumentException("length must be > 0");
            }
        }
    }

    private record Block(int offset, int length) {
        Block {
            if (offset < 0) {
                throw new IllegalArgumentException("offset must be >= 0");
            }
            if (length <= 0) {
                throw new IllegalArgumentException("length must be > 0");
            }
        }
    }
}
