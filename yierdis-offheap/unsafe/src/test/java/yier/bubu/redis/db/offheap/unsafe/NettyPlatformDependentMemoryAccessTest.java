package yier.bubu.redis.db.offheap.unsafe;

import org.junit.Assert;
import org.junit.Test;

public class NettyPlatformDependentMemoryAccessTest {
    @Test
    public void allocatePutGetAndCopyWork() {
        long a1 = 0;
        long a2 = 0;
        try {
            a1 = NettyPlatformDependentMemoryAccess.allocateMemory(16);
            a2 = NettyPlatformDependentMemoryAccess.allocateMemory(16);
            Assert.assertTrue(a1 != 0);
            Assert.assertTrue(a2 != 0);

            byte[] src = new byte[16];
            for (int i = 0; i < src.length; i++) {
                src[i] = (byte) (i + 1);
            }

            NettyPlatformDependentMemoryAccess.copyMemory(src, 0, a1, src.length);
            NettyPlatformDependentMemoryAccess.copyMemory(a1, a2, src.length);

            byte[] dst = new byte[16];
            NettyPlatformDependentMemoryAccess.copyMemory(a2, dst, 0, dst.length);
            Assert.assertArrayEquals(src, dst);

            // Spot-check put/get.
            NettyPlatformDependentMemoryAccess.putByte(a1 + 3, (byte) 99);
            Assert.assertEquals((byte) 99, NettyPlatformDependentMemoryAccess.getByte(a1 + 3));
        } finally {
            if (a1 != 0) {
                NettyPlatformDependentMemoryAccess.freeMemory(a1);
            }
            if (a2 != 0) {
                NettyPlatformDependentMemoryAccess.freeMemory(a2);
            }
        }
    }
}

