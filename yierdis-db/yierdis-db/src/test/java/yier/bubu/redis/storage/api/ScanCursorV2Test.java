package yier.bubu.redis.storage.api;

import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class ScanCursorV2Test {
    @Test
    public void cursorRoundTripsGenerationPhaseAndPosition() {
        ScanCursorV2 cursor = ScanCursorV2.of(12345, 1, 0xfedcba98L);

        ScanCursorV2 parsed = ScanCursorV2.of(Long.parseLong(new String(cursor.toAsciiBytes(), StandardCharsets.US_ASCII)));

        Assert.assertEquals(12345, parsed.generation());
        Assert.assertEquals(1, parsed.phase());
        Assert.assertEquals(0xfedcba98L, parsed.position());
    }

    @Test
    public void cursorRejectsValuesOutsideTheWireLayout() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ScanCursorV2.of(-1, 0, 0));
        Assert.assertThrows(IllegalArgumentException.class, () -> ScanCursorV2.of(0x20000000, 0, 0));
        Assert.assertThrows(IllegalArgumentException.class, () -> ScanCursorV2.of(0, 2, 0));
        Assert.assertThrows(IllegalArgumentException.class, () -> ScanCursorV2.of(0, 0, 0x1_0000_0000L));
        Assert.assertThrows(IllegalArgumentException.class, () -> ScanCursorV2.of(-1L));
    }

    @Test
    public void wireParsingAcceptsAnyNonNegativeCursorAsOpaque() {
        // 客户端可把 cursor 当不透明整数乱填：phase 位超出内部使用的 0/1 时也必须可解析，
        // 是否重启迭代由存储层决定，解析本身不得拒绝。
        for (long value : new long[]{1L, 2L << 32, 3L << 32, Long.MAX_VALUE}) {
            ScanCursorV2 parsed = ScanCursorV2.of(value);

            Assert.assertEquals(value, parsed.value());
            Assert.assertArrayEquals(
                    Long.toString(value).getBytes(StandardCharsets.US_ASCII),
                    parsed.toAsciiBytes()
            );
        }
        Assert.assertEquals(2, ScanCursorV2.of(2L << 32).phase());
        Assert.assertEquals(0L, ScanCursorV2.of(0L).value());
    }

    @Test
    public void wireGenerationRepeatsAfterTheDocumentedHorizon() {
        long firstGeneration = 17L;
        long repeatedGeneration = firstGeneration + (1L << 29);

        ScanCursorV2 first = ScanCursorV2.of((int) firstGeneration, 1, 91L);
        ScanCursorV2 repeated = ScanCursorV2.of((int) (repeatedGeneration & 0x1fff_ffffL), 1, 91L);

        Assert.assertEquals(first.value(), repeated.value());
    }
}
