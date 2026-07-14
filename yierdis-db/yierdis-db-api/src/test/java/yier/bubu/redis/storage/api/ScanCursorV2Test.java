package yier.bubu.redis.storage.api;

import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class ScanCursorV2Test {
    @Test
    public void cursorRoundTripsGenerationPhaseAndPosition() {
        ScanCursorV2 cursor = ScanCursorV2.of(12345, 1, 0xfedcba98L);

        ScanCursorV2 parsed = ScanCursorV2.of(Long.parseLong(new String(cursor.toBulkStringAscii(), StandardCharsets.US_ASCII)));

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
        Assert.assertThrows(IllegalArgumentException.class, () -> ScanCursorV2.of(2L << 32));
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
