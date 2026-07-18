package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;

import java.util.OptionalLong;
import java.util.Set;

public class BenchmarkConfigTest {
    @Test
    public void invalidWorkloadBoundsAreRejected() {
        Assert.assertThrows(IllegalArgumentException.class, () ->
                new BenchmarkConfig("", 0, 0, 0, 0, 0, OptionalLong.empty(), true,
                        Set.of(), 5, 1L, BenchmarkFormat.HUMAN, "", "", -1));
    }

    @Test
    public void normalizesHostAndTestSelection() {
        BenchmarkConfig config = new BenchmarkConfig(
                " 127.0.0.1 ", 16378, 100, 4, 3, 1, OptionalLong.empty(), true,
                Set.of(" SET ", "get", "GET", "  "), 3, 7L, BenchmarkFormat.HUMAN,
                "", "", 0
        );

        Assert.assertEquals("127.0.0.1", config.host());
        Assert.assertEquals(Set.of("set", "get"), config.tests());
    }

    @Test
    public void nullTestSelectionAndCredentialsBecomeEmpty() {
        BenchmarkConfig config = new BenchmarkConfig(
                "127.0.0.1", 16378, 100, 4, 3, 1, OptionalLong.empty(), true,
                null, 3, 7L, BenchmarkFormat.HUMAN, null, null, 0
        );

        Assert.assertEquals(Set.of(), config.tests());
        Assert.assertEquals("", config.username());
        Assert.assertEquals("", config.password());
    }

    @Test
    public void requiredReferencesRejectNull() {
        Assert.assertThrows(NullPointerException.class, () ->
                config(null, 16378, 100, 4, 3, 1, OptionalLong.empty(), 3, "", "", 0));
        Assert.assertThrows(NullPointerException.class, () ->
                config("127.0.0.1", 16378, 100, 4, 3, 1, null, 3, "", "", 0));
        Assert.assertThrows(NullPointerException.class, () ->
                new BenchmarkConfig(
                        "127.0.0.1", 16378, 100, 4, 3, 1, OptionalLong.empty(), true,
                        Set.of(), 3, 7L, null, "", "", 0
                ));
    }

    @Test
    public void eachValidationRuleRejectsItsInvalidEdge() {
        assertInvalid(() -> config(" ", 16378, 100, 4, 3, 1, OptionalLong.empty(), 3, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 0, 100, 4, 3, 1, OptionalLong.empty(), 3, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 65536, 100, 4, 3, 1, OptionalLong.empty(), 3, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 16378, 0, 4, 3, 1, OptionalLong.empty(), 3, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 16378, 100, 0, 3, 1, OptionalLong.empty(), 3, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 16378, 100, 4, 0, 1, OptionalLong.empty(), 3, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 16378, 100, 4, 1_073_741_825, 1,
                OptionalLong.empty(), 3, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 16378, 100, 4, 3, 0, OptionalLong.empty(), 3, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 16378, 100, 4, 3, 1, OptionalLong.of(-1), 3, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 16378, 100, 4, 3, 1, OptionalLong.empty(), -1, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 16378, 100, 4, 3, 1, OptionalLong.empty(), 5, "", "", 0));
        assertInvalid(() -> config("127.0.0.1", 16378, 100, 4, 3, 1, OptionalLong.empty(), 3, "", "", -1));
        assertInvalid(() -> config("127.0.0.1", 16378, 100, 4, 3, 1, OptionalLong.empty(), 3, "acl-user", "", 0));
    }

    @Test
    public void inclusiveBoundsAreAccepted() {
        BenchmarkConfig minimums = config(
                "localhost", 1, 1, 1, 1, 1, OptionalLong.of(0), 0, "", "", 0
        );
        BenchmarkConfig maximums = config(
                "localhost", 65535, 1, 1, 1_073_741_824, 1, OptionalLong.empty(), 4,
                "acl-user", "secret", 0
        );

        Assert.assertEquals(1, minimums.port());
        Assert.assertEquals(0L, minimums.keyspace().orElseThrow());
        Assert.assertEquals(65535, maximums.port());
        Assert.assertEquals(1_073_741_824, maximums.dataSize());
    }

    private static BenchmarkConfig config(
            String host,
            int port,
            int requests,
            int clients,
            int dataSize,
            int pipeline,
            OptionalLong keyspace,
            int precision,
            String username,
            String password,
            int database
    ) {
        return new BenchmarkConfig(
                host, port, requests, clients, dataSize, pipeline, keyspace, true,
                Set.of(), precision, 7L, BenchmarkFormat.HUMAN, username, password, database
        );
    }

    private static void assertInvalid(Runnable constructorCall) {
        Assert.assertThrows(IllegalArgumentException.class, constructorCall::run);
    }
}
