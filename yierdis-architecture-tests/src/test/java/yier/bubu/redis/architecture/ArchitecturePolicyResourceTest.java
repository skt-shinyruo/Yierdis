package yier.bubu.redis.architecture;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ArchitecturePolicyResourceTest {
    @Test
    public void architecturePolicyResourceNamesCurrentBoundaryRules() throws Exception {
        try (InputStream in = ArchitecturePolicyResourceTest.class.getResourceAsStream("/architecture-policy.yml")) {
            Assert.assertNotNull("missing architecture-policy.yml", in);
            String policy = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            Assert.assertTrue(policy.contains("yierdis-command-api:"));
            Assert.assertTrue(policy.contains("yierdis-command-kernel:"));
            Assert.assertTrue(policy.contains("yierdis-command-defaults:"));
            Assert.assertTrue(policy.contains("yierdis-executor-core:"));
            Assert.assertTrue(policy.contains("yierdis-storage-api:"));
            Assert.assertTrue(policy.contains("yierdis-storage-memory:"));
            Assert.assertTrue(policy.contains("yierdis-storage-testkit:"));
            Assert.assertTrue(policy.contains("yierdis-runtime-api:"));
            Assert.assertTrue(policy.contains("yierdis-engine:"));
            Assert.assertTrue(policy.contains("yierdis-runtime-embedded:"));
            Assert.assertTrue(policy.contains("yierdis-server-app:"));
            Assert.assertTrue(policy.contains("yierdis-custom-v1-wire:"));
            Assert.assertTrue(policy.contains("yierdis-custom-v1-execution-adapter:"));
            Assert.assertTrue(policy.contains("yierdis-custom-v1-netty:"));
            Assert.assertTrue(policy.contains("forbidden_imports:"));
            Assert.assertTrue(policy.contains("yier.bubu.redis.protocol.reply"));
            Assert.assertTrue(policy.contains("application_composition_root"));
            Assert.assertTrue(policy.contains("no_storage_internal_imports"));
        }
    }
}
