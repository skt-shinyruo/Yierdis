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
            Assert.assertTrue(policy.contains("yierdis-command-core:"));
            Assert.assertTrue(policy.contains("yierdis-command-builtin:"));
            Assert.assertTrue(policy.contains("yierdis-server-executor:"));
            Assert.assertTrue(policy.contains("yierdis-db-api:"));
            Assert.assertTrue(policy.contains("yierdis-db-memory:"));
            Assert.assertTrue(policy.contains("yierdis-db-testkit:"));
            Assert.assertTrue(policy.contains("yierdis-server-runtime-api:"));
            Assert.assertTrue(policy.contains("yierdis-server-core:"));
            Assert.assertTrue(policy.contains("yierdis-server-runtime:"));
            Assert.assertTrue(policy.contains("yierdis-server-main:"));
            Assert.assertTrue(policy.contains("yierdis-networking-custom-v1:"));
            Assert.assertTrue(policy.contains("yierdis-networking-custom-v1-execution:"));
            Assert.assertTrue(policy.contains("yierdis-networking-netty:"));
            Assert.assertTrue(policy.contains("forbidden_imports:"));
            Assert.assertTrue(policy.contains("yier.bubu.redis.protocol.custom.v1.reply"));
            Assert.assertTrue(policy.contains("application_composition_root"));
            Assert.assertTrue(policy.contains("no_storage_internal_imports"));
        }
    }

    @Test
    public void architecturePolicyResourceDocumentsTargetPackageOwnership() throws Exception {
        try (InputStream in = ArchitecturePolicyResourceTest.class.getResourceAsStream("/architecture-policy.yml")) {
            Assert.assertNotNull("missing architecture-policy.yml", in);
            String policy = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            Assert.assertTrue(policy.contains("target_packages:"));
            Assert.assertTrue(policy.contains("yier.bubu.redis.app.server"));
            Assert.assertTrue(policy.contains("yier.bubu.redis.execution.api"));
            Assert.assertTrue(policy.contains("yier.bubu.redis.command.kernel"));
            Assert.assertTrue(policy.contains("yier.bubu.redis.storage.memory"));
            Assert.assertTrue(policy.contains("retired_from_active_source_tree:"));
            Assert.assertFalse(policy.contains("legacy_allowed_during_migration:"));
        }
    }
}
