package yier.bubu.redis.command.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;

public class CommandSyntaxTest {
    @Test
    public void arityValidatesAndEmitsMetadataFromOneObject() {
        CommandArity exact = CommandArity.exact(2);
        Assert.assertEquals(2, exact.redisMetadataArity());
        Assert.assertNull(exact.validate("get", ArgReader.of(request("GET", "k"))));
        Assert.assertNotNull(exact.validate("get", ArgReader.of(request("GET"))));

        CommandArity oneOf = CommandArity.oneOf(2, 4);
        Assert.assertEquals(-2, oneOf.redisMetadataArity());
        Assert.assertNull(oneOf.validate("bitcount", ArgReader.of(request("BITCOUNT", "k"))));
        Assert.assertNull(oneOf.validate("bitcount", ArgReader.of(request("BITCOUNT", "k", "0", "1"))));
    }

    @Test
    public void registrationTakesOneSpecWithoutASeparateName() throws Exception {
        Assert.assertNotNull(CommandModule.Registration.class.getMethod(
                "register", CommandSpec.class));
        for (java.lang.reflect.Method method : CommandModule.Registration.class.getMethods()) {
            if (method.getName().equals("register")) {
                Assert.assertArrayEquals(new Class<?>[]{CommandSpec.class}, method.getParameterTypes());
            }
        }
    }

    private static ByteArrayExecutionRequest request(String... args) {
        return ByteArrayExecutionRequest.fromUtf8(args[0], java.util.List.of(args).subList(1, args.length));
    }
}
