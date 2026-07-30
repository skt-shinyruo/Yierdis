package yier.bubu.redis.command.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;

public class CommandSyntaxTest {
    @Test
    public void arityValidatesAndEmitsMetadataFromOneObject() throws Exception {
        CommandArity exact = CommandArity.exact(2);
        Assert.assertEquals(2, exact.redisMetadataArity());
        Assert.assertThrows(CommandParseException.class,
                () -> exact.validate("get", CommandArgs.of(request("GET"))));
        exact.validate("get", CommandArgs.of(request("GET", "k")));

        CommandArity oneOf = CommandArity.oneOf(2, 4);
        Assert.assertEquals(-2, oneOf.redisMetadataArity());
        oneOf.validate("bitcount", CommandArgs.of(request("BITCOUNT", "k")));
        oneOf.validate("bitcount", CommandArgs.of(request("BITCOUNT", "k", "0", "1")));
    }

    @Test
    public void registrationTakesOnlySpecs() throws Exception {
        Assert.assertNotNull(CommandModule.Registration.class.getMethod(
                "register", CommandSpec.class));
        java.util.List<Class<?>> registrationTypes = new java.util.ArrayList<>();
        for (java.lang.reflect.Method method : CommandModule.Registration.class.getMethods()) {
            if (method.getName().equals("register")) {
                Assert.assertEquals(1, method.getParameterCount());
                registrationTypes.add(method.getParameterTypes()[0]);
            }
        }
        Assert.assertEquals(java.util.List.of(CommandSpec.class), registrationTypes);
    }

    private static ByteArrayExecutionRequest request(String... args) {
        return ByteArrayExecutionRequest.fromUtf8(args[0], java.util.List.of(args).subList(1, args.length));
    }
}
