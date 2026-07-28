package yier.bubu.redis.command.defaults;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.execution.api.DbIndexSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.storage.api.DbEngine;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;

public class CommandSupportFastPathTest {
    private static final YierdisDbRouter TEST_ROUTER = new YierdisDbRouter() {
        @Override
        public DbEngine dbFor(DbIndexSession session) {
            return null;
        }

        @Override
        public int databases() {
            return 1;
        }
    };

    @Test
    public void utf8PrefersReadOnlyFastPathWhenAvailable() {
        ExecutionRequest request = fastPathAwareRequest(ascii("PING"));

        Assert.assertEquals("PING", CommandSupport.utf8(ArgReader.of(request).bytes(0)));
    }

    @Test
    public void parseLongPrefersReadOnlyFastPathWhenAvailable() {
        ExecutionRequest request = fastPathAwareRequest(ascii("12345"));

        Assert.assertEquals(12345L, ArgReader.of(request).longAt(0));
    }

    @Test
    public void sliceResetFromRequestPrefersReadOnlyFastPathWhenAvailable() {
        byte[] first = ascii("k1");
        byte[] second = ascii("k2");
        CommandSupport support = new CommandSupport(TEST_ROUTER, null);
        ExecutionRequest request = fastPathAwareRequest(ascii("DEL"), first, second);

        support.sliceResetFromRequest(request, 1, 2);

        Assert.assertSame(first, support.slice().get(0));
        Assert.assertSame(second, support.slice().get(1));
    }

    private static ExecutionRequest fastPathAwareRequest(byte[]... argv) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                return handleObjectMethod(proxy, method.getName(), args);
            }
            return switch (method.getName()) {
                case "argc" -> argv.length;
                case "isNull" -> argv[(int) args[0]] == null;
                case "len" -> {
                    byte[] arg = argv[(int) args[0]];
                    yield arg == null ? -1 : arg.length;
                }
                case "byteAt" -> argv[(int) args[0]][(int) args[1]];
                case "copyToByteArray" -> {
                    byte[] arg = argv[(int) args[0]];
                    System.arraycopy(arg, 0, (byte[]) args[1], (int) args[2], arg.length);
                    yield null;
                }
                case "toByteArray" -> throw new AssertionError("unexpected defensive copy");
                case "readOnlyByteArray" -> argv[(int) args[0]];
                case "retainedBytes" -> retainedBytes(argv);
                case "close" -> null;
                default -> throw new UnsupportedOperationException("unexpected method: " + method.getName());
            };
        };
        return (ExecutionRequest) Proxy.newProxyInstance(
                ExecutionRequest.class.getClassLoader(),
                new Class[]{ExecutionRequest.class},
                handler
        );
    }

    private static Object handleObjectMethod(Object proxy, String name, Object[] args) {
        if ("toString".equals(name)) {
            return proxy.getClass().getName();
        }
        if ("hashCode".equals(name)) {
            return System.identityHashCode(proxy);
        }
        if ("equals".equals(name)) {
            return proxy == args[0];
        }
        throw new UnsupportedOperationException("unexpected Object method: " + name);
    }

    private static int retainedBytes(byte[][] argv) {
        int retained = 0;
        for (byte[] arg : argv) {
            retained += arg == null ? 0 : arg.length;
        }
        return retained;
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
