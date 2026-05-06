package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ForeignMemoryAutoModulesTest {
    @Test
    public void ensureFfmAvailableMethodExistsAndPassesOnJdk25() throws Exception {
        Method method = ForeignMemoryAutoModules.class.getDeclaredMethod("ensureFfmAvailable");
        method.setAccessible(true);
        try {
            method.invoke(null);
        } catch (InvocationTargetException e) {
            throw unwrap(e);
        }
    }

    private static Exception unwrap(InvocationTargetException e) throws Exception {
        Throwable cause = e.getCause();
        if (cause instanceof Exception ex) {
            throw ex;
        }
        throw new RuntimeException(cause);
    }
}
