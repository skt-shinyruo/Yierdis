package yier.bubu.redis.client;

// CLI 输出回归：覆盖 RESP3 扩展类型的打印稳定性，避免 future changes 引入不可读/异常行为。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespAttribute;
import yier.bubu.redis.protocol.RespBigNumber;
import yier.bubu.redis.protocol.RespBlobError;
import yier.bubu.redis.protocol.RespBoolean;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespDouble;
import yier.bubu.redis.protocol.RespMap;
import yier.bubu.redis.protocol.RespNull;
import yier.bubu.redis.protocol.RespSet;
import yier.bubu.redis.protocol.RespVerbatimString;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class YierdisCliResp3PrintTest {
    @Test
    public void printResp3ExtendedTypesDoesNotThrowAndHasStableMarkers() {
        String out = captureStdout(() -> {
            YierdisCli.printResp(RespBoolean.of(true), false);
            YierdisCli.printResp(RespDouble.of(3.14), false);
            YierdisCli.printResp(RespBigNumber.of("123456789012345678901234567890"), false);
            YierdisCli.printResp(RespSet.of(List.of(
                    RespBulkString.ofString("a"),
                    RespBulkString.ofString("b")
            )), false);

            RespMap attrs = RespMap.of(List.of(new RespMap.Entry(
                    RespBulkString.ofString("meta"),
                    RespBulkString.ofString("x")
            )));
            YierdisCli.printResp(RespAttribute.of(attrs, RespBulkString.ofString("ok")), false);

            YierdisCli.printResp(RespVerbatimString.ofBytes("txt", "hello".getBytes(StandardCharsets.UTF_8)), false);
            YierdisCli.printResp(RespBlobError.ofBytes("ERR blob".getBytes(StandardCharsets.UTF_8)), false);
            YierdisCli.printResp(RespNull.INSTANCE, false);
        });

        Assert.assertTrue(out.contains("(true)"));
        Assert.assertTrue(out.contains("(double) 3.14"));
        Assert.assertTrue(out.contains("(big number) 123456789012345678901234567890"));
        Assert.assertTrue(out.contains("1) a"));

        Assert.assertTrue(out.contains("(attributes)"));
        Assert.assertTrue(out.contains("meta"));
        Assert.assertTrue(out.contains("x"));
        Assert.assertTrue(out.contains("ok"));

        Assert.assertTrue(out.contains("txt:hello"));
        Assert.assertTrue(out.contains("(error) ERR blob"));
        Assert.assertTrue(out.contains("(nil)"));
    }

    private static String captureStdout(Runnable task) {
        PrintStream oldOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(baos, true, StandardCharsets.UTF_8));
            task.run();
        } finally {
            System.setOut(oldOut);
        }
        return baos.toString(StandardCharsets.UTF_8);
    }
}

