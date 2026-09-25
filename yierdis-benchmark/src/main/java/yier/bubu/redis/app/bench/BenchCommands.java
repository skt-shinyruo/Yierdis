package yier.bubu.redis.app.bench;

import java.io.PrintWriter;

public final class BenchCommands {
    /** 参数错误（usage error）的退出码：只打一行原因到 err，不打 usage。 */
    public static final int USAGE_ERROR = 2;

    private BenchCommands() {
    }

    public static void writeOutput(PrintWriter out, String rendered) {
        out.print(rendered);
        out.flush();
        if (out.checkError()) {
            throw new IllegalStateException("failed to write benchmark output");
        }
    }
}
