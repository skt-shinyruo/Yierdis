package yier.bubu.redis.app.bench;

import yier.bubu.redis.app.bench.redis.RedisBenchmarkCommand;
import yier.bubu.redis.app.bench.storage.StorageBenchmarkCommand;

import java.io.PrintWriter;
import java.util.Arrays;

public final class YierdisBench {
    public static void main(String[] args) {
        int exitCode = execute(
                args,
                new PrintWriter(System.out, true),
                new PrintWriter(System.err, true)
        );
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    /** 路由：第一个位置参数 "storage" 选 storage benchmark，其余全部交给 redis benchmark。 */
    public static int execute(String[] args, PrintWriter out, PrintWriter err) {
        if (args.length > 0 && args[0].equals("storage")) {
            return new StorageBenchmarkCommand().run(Arrays.copyOfRange(args, 1, args.length), out, err);
        }
        return new RedisBenchmarkCommand().run(args, out, err);
    }

    private YierdisBench() {
    }
}
