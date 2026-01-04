package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 单线程命令执行器：将 Netty I/O 线程与命令执行解耦，保持 Redis 风格“单线程命令语义”。
 * <p>
 * - I/O 线程负责解码与投递；执行器线程负责访问 DB、执行命令并写回响应。
 * - 采用有界队列作为背压机制，避免积压导致 OOM。
 */
public final class CommandExecutor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CommandExecutor.class);

    private static final long POLL_TIMEOUT_MILLIS = 50;

    private final YierdisDb db;
    private final YierdisFastCommandProcessor commandProcessor;
    private final BlockingQueue<Task> queue;
    private final CountDownLatch started = new CountDownLatch(1);
    private final Thread thread;

    private volatile boolean running = true;

    public CommandExecutor(YierdisDb db, YierdisFastCommandProcessor commandProcessor, int queueCapacity) {
        this.db = Objects.requireNonNull(db, "db");
        this.commandProcessor = Objects.requireNonNull(commandProcessor, "commandProcessor");
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be > 0");
        }
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.thread = new Thread(this::runLoop, "yierdis-command-executor");
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
        try {
            started.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while starting CommandExecutor", e);
        }
    }

    /**
     * 尝试投递一个客户端命令。
     * <p>
     * 成功：执行器接管 {@link RespCommand} 的生命周期并负责 recycle。
     * 失败：调用方仍拥有 {@link RespCommand}，必须自行 recycle。
     */
    public boolean trySubmit(ChannelHandlerContext ctx, RespCommand cmd) {
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(cmd, "cmd");
        if (!running) {
            return false;
        }
        return queue.offer(Task.command(ctx, cmd));
    }

    /**
     * 投递维护任务（例如定期过期清理），在执行器线程上运行。
     */
    public boolean trySubmitMaintenance(Runnable task) {
        Objects.requireNonNull(task, "task");
        if (!running) {
            return false;
        }
        return queue.offer(Task.maintenance(task));
    }

    private void runLoop() {
        try {
            db.bindToCurrentThread();
        } catch (Throwable t) {
            log.error("Failed to bind DB to executor thread", t);
            throw t;
        } finally {
            started.countDown();
        }

        while (running || !queue.isEmpty()) {
            Task task;
            try {
                task = queue.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // 允许 shutdown 通过 interrupt 加速退出。
                continue;
            }
            if (task == null) {
                continue;
            }

            if (task.maintenance != null) {
                try {
                    task.maintenance.run();
                } catch (Throwable t) {
                    log.debug("Maintenance task failed", t);
                }
                continue;
            }

            executeCommand(task.ctx, task.cmd);
        }

        // 退出时兜底：回收队列中残留的命令，防止 retainedSlice 泄漏。
        Task leftover;
        while ((leftover = queue.poll()) != null) {
            if (leftover.cmd != null) {
                leftover.cmd.recycle();
            }
        }
    }

    private void executeCommand(ChannelHandlerContext ctx, RespCommand cmd) {
        ByteBuf out = ctx.alloc().buffer();
        try {
            RespWriter writer = new RespWriter(out);
            commandProcessor.execute(cmd, writer);
            ctx.writeAndFlush(out);
            out = null;
        } catch (Throwable t) {
            // 兜底：避免执行器线程异常导致连接挂死；尽量返回统一错误。
            try {
                if (out != null) {
                    new RespWriter(out).error("ERR internal error");
                    ctx.writeAndFlush(out);
                    out = null;
                }
            } catch (Throwable ignored) {
                // ignore
            }
            log.debug("Command execution failed", t);
        } finally {
            cmd.recycle();
            if (out != null) {
                out.release();
            }
        }
    }

    @Override
    public void close() {
        running = false;
        thread.interrupt();
        try {
            thread.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // 即使 join 超时，也要尽量回收队列内命令。
            Task leftover;
            while ((leftover = queue.poll()) != null) {
                if (leftover.cmd != null) {
                    leftover.cmd.recycle();
                }
            }
        }
    }

    private static final class Task {
        private final ChannelHandlerContext ctx;
        private final RespCommand cmd;
        private final Runnable maintenance;

        private Task(ChannelHandlerContext ctx, RespCommand cmd, Runnable maintenance) {
            this.ctx = ctx;
            this.cmd = cmd;
            this.maintenance = maintenance;
        }

        static Task command(ChannelHandlerContext ctx, RespCommand cmd) {
            return new Task(ctx, cmd, null);
        }

        static Task maintenance(Runnable task) {
            return new Task(null, null, task);
        }
    }
}
