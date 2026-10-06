package yier.bubu.redis.command.defaults.admin;

import java.util.Objects;
import java.util.function.Function;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.storage.api.DbAccountingReconciliation;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.RuntimeDbEngine;

/**
 * 管理命令。{@code YDRECONCILE} 对当前选中的 DB 做账本对账，成功时解除 degraded 并回复 {@code +OK}。
 * <p>
 * 它不经过 mutation executor，因此写入准入门控拦不住它；失败时回复 {@code -ERR} 且 degraded 保持不变。
 */
public final class AdminCommands {
    private static final String RECONCILE_FAILED = "ERR reconciliation failed";

    private final CommandSupport support;

    public AdminCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(
                new CommandSyntax(
                        "YDRECONCILE",
                        CommandArity.exact(1),
                        CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE
                ),
                this::reconcile
        ));
    }

    private Function<CommandSession, PreparedCommand> reconcile(CommandArgs args) {
        return session -> CommandSupport.preparedAction(
                ReplyShapes.simpleString("OK"),
                this::reconcileSelectedDb
        );
    }

    private CommandResult reconcileSelectedDb(CommandSession session) {
        DbEngine db = support.commandDb(session);
        // 分发层只暴露 DbEngine，对账入口在 RuntimeDbEngine 上；stock engine 都实现该接口。
        // 失败结果留在 health 快照里。协议回复收成 -ERR，degraded 保持；成功才是 +OK。
        DbAccountingReconciliation reconciliation = ((RuntimeDbEngine) db).reconcileAccounting();
        if (!reconciliation.succeeded()) {
            return CommandResult.controlError(RECONCILE_FAILED);
        }
        return CommandResult.reply(RedisReplies.simpleString("OK"));
    }
}
