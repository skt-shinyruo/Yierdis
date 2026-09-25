package yier.bubu.redis.logging;

import ch.qos.logback.core.spi.ContextAwareBase;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusListener;

/**
 * 把 Logback 自己的 <em>配置期</em> 错误打到 stderr。
 * <p>
 * 为什么需要它：Logback 内部有个 status 通道，配置期出错（目录不可写、appender 起不来、definer 报错）
 * 默认只记在内存里，不打任何东西——表现就是"服务照常起来，但日志从此静默消失"。实测：不挂监听器时，
 * FILE appender 打不开是 <b>0 行</b> 输出。
 * <p>
 * 为什么不用 Logback 自带的两个：
 * <ul>
 *   <li>{@code OnConsoleStatusListener} 写 <b>stdout</b>——stdout 在本项目要保持干净（CLI 复用同一进程）。</li>
 *   <li>{@code OnErrorConsoleStatusListener} 名字有误导：它只是换到 stderr，并<b>不</b>过滤级别，
 *       实测正常启动也会打 18 行 INFO。</li>
 * </ul>
 * 这里只放行 {@link Status#ERROR}：正常启动零噪音，配置出错必有输出。
 */
public final class StatusErrorsToStderr extends ContextAwareBase implements StatusListener {

    @Override
    public void addStatusEvent(Status status) {
        try {
            if (status.getLevel() != Status.ERROR) {
                return;
            }
            System.err.println("[logback] ERROR in " + status.getOrigin() + " - " + status.getMessage());
            if (status.getThrowable() != null) {
                System.err.println("[logback]   caused by " + status.getThrowable());
            }
        } catch (Throwable ignored) {
            // 监听器自己的异常绝不能反过来把 Logback 的配置流程搞挂。
        }
    }
}
