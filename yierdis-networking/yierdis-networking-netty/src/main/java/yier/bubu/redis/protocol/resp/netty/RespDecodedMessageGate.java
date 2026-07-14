package yier.bubu.redis.protocol.resp.netty;

import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

/**
 * 在 RESP 解码完成但尚未向下游传播前执行的连接本地准入边界。
 */
public interface RespDecodedMessageGate {
    RespDecodedMessageGate PASS_THROUGH = (ctx, decoded, resumeOnEventLoop) -> Admission.admitted(decoded);

    Admission tryAdmit(ChannelHandlerContext ctx, Object decoded, Runnable resumeOnEventLoop);

    enum Status {
        ADMITTED,
        WAITING,
        CLOSED
    }

    record Admission(Status status, Object forwardedMessage) {
        public Admission {
            Objects.requireNonNull(status, "status");
            if (status == Status.ADMITTED) {
                Objects.requireNonNull(forwardedMessage, "forwardedMessage");
            }
        }

        public static Admission admitted(Object forwardedMessage) {
            return new Admission(Status.ADMITTED, Objects.requireNonNull(forwardedMessage, "forwardedMessage"));
        }

        public static Admission waiting() {
            return new Admission(Status.WAITING, null);
        }

        public static Admission closed() {
            return new Admission(Status.CLOSED, null);
        }
    }
}
