package yier.bubu.redis.protocol.resp.netty;

/**
 * 解码器对手工读取节流的最小控制面，避免把传输对象放入请求租约中。
 */
public interface InboundReadControl {
    InboundReadControl NOOP = new InboundReadControl() {
        @Override
        public void pauseIngress() {
        }

        @Override
        public void resumeIngress() {
        }
    };

    void pauseIngress();

    void resumeIngress();

    default void resumeIngressForProgress() {
        resumeIngress();
    }
}
