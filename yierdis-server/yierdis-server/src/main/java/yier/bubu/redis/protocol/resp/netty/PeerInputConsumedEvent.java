package yier.bubu.redis.protocol.resp.netty;

/**
 * decoder 在对端半关闭后已处理完缓冲区（完整命令已 handoff、不完整帧已丢弃）时发出。
 * 下游据此停止接收新回复槽位，并在已登记回复全部写出后关闭连接。
 */
public final class PeerInputConsumedEvent {
    public static final PeerInputConsumedEvent INSTANCE = new PeerInputConsumedEvent();

    private PeerInputConsumedEvent() {
    }
}
