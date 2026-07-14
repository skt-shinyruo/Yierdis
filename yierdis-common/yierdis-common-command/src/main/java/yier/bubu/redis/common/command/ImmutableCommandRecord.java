package yier.bubu.redis.common.command;

/**
 * 由调用方显式拥有的不可变命令记录。
 */
public interface ImmutableCommandRecord extends CommandRecordView, AutoCloseable {
    ImmutableCommandRecord retain();

    @Override
    void close();
}
