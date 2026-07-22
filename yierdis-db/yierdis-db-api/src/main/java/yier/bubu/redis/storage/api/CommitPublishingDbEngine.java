package yier.bubu.redis.storage.api;

public interface CommitPublishingDbEngine extends RuntimeDbEngine {
    void attachCommitPublisher(DbCommitPublisher publisher, int dbIndex);
}
