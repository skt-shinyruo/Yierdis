/**
 * 日志基础设施。
 * <p>
 * 调用点用 Lombok {@code @Slf4j} 生成 logger（默认字段名 {@code log}），日志配置在
 * {@code src/main/resources/logback.xml}。这个包只放配置本身表达不了、又必须由代码保证的东西：
 * 目前只有一个 {@link yier.bubu.redis.logging.StatusErrorsToStderr}——把 Logback 自己的配置期错误
 * 打到 stderr，同时阻止它把整份 status 全量 dump 到 stdout。
 */
package yier.bubu.redis.logging;
