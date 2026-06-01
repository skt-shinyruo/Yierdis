/**
 * {@code yierdis-db-memory} 拥有的 in-memory 存储实现。
 *
 * <p>该包同时维护 key directory、TTL index、entry table、value root 与 maxmemory 账本。
 * 命令、协议、executor、server 等上层模块应依赖 {@code yierdis-db-api}，不要直接导入这些实现类型；
 * 这样才能把 native handle、内存预算和过期清理语义封在存储模块内部。</p>
 */
package yier.bubu.redis.storage.memory;
