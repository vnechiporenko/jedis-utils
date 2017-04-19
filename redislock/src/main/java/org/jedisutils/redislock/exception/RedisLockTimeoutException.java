package org.jedisutils.redislock.exception;

/**
 * Timeout exception which is throw is lock was not acquired during timeout
 */
public class RedisLockTimeoutException extends RedisLockException {
  
  public RedisLockTimeoutException(String key) {
    super(key, "Redis lock acquisition timeout. Key: " + key);
  }
  
}
