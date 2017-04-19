package org.jedisutils.redislock.exception;

/**
 * Exception which is throw if Redis lock manager 
 * was not started before getting remote lock
 */
public class RedisLockManagerNotReadyException extends RedisLockException {

  public RedisLockManagerNotReadyException(String key) {
    super(key, "Redis lock manager was not started");
  }
  
}
