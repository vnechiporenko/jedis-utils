package org.jedisutils.redislock.exception;

/**
 * Base class for library custom exceptions
 */
public abstract class RedisLockException extends RuntimeException {
  
  private final String key;
  
  RedisLockException(String key, String message) {
    super(message);
    this.key = key;
  }

  /**
   * Redis lock key
   * @return 
   */
  public String getKey() {
    return key;
  }    
    
}
