package org.jedisutils.redislock;

import org.jedisutils.redislock.exception.RedisLockManagerNotReadyException;
import org.junit.Test;

public class RedisLockManagerState extends AbstractRedisLockTest {
  
  @Test(expected=RedisLockManagerNotReadyException.class)
  public void checkManagerNotReady() throws InterruptedException {
    RedisLockManager lockManager2 = new RedisLockManager(jedisPool, UNLOCK_CHANNEL_NAME, NODE_2);
    try (RedisLock l = lockManager2.getLock(NODE_1)) {      
    }
  }

  @Test(expected=RedisLockManagerNotReadyException.class)
  public void checkManagerNotReady2() throws InterruptedException {
    RedisLockManager lockManager2 = new RedisLockManager(jedisPool, UNLOCK_CHANNEL_NAME, NODE_2);
    try (RedisLock l = lockManager2.lock(NODE_1, 4)) {      
    }
  }
  
}
