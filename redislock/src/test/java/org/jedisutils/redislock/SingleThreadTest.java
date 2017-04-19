package org.jedisutils.redislock;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class SingleThreadTest extends AbstractRedisLockTest {
    
  @Test
  public void checkNotLocked() throws Exception {
    try (RedisLock l = lockManager.getLock(LOCK_KEY_1)) {
      Assert.assertEquals("Invalid hold count for unlocked state", 0, l.getHoldCount());
      Assert.assertEquals("Invalid lock key", LOCK_KEY_1, l.getKey());
      Assert.assertFalse("Invalid held by current thread", l.isHeldByCurrentThread());
      try (Jedis jedis = jedisPool.getResource()) {
        Assert.assertFalse("Lock key exists for unlocked state", jedis.exists(LOCK_KEY_1));
      }
    }
  }
  
  @Test
  public void checkLocked() throws Exception {
    try (RedisLock l = lockManager.getLock(LOCK_KEY_1)) {
      l.lock(2);
      Assert.assertEquals("Invalid hold count for locked state", 1, l.getHoldCount());
      Assert.assertTrue("Invalid held by current thread", l.isHeldByCurrentThread());
      try (Jedis jedis = jedisPool.getResource()) {
        Assert.assertTrue("Lock key doesn't exist for locked state", jedis.exists(LOCK_KEY_1));
        String value = jedis.get(LOCK_KEY_1);
        Assert.assertEquals("Invalid lock key value", value, lockManager.getLockValue(l));
        long ttl = jedis.ttl(LOCK_KEY_1);        
        Assert.assertTrue("Invalid lock key ttl", ttl > 0 && ttl <= 2);
      }
    }    
  }
  
  @Test 
  public void checkUnlocked() throws Exception {
    try (RedisLock l = lockManager.getLock(LOCK_KEY_1)) {
      l.lock(2);
      l.unlock(false);
      Assert.assertEquals("Invalid hold count for unlocked state", 0, l.getHoldCount());
      Assert.assertEquals("Invalid lock key", LOCK_KEY_1, l.getKey());
      Assert.assertFalse("Invalid held by current thread", l.isHeldByCurrentThread());
      try (Jedis jedis = jedisPool.getResource()) {
        Assert.assertFalse("Lock key exists for unlocked state", jedis.exists(LOCK_KEY_1));
      }
      
    }    
  }
  
  @Test 
  public void checkClosed() throws Exception {
    RedisLock lock = null;
    try (RedisLock l = lockManager.lock(LOCK_KEY_1, 2)) {
      lock = l;
    }
    Assert.assertEquals("Invalid hold count for closed state", 0, lock.getHoldCount());
    Assert.assertFalse("Invalid held by current thread", lock.isHeldByCurrentThread());
    try (Jedis jedis = jedisPool.getResource()) {
      Assert.assertFalse("Lock key exists for closed state", jedis.exists(LOCK_KEY_1));
    }
  }

  @Test
  public void checkNestedLock() throws Exception {
    RedisLock lock = null;
    try (RedisLock l = lockManager.getLock(LOCK_KEY_1)) {
      l.lock(1);
      lock = l;
      try (RedisLock l2 = lockManager.lock(LOCK_KEY_1, 4)) {
        Assert.assertTrue("RedisLock pointers are not same for same key", l == l2);
        Assert.assertEquals("Invalid hold count for inner lock state", 2, l.getHoldCount());
        Assert.assertTrue("Invalid held by current thread", l.isHeldByCurrentThread());
        try (Jedis jedis = jedisPool.getResource()) {
          Assert.assertTrue("Lock key doesn't exist for inner locked state", jedis.exists(LOCK_KEY_1));
          String value = jedis.get(LOCK_KEY_1);
          Assert.assertEquals("Invalid lock key value", value, lockManager.getLockValue(l));
          long ttl = jedis.ttl(LOCK_KEY_1);        
          Assert.assertTrue("Invalid lock key ttl", ttl > 2 && ttl <= 4);
        }        
      }
      
      Assert.assertEquals("Invalid hold count after closing of inner lock", 1, l.getHoldCount());
      Assert.assertTrue("Invalid held by current thread after closing of inner lock", l.isHeldByCurrentThread());
      try (Jedis jedis = jedisPool.getResource()) {
        Assert.assertTrue("Lock key doesn't exist  after closing of inner lock", jedis.exists(LOCK_KEY_1));
        String value = jedis.get(LOCK_KEY_1);
        Assert.assertEquals("Invalid lock key value  after closing of inner lock", value, lockManager.getLockValue(l));
        long ttl = jedis.ttl(LOCK_KEY_1);        
        Assert.assertTrue("Invalid lock key ttl after closing of inner lock", ttl > 2 && ttl <= 4);
      }      
      
    }   
    Assert.assertEquals("Invalid hold count for closed state", 0, lock.getHoldCount());
    Assert.assertFalse("Invalid held by current thread", lock.isHeldByCurrentThread());
    try (Jedis jedis = jedisPool.getResource()) {
      Assert.assertFalse("Lock key exists for closed state", jedis.exists(LOCK_KEY_1));
    }    
    
  }
  
  @Test(expected = IllegalMonitorStateException.class)
  public void checkUnlockForNonLocked() throws InterruptedException {
    try (RedisLock l = lockManager.getLock(LOCK_KEY_1)) {
      l.unlock(false);
    }    
  }

  @Test(expected = IllegalMonitorStateException.class)
  public void checkUnlockTwice() throws InterruptedException {
    try (RedisLock l = lockManager.lock(LOCK_KEY_1, 1)) {
      l.unlock(false);
      l.unlock(false);
    }    
  }
  
  @Test
  public void checkExpire() throws InterruptedException {
    try (RedisLock l = lockManager.lock(LOCK_KEY_1, 1)) {
      l.expire(5);
      Assert.assertEquals("Invalid hold count after expire call", 1, l.getHoldCount());
      Assert.assertTrue("Invalid held by current thread after expire call", l.isHeldByCurrentThread());      
      try (Jedis jedis = jedisPool.getResource()) {
        Assert.assertTrue("Lock key doesn't exist after expire call", jedis.exists(LOCK_KEY_1));
        String value = jedis.get(LOCK_KEY_1);
        Assert.assertEquals("Invalid lock key value after expire call", value, lockManager.getLockValue(l));
        long ttl = jedis.ttl(LOCK_KEY_1);        
        Assert.assertTrue("Invalid lock key ttl after expire call", ttl > 2 && ttl <= 5);
      }         
    }        
  }
  
  @Test(expected = IllegalMonitorStateException.class)
  public void checkExpireForNonLocked() throws InterruptedException {
    try (RedisLock l = lockManager.getLock(LOCK_KEY_1)) {
      l.expire(5);
    }    
  }
  
  
}
