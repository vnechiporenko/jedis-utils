package org.jedisutils.redislock;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jedisutils.redislock.exception.RedisLockTimeoutException;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class MultiThreadTest extends AbstractRedisLockTest {
  
  private ScheduledThreadPoolExecutor executor;
  
  @Override
  public void init() {
    super.init();
    executor = new ScheduledThreadPoolExecutor(10);
  }
  
  @Override
  public void shutdown() {
    if (!executor.isShutdown()) {
      executor.shutdown();
    }
    super.shutdown();
  }
  
  @Test
  public void checkSimultaneousLock() throws Throwable {
    final long holdTime = 1500;
    
    ScheduledFuture<Throwable> f1 = executor.schedule(() -> {
      try (RedisLock l = lockManager.lock(LOCK_KEY_1, 5)) {
        Thread.sleep(holdTime);        
        assertLockOwning(l);
        return null;
      } catch (Throwable ex) {
        return ex;
      }
      
    }, 0, TimeUnit.MILLISECONDS);
    
    ScheduledFuture<Throwable> f2 = executor.schedule(() -> {
      long ts = System.currentTimeMillis();
      try (RedisLock l = lockManager.lock(LOCK_KEY_1, 5)) {
        long waitTime = System.currentTimeMillis() - ts;
        Assert.assertFalse("Invalid lock wait time: " + waitTime, waitTime < holdTime - 200 || waitTime > Math.round(1.1 * holdTime));
        assertLockOwning(l);        
        return null;
      } catch (Throwable ex) {
        return ex;
      }
    }, 100, TimeUnit.MILLISECONDS);
       
    Thread.sleep(200);
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
    
    if (f1.get() != null) {
      throw f1.get();
    }
    if (f2.get() != null) {
      throw f2.get();
    }
    
  }
  
  @Test(expected=RedisLockTimeoutException.class)
  public void checkTimeout() throws Throwable {
    final long holdTime = 2000;
    
    ScheduledFuture<Throwable> f1 = executor.schedule(() -> {
      try (RedisLock l = lockManager.lock(LOCK_KEY_1, 5)) {
        Thread.sleep(holdTime);        
        assertLockOwning(l);
        return null;
      } catch (Throwable ex) {
        return ex;
      }
      
    }, 0, TimeUnit.MILLISECONDS);
    
    ScheduledFuture<Throwable> f2 = executor.schedule(() -> {
      try (RedisLock l = lockManager.lock(LOCK_KEY_1, 1)) {       
        return null;
      } catch (Throwable ex) {
        return ex;
      }
    }, 100, TimeUnit.MILLISECONDS);
       
    Thread.sleep(200);
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
    
    if (f1.get() != null) {
      throw f1.get();
    }
    if (f2.get() != null) {
      throw f2.get();
    }
    
  }  
  
  protected void assertLockOwning(RedisLock l) {
    Assert.assertEquals("Invalid hold count for locked state", 1, l.getHoldCount());
    Assert.assertTrue("Invalid held by current thread", l.isHeldByCurrentThread());
    try (Jedis jedis = jedisPool.getResource()) {
      Assert.assertTrue("Lock key doesn't exist for locked state", jedis.exists(LOCK_KEY_1));
      String value = jedis.get(LOCK_KEY_1);
      Assert.assertEquals("Invalid lock key value", value, lockManager.getLockValue(l));
    }    
  }
       
  
  
}
