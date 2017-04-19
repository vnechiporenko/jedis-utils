package org.jedisutils.redislock;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.jedisutils.redislock.RedisLockManager;
import org.junit.After;
import org.junit.Before;
import redis.clients.jedis.JedisPool;

public abstract class AbstractRedisLockTest {
  
  protected JedisPool jedisPool;
  protected RedisLockManager lockManager;
  protected final String LOCK_KEY_1 = "lock_key1";
  protected final String LOCK_KEY_2 = "lock_key2"; 
  protected final String NODE_1 = "node1"; 
  protected final String NODE_2 = "node2"; 
  protected final String UNLOCK_CHANNEL_NAME = "test_unlock_channel";
  
  @Before
  public void init() {
    jedisPool = createPool();
    lockManager = new RedisLockManager(jedisPool, UNLOCK_CHANNEL_NAME, NODE_1);
    lockManager.start();
  }
  
  @After
  public void shutdown() {
    lockManager.shutdown();
    jedisPool.close();
    jedisPool = null;
  }
  
  protected JedisPool createPool() {
    String redisHostName = System.getenv("REDIS");
    if (redisHostName == null || redisHostName.trim().isEmpty()) {
      redisHostName = "localhost";
    }    
    
    return new JedisPool(redisHostName);    
  }
}
