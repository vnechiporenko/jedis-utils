package org.jedisutils.redislock;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.ldap.ManageReferralControl;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

/**
 * Listener of Redis channel for lock released events
 */
class RedisLockChannelListener extends JedisPubSub implements Runnable {
  
  private final AtomicBoolean stopped = new AtomicBoolean();
  private final JedisPool pool;
  private final String channelName;
  private final RedisLockManager redisLockManager;
  private final long RECONNECT_TIMEOUT = 2000;
  private final Thread listnerThread;
  private static Logger log = Logger.getLogger(RedisLockChannelListener.class.getName());  
  
  RedisLockChannelListener(RedisLockManager redisLockManager, JedisPool pool, String channelName) {
    this.redisLockManager = redisLockManager;
    this.pool = pool;
    this.channelName = channelName;  
    listnerThread = new Thread(this);
    listnerThread.setDaemon(true);
    listnerThread.setName("distribute-lock-channel-listener");
    listnerThread.start();
  }
  
  /**
   * Listener thread main loop
   */
  @Override
  public void run() {
    boolean failed = false;
    while (!stopped.get() && !Thread.interrupted()) {
      try {
        try (Jedis jedis = pool.getResource()) {          
          // notify all lock handlers after reconnection
          redisLockManager.notifyAllLocks();
          jedis.subscribe(this, channelName);
        }
        // Timeout before reconnect attempt
        Thread.sleep(RECONNECT_TIMEOUT); 
        failed = false;
      } catch (InterruptedException ex) {
        break;
      } catch (Exception ex) { 
        if (!failed) {
          failed = true;
          log.log(Level.WARNING, "Failed to subscribe to channel " + channelName, ex);
        }
        try {
          // Timeout before reconnect attempt
          Thread.sleep(RECONNECT_TIMEOUT); 
        } catch (InterruptedException ex1) {
          break;
        }
      }
    }
  }
  

  @Override
  public void onMessage(String channel, String message) {
    if (stopped.get())
      return;
    redisLockManager.notifyLock(message);
  }

  @Override
  public void onPMessage(String pattern, String channel, String message) {
    onMessage(channel, message);
  }
  
  /**
   * Stops listener thread
   */
  public void stop() {
    stopped.set(true);
    if (listnerThread != null && listnerThread.isAlive()) {
      try {
        unsubscribe();
      } catch (Exception ex) {
        log.log(Level.FINE, "Failed to unsubscribe from channel " + channelName, ex);
      }
      listnerThread.interrupt();      
    }
  }
  
  
}
