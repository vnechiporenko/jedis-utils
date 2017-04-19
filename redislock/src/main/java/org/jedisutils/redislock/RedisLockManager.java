package org.jedisutils.redislock;

import java.lang.ref.ReferenceQueue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.jedisutils.redislock.exception.RedisLockManagerNotReadyException;
import org.jedisutils.redislock.exception.RedisLockTimeoutException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

/**
 * Manages redis locks. Provider API for getting locks
 */
public class RedisLockManager {

  private static Logger log = Logger.getLogger(RedisLockManager.class.getName());
  
  // Weak values map for all created RedisLock objects
  private final ConcurrentHashMap<String, RedisLockWeakReference> locks = new ConcurrentHashMap<>();  
  // Next unique ID for lock for single proccess
  private final AtomicLong lockNextId = new AtomicLong();
  // Queue for GC evict events
  private final ReferenceQueue<RedisLock> refQueue = new ReferenceQueue<>();
  // Redis connection pool
  private final JedisPool jedisPool;
  // Unique ID of proccess
  private final String nodeId;
  // Maximum release remote lock attempts
  private int maxReleaseAttempts = 5;
  // Timeout between release remote lock attemps
  private long releaseAttemptTimeout = 1000;
  // Listener for redis channel with unlock events
  private RedisLockChannelListener channelListener;
  // Unlock event channel name
  private final String channelName;
  // Wherever or not the manager and listener in STOPPED state
  private AtomicBoolean stopped = new AtomicBoolean(true);
  

  /**
   * RedisLockManager ctor.
   * @param jedisPool Redis connection pool
   * @param channelName name of redis channel which will be used for "lock-released" messages. 
   * The channel MUST NOT be used for other messages.
   */
  public RedisLockManager(JedisPool jedisPool, String channelName) {    
    this(jedisPool, channelName, UUID.randomUUID().toString());
  }
  
  /**
   * RedisLockManager ctor.
   * @param jedisPool Redis connection pool
   * @param channelName name of redis channel which will be used for "lock-released" messages. 
   * @param nodeId unique ID of Redis lock manager instance
   * The channel MUST NOT be used for other messages.
   */
  public RedisLockManager(JedisPool jedisPool, String channelName, String nodeId) {    
    this.jedisPool = jedisPool;
    this.channelName = channelName;
    this.nodeId = nodeId;
  }
  
  /**
   * Starts listener thread for reading messages from Redis channel. 
   * This methods MUST be called before calling of getLock, lock and other methods.
   * @throws IllegalStateException if manager was already started
   */
  @PostConstruct
  public void start() throws IllegalStateException {  
    if (!stopped.get()) {
      throw new IllegalStateException("Redis lock manager was already started");
    }
    log.log(Level.FINE, "Start redis lock manager. ID: {0}", nodeId);
    channelListener = new RedisLockChannelListener(this, jedisPool, channelName);    
    stopped.set(false);
  }
  
  /**
   * Gets distribute lock but DO NOT acquire it.
   * @param key Redis key, which will be used for distribute critical section 
   * @return RedisLock object in unlocked state for current thread
   * @throws RedisLockManagerNotReadyException if manager was not started
   */
  public RedisLock getLock(String key) throws RedisLockManagerNotReadyException {
    if (stopped.get()) {
      throw new RedisLockManagerNotReadyException(key);
    }
    // clean-up locks map first
    evictLocks();
    
    // get or create new lock atomary
    RedisLockWeakReference ref = locks.computeIfAbsent(key, this::createLock);
    RedisLock lock = ref.get();
    if (lock == null) { 
      // this usually should not happen, but theretically it is possible
      // so just call getLock again
      log.log(Level.WARNING, "RedisLock was collected by GC too fast");
      return getLock(key);
    }
    return lock;
  }
  
  /**
   * Remove from locks map all released by GC RedisLock objects
   */
  private void evictLocks() {
    RedisLockWeakReference ref;
    while ((ref = (RedisLockWeakReference)refQueue.poll()) != null) {
      locks.remove(ref.getKey(), ref);
    }
  }

  /**
   * Gets distribute lock and acquire it. 
   * TTL for lock key is equal to timeout.
   * @param key Redis key, which will be used for distribute critical section 
   * @param timeoutSec timeout in seconds for acquiring operation
   * @return RedisLock object in locked state for current thread
   * @throws RedisLockManagerNotReadyException if manager was not started
   * @throws RedisLockTimeoutException if timeout occurred
   * @throws InterruptedException
   */
  public RedisLock lock(String key, int timeoutSec) throws InterruptedException, RedisLockManagerNotReadyException, RedisLockTimeoutException {
    return lock(key, timeoutSec, timeoutSec);
  }
  
  /**
   * Gets distribute lock and acquire it.
   * @param key Redis key, which will be used for distribute critical section 
   * @param timeoutSec timeout in seconds for acquiring operation
   * @param ttlSec TTL for Redis lock key in seconds
   * @return RedisLock object in locked state for current thread
   * @throws RedisLockManagerNotReadyException if manager was not started
   * @throws RedisLockTimeoutException if timeout occurred
   * @throws InterruptedException
   */
  public RedisLock lock(String key, int timeoutSec, int ttlSec) throws InterruptedException, RedisLockManagerNotReadyException, RedisLockTimeoutException {
    RedisLock lock = getLock(key);
    lock.lock(timeoutSec, ttlSec);
    return lock;    
  }
 
  /**
   * Stop channel listener and release all locked Redis keys.
   */
  @PreDestroy
  public void shutdown() {
    if (stopped.getAndSet(true)) {
      return;
    }
    if (channelListener != null) {
      channelListener.stop();
      channelListener = null;
    }
    if (locks.isEmpty()) {
      // release all remote locks
      try (Jedis jedis = jedisPool.getResource()) {
        for (RedisLockWeakReference ref : locks.values()) {
          RedisLock lock = ref.get();
          if (lock != null) {
            String curValue = jedis.get(lock.getKey());
            if (getLockValue(lock).equals(curValue)) {
              jedis.del(lock.getKey());
            }
            jedis.publish(channelName, lock.getKey());
          }
        }
      }
    }

  }
  
  /**
   * Acquire lock on Redis key
   * @param lock remote lock 
   * @param ttl time to live for key in seconds
   * @return true if remote lock is acquired, false otherwise
   */
  boolean acquireRemoteLock(RedisLock lock, int ttl) {
    try (Jedis jedis = jedisPool.getResource()) {
      // perform SET NX EX command
      String resp = jedis.set(lock.getKey(), getLockValue(lock), "NX", "EX", ttl);
      return resp != null;
    }
  }
  
  /**
   * Delete Redis lock key if it was owned by current process
   * @param lock remote lock
   * @return true if remote lock key was removed successfully, false otherwise
   * @throws InterruptedException 
   */
  boolean releaseRemoteLock(RedisLock lock) throws InterruptedException {
    for (int i = 0; i < maxReleaseAttempts; i++) {
      boolean lastAttempt = i >= (maxReleaseAttempts - 1);
      if (tryReleaseRemoteLock(lock, lastAttempt)) {
        return true;
      }
      if (!lastAttempt) {
        Thread.sleep(releaseAttemptTimeout);
      }
    }
    return false;
  }
  
  /** 
   * Attempt to delete Redis lock key with ignoring any errors
   * @param lock remote lock
   * @param lastAttempt if this is last attempt to release remote lock
   * @return true if remote lock is not owner by current process anymore, false otherwise
   */
  private boolean tryReleaseRemoteLock(RedisLock lock, boolean lastAttempt) {
    
    try (Jedis jedis = jedisPool.getResource()) {
      String value = getLockValue(lock);
      jedis.watch(lock.getKey());
      String curValue = jedis.get(lock.getKey());
      if (value.equals(curValue)) {
        Transaction tx = jedis.multi();
        tx.del(lock.getKey());
        tx.publish(channelName, lock.getKey());
        tx.exec();
      }
      return true;
    } catch (RuntimeException ex) {
      log.log((lastAttempt) ? Level.SEVERE : Level.WARNING, "Error occurred while releasing remote lock " + lock.getKey(), ex);
      return false;
    }
  }  
  
  /** 
   * Prolongate remote lock TTL
   * @param lock remote lock
   * @param ttl TTL
   * @return true if operation success, false if current process don't own remote lock
   */
  boolean prolongateRemoteLock(RedisLock lock, int ttl) {
    try (Jedis jedis = jedisPool.getResource()) {
      String value = getLockValue(lock);
      jedis.watch(lock.getKey());      
      String curValue = jedis.get(lock.getKey());
      if (value.equals(curValue)) {
        Transaction tx = jedis.multi();
        tx.expire(lock.getKey(), ttl);
        return tx.exec() != null;
      } else {
        return false;
      }      
    }    
  }
  
  /**
   * Sends wake up signal to all remote locks
   */
  void notifyAllLocks() {
    for (RedisLockWeakReference ref : locks.values()) {
      RedisLock lock = ref.get();
      if (lock != null) {
        lock.sendWakeUpSignal();
      }
    }
  }
  
  /**
   * Sends wake up signal to remote lock
   * @param key lock key
   */
  void notifyLock(String key) {
    RedisLockWeakReference ref = locks.get(key);
    RedisLock lock = ref.get();
    if (lock != null) {
      lock.sendWakeUpSignal();
    }
  }
  
  /**
   * Creates new RedisLock
   * @param key redis key which is used for lock
   * @return weak reference to RedisLock
   */
  private RedisLockWeakReference createLock(String key) {
    RedisLock lock = new RedisLock(key, lockNextId.incrementAndGet(), this);
    return new RedisLockWeakReference(lock, refQueue);
  }
  
  /**
   * Gets value which should be stored in Redis key if current process owns the lock
   * @param lock remote lock
   * @return Redis lock key value
   */
  String getLockValue(RedisLock lock) {
    return nodeId + lock.getId();
  }

  /**
   * Maximum release remote lock attempts
   * @return number of attempts
   */
  public int getMaxReleaseAttempts() {
    return maxReleaseAttempts;
  }

  public void setMaxReleaseAttempts(int maxReleaseAttempts) {
    if (maxReleaseAttempts <= 0) {
      throw new IllegalArgumentException("maxReleaseAttempts");
    }
    this.maxReleaseAttempts = maxReleaseAttempts;
  }

  /**
   * Gets release remote lock timeout in milliseconds
   * @return timeout in milliseconds
   */
  public long getReleaseAttemptTimeout() {
    return releaseAttemptTimeout;
  }

  public void setReleaseAttemptTimeout(long releaseAttemptTimeout) {
    if (releaseAttemptTimeout <= 0) {
      throw new IllegalArgumentException("releaseAttemptTimeout");
    }    
    this.releaseAttemptTimeout = releaseAttemptTimeout;
  }
  
  String getNodeId() {
    return nodeId;
  }
  
}
