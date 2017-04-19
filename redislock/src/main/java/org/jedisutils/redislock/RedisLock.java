package org.jedisutils.redislock;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.jedisutils.redislock.exception.RedisLockTimeoutException;

/**
 * Distribute lock for multi-process synchronization based on Redis. 
 * 
 * Please, make sure to use it in try-finally-resource blocks
 */
public class RedisLock implements AutoCloseable {
  
  // local lock which is used to reduce number of request to Redis
  private final ReentrantLock localLock = new ReentrantLock();  
  // redis key which is used for synchronization
  private final String key;
  // creator of lock which holds Redis connection pool
  private final RedisLockManager manager;
  // unique ID of the lock for single process
  private final long lockId;
  // singal flag which should be set up by channel listener
  private volatile boolean signal = false;
  
  RedisLock(String key, long lockId, RedisLockManager manager) {
    this.key = key;
    this.lockId = lockId;
    this.manager = manager;
  }

  /**
   * Acquire lock. Lock key TTL is equal to wait timeout.
   * @param timeoutSec timeout in seconds
   * @throws RedisLockTimeoutException if timeout is off
   * @throws InterruptedException 
   */
  public void lock(int timeoutSec) throws InterruptedException, RedisLockTimeoutException {
    lock(timeoutSec, timeoutSec);
  }
  
  /**
   * Acquire lock
   * @param timeoutSec timeout in seconds
   * @param ttlSec TTL in seconds for Redis key
   * @throws RedisLockTimeoutException if timeout is off
   * @throws InterruptedException 
   */
  public void lock(int timeoutSec, int ttlSec) throws InterruptedException, RedisLockTimeoutException {
    assert(timeoutSec >= 0);
    assert(ttlSec > 0);
    if (!tryLock(timeoutSec, ttlSec)) {
      throw new RedisLockTimeoutException(key);
    }    
  }
  
  /**
   * Attempt to acquire lock. Lock key TTL is equal to wait timeout.
   * @param timeoutSec timeout in seconds
   * @return true if current thread owns the lock, false otherwise
   * @throws InterruptedException 
   */
  public boolean tryLock(int timeoutSec) throws InterruptedException {
    return tryLock(timeoutSec, timeoutSec);
  }
  
  /**
   * Attempt to acquire lock. 
   * @param timeoutSec timeout in seconds
   * @param ttlSec TTL in seconds for Redis key
   * @return true if current thread owns the lock, false otherwise
   * @throws InterruptedException 
   */  
  public boolean tryLock(int timeoutSec, int ttlSec) throws InterruptedException {
    long endTs = System.currentTimeMillis() + timeoutSec * 1000L;
    
    // Acquire local lock first
    if (!localLock.tryLock(timeoutSec, TimeUnit.SECONDS)) {
      return false;
    }
    
    // In case of nested critical section for same lock we don't need to
    // acquire remote lock twice
    if (localLock.getHoldCount() == 1) {
      try {
        // acquire remote lock attempts loop
        boolean remoteLockAcquired = false;
        while (!remoteLockAcquired) {
          
          // reset signal flag before attempt
          synchronized (this) {
            signal = false; 
          }
          
          // attemp to acquire remote lock
          remoteLockAcquired = manager.acquireRemoteLock(this, ttlSec);
          if (remoteLockAcquired) {
            break;
          }
          
          // check for timeout
          long timeout = endTs - System.currentTimeMillis();
          if (timeout <= 0) {
            localLock.unlock();
            return false;
          }
          
          // wait for signal that remote lock is released
          synchronized (this) {
            if (!signal) {
              wait(timeout);
            }
          }
          
        }
        return true;
      } catch (RuntimeException | Error ex) {
        // free local lock in case of any error
        localLock.unlock();
        throw ex;
      }
    } else {
      try {
        // just prolongate remote lock TTL
        manager.prolongateRemoteLock(this, ttlSec);
      } catch (RuntimeException | Error ex) {
        // free local lock in case of any error
        localLock.unlock();
        throw ex;
      }
      return true;
    }
    
  }
  
  /**
   * Update redis lock key TTL
   * @param ttlSec TTL in seconds for Redis key
   * @throws IllegalMonitorStateException if current thread doesn't own the lock
   */
  public void expire(int ttlSec) throws IllegalMonitorStateException {
    if (!localLock.isHeldByCurrentThread()) {
      throw new IllegalMonitorStateException();      
    }
    manager.prolongateRemoteLock(this, ttlSec);
  }
  
  public int getHoldCount() {
    return localLock.getHoldCount();
  }
  
  public boolean isHeldByCurrentThread() {
    return localLock.isHeldByCurrentThread();
  }
  
  
  /**
   * Release lock
   * @param ignoreRemoteError whenever or not release local lock in case of any errors in releasing of remote lock
   * @throws InterruptedException 
   * @throws IllegalMonitorStateException if current thread doesn't own the lock
   */
  public void unlock(boolean ignoreRemoteError) throws InterruptedException {
    if (!localLock.isHeldByCurrentThread()) {
      throw new IllegalMonitorStateException();      
    }    
    // unlock remote lock first
    try {
      if (localLock.getHoldCount() == 1) {
        manager.releaseRemoteLock(this);
      }
    } catch (RuntimeException | Error ex) {
      if (!ignoreRemoteError) {
        throw ex;
      }
    } catch (InterruptedException ex) {
      if (!ignoreRemoteError) {
        throw ex;
      } 
      Thread.currentThread().interrupt();
    }
    // unlock local lock
    localLock.unlock();
  }
  
  /**
   * Redis lock key
   * @return 
   */
  public String getKey() {
    return key;
  }
  
  long getId() {
    return lockId;
  }

  /**
   * Releases lock if it was locked
   * @throws InterruptedException 
   */
  @Override
  public void close() throws InterruptedException {
    if (localLock.isHeldByCurrentThread()) {
      unlock(true);
    }
  }
  
  synchronized void sendWakeUpSignal() {
    signal = true; 
    notifyAll();
  }
  
}
