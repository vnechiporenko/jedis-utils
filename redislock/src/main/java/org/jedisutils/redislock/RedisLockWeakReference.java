package org.jedisutils.redislock;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * Weak references which is used for locks map in RedisLockManager
 */
class RedisLockWeakReference extends WeakReference<RedisLock> {
  
  private final String key;
  
  RedisLockWeakReference(RedisLock referent, ReferenceQueue<? super RedisLock> q) {
    super(referent, q);
    this.key = referent.getKey();
  }
  
  public String getKey() {
    return key;
  }
}
