# jedis-utils
Redis based utils like DistributedLock, AmazonConnectionPool and etc. Each util is implemented via Jedis library. There is 'Redisson' library that has implementation of distributed locks, but it is not compatible with Jedis and use its own connection pools. This makes it hard to use in projects which already use Jedis library. Another point to use jedis-utils is that source code is much simple and readable than in Redisson.

