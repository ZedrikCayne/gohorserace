Welcome to Go Horse Race. Redis in Java.

Using https://github.com/tair-opensource/compatibility-test-suite-for-redis
we're getting close to having something which looks 'compatible'

```
Summary: version: 1.0.0, total tests: 50, passed: 50, rate: 100.00%
Summary: version: 1.0.1, total tests: 2, passed: 2, rate: 100.00%
Summary: version: 1.0.5, total tests: 2, passed: 2, rate: 100.00%
Summary: version: 1.2.0, total tests: 14, passed: 14, rate: 100.00%
Summary: version: 2.0.0, total tests: 32, passed: 32, rate: 100.00%
Summary: version: 2.2.0, total tests: 15, passed: 15, rate: 100.00%
Summary: version: 2.4.0, total tests: 8, passed: 8, rate: 100.00%
Summary: version: 2.6.0, total tests: 15, passed: 13, rate: 86.67%
Summary: version: 2.6.12, total tests: 2, passed: 2, rate: 100.00%
Summary: version: 2.8.0, total tests: 10, passed: 7, rate: 70.00%
Summary: version: 2.8.7, total tests: 1, passed: 1, rate: 100.00%
Summary: version: 2.8.9, total tests: 9, passed: 6, rate: 66.67%
Summary: version: 3.0.0, total tests: 1, passed: 0, rate: 0.00%
Summary: version: 3.0.2, total tests: 1, passed: 1, rate: 100.00%
Summary: version: 3.2.0, total tests: 22, passed: 3, rate: 13.64%
Summary: version: 3.2.1, total tests: 1, passed: 1, rate: 100.00%
Summary: version: 3.2.10, total tests: 4, passed: 0, rate: 0.00%
Summary: version: 4.0.0, total tests: 7, passed: 7, rate: 100.00%
Summary: version: 5.0.0, total tests: 24, passed: 4, rate: 16.67%
Summary: version: 6.0.0, total tests: 8, passed: 6, rate: 75.00%
Summary: version: 6.0.6, total tests: 5, passed: 5, rate: 100.00%
Summary: version: 6.2.0, total tests: 62, passed: 44, rate: 70.97%
Summary: version: 7.0.0, total tests: 55, passed: 25, rate: 45.45%
Summary: version: 7.2.0, total tests: 2, passed: 2, rate: 100.00%
```

Most of what is missing involves streams, geo, and blocking sorted set commands and lua functions. (eval and evalsha work)

Don't use this in production, but if you need a redis to run on windows and don't want to run the WSL this should do it for you.

Implemented so far: (Note, some of the 'implemented' are more useful error messages saying we are never actually going to support this...)

ACL
APPEND
ASKING
AUTH
BGREWRITEAOF
BGSAVE
BITCOUNT
BITFIELD
BITFIELDRO
BITOP
BITPOS
BLMOVE
BLMPOP
BLPOP
BRPOP
BRPOPLPUSH
CLIENT
CLIENTSETINFO
COPY
DBSIZE
DECR
DECRBY
DEL
DISCARD
ECHO
EVAL
EVALRO
EVALSHA
EVALSHARO
EXEC
EXISTS
EXPIRE
EXPIREAT
EXPIRETIME
FLUSHALL
FLUSHDB
GET
GETBIT
GETDEL
GETEX
GETRANGE
GETSET
HDEL
HELLO
HEXISTS
HGET
HGETALL
HINCRBY
HINCRBYFLOAT
HKEYS
HLEN
HMGET
HRANDFIELD
HSCAN
HSET
HSETNX
HSTRLEN
HVALS
INCR
INCRBY
INCRBYFLOAT
KEYS
LINDEX
LINSERT
LLEN
LMOVE
LMPOP
LOLWUT
LPOP
LPOS
LPUSH
LPUSHX
LRANGE
LREM
LSET
LTRIM
MGET
MONITOR
MOVE
MSET
MSETNX
MULTI
PERSIST
PEXPIRE
PEXPIREAT
PEXPIRETIME
PING
PSETEX
PSUBSCRIBE
PTTL
PUBLISH
PUBSUB
PUNSUBSCRIBE
QUIT
RANDOMKEY
RENAME
RENAMENX
RESET
RPOP
RPOPLPUSH
RPUSH
RPUSHX
SADD
SAVE
SCAN
SCARD
SCRIPT
SDIFF
SDIFFSTORE
SELECT
SET
SETBIT
SETEX
SETNX
SETRANGE
SHUTDOWN
SINTERCARD
SINTER
SINTERSTORE
SISMEMBER
SMISMEMBER
SMOVE
SORT
SPOP
SRANDMEMBER
SREM
SSCAN
STRLEN
SUBSCRIBE
SUNION
SUNIONSTORE
SWAPDB
TIME
TOUCH
TTL
UNLINK
UNSUBSCRIBE
UNWATCH
WATCH
ZADD
ZCARD
ZCOUNT
ZDIFF
ZDIFFSTORE
ZINCRBY
ZINTERCARD
ZINTER
ZINTERSTORE
ZLEXCOUNT
ZMPOP
ZMSCORE
ZPOPMAX
ZPOPMIN
ZRANDMEMBER
ZRANGE
ZRANGEBYLEX
ZRANGEBYSCORE
ZRANGESTORE
ZRANK
ZREM
ZREMRANGEBYLEX
ZREMRANGEBYRANK
ZREMRANGEBYSCORE
ZREVRANGE
ZREVRANGEBYLEX
ZREVRANGEBYSCORE
ZREVRANK
ZSCAN
ZSCORE
ZUNION
ZUNIONSTORE

Couple caveats: I wasn't particularily careful about keeping 100% binary all the way through on some commands, so if you are storing stuff other than text you might get something unexpected.

It doesn't send the same error messages as a real redis server. The only bits of source I actually took from the real redis repo are the json files that describe the commands, and I used that to build the implementation classes.
There are vestigial bits in there about key parser commands (that I see why they exist now that I put in transactions, which I won't guarantee will work exactly the same as a real redis..but it is 'close enough')

Good luck out there.
