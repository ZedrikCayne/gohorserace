Welcome to Go Horse Race. Redis in Java.

Not everything is implemented. This will respond to all available 7.2 commands, but spits errors.

The implementations of commands are in server.commands.impl

Under 'Command<>Command' (Don't judge)

Don't use this in production, but if you need a redis to run on windows and don't want to run the WSL this should do it for you.

Running the script in src/implemented.sh gives you the implemented list.

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
MSET
MSETNX
MULTI
PERSIST
PEXPIRE
PEXPIREAT
PEXPIRETIME
PING
PSUBSCRIBE
PTTL
PUBLISH
PUBSUB
PUNSUBSCRIBE
QUIT
RANDOMKEY
RENAME
RENAMENX
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
SPOP
SRANDMEMBER
SREM
SSCAN
STRLEN
SUBSCRIBE
SUNION
SUNIONSTORE
TIME
TTL
UNLINK
UNSUBSCRIBE
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

Currently passes redis-benchmark on default settings. And at least on my test laptop running either a local redis or the go horse race....it's comparable.

Is it fast? Not really. But it works.

Couple caveats: I wasn't particularily careful about keeping 100% binary all the way through on some commands, so if you are storing stuff other than text you might get something unexpected.

I patched up everything my current application is using (Storing zipped binaries with hget/hset in lua). Fixing it up shouldn't be a terrible stretch.

Pull requests? Sure. Will I actually check this often? Probably not. Put this together to solve a particular problem and it is 'good enough'.

Is the code any good? Not really. It's a mish mash of whatever I felt like doing at the time and whatever seemed to be the right way to do stuff.
It isn't consistent with itself.

It doesn't send the same error messages as a real redis server. The only bits of source I actually took from the real redis repo are the json files that describe the commands, and I used that to build the implementation classes.
There are vestigial bits in there about key parser commands (that I see why they exist now that I put in transactions, which I won't guarantee will work exactly the same as a real redis..but it is 'close enough')

Good luck out there.
