### redis cluster proxy

#### 说明
基于redis cluster-3.x开发的proxy，目前支持大部分指令，其他指令后续完善开发中


#### 安装方式
```
cd nginx-1.10.1
./configure --with-stream --add-module=ngx_stream_redis3_proxy/
make && make install

```


#### 使用方式

##### nginx.conf配置
```
user  nobody;
worker_processes  1;

#daemon off;
#master_process off;

error_log  logs/error.log debug;
#error_log  logs/error.log  notice;
#error_log  logs/error.log  info;

#pid        logs/nginx.pid;

events {
    worker_connections  1024;
}


stream {

upstream backend {
    server 127.0.0.1:8000;
    server 127.0.0.1:8001;
    server 127.0.0.1:8002;
    server 127.0.0.1:8003;
    server 127.0.0.1:8004;
    server 127.0.0.1:8005;
    redis_cluster;
}



server {
    listen 8015;
    redis_proxy_pass backend;
}

```

##### 测试

```
redis-cli -p 8015 set abc 123
OK

redis-cli -p 8015 get abc
"123"
```



#### 支持的指令
- PING
- QUIT
- EXISTS
- PERSIST
- PTTL
- TTL
- TYPE
- DUMP
- DECR
- GET
- INCR
- STRLEN
- HGETALL
- HKEYS
- HLEN
- HVALS
- LLEN
- LPOP
- RPOP
- SCARD
- SMEMBERS
- SPOP
- ZCARD
- PFCOUNT
- AUTH
- EXPIRE
- EXPIREAT
- PEXPIRE
- PEXPIREAT
- APPEND
- DECRBY
- GETBIT
- GETSET
- INCRBY
- INCRBYFLOAT
- SETNX
- HEXISTS
- HGET
- LINDEX
- LPUSHX
- RPOPLPUSH
- RPUSHX
- SISMEMBER
- ZRANK
- ZREVRANK
- ZSCORE
- GETRANGE
- PSETEX
- SETBIT
- SETEX
- SETRANGE
- HINCRBY
- HINCRBYFLOAT
- HSET
- HSETNX
- LRANGE
- LREM
- LSET
- LTRIM
- SMOVE
- ZCOUNT
- ZLEXCOUNT
- ZINCRBY
- ZREMRANGEBYLEX
- ZREMRANGEBYRANK
- ZREMRANGEBYSCORE
- LINSERT
- SORT
- BITCOUNT
- BITPOS
- SET
- HDEL
- HMGET
- HMSET
- HSCAN
- LPUSH
- RPUSH
- SADD
- SDIFF
- SDIFFSTORE
- SINTER
- SINTERSTORE
- SREM
- SUNION
- SUNIONSTORE
- SRANDMEMBER
- SSCAN
- PFADD
- PFMERGE
- ZADD
- ZINTERSTORE
- ZRANGE
- ZRANGEBYSCORE
- ZREM
- ZREVRANGE
- ZRANGEBYLEX
- ZREVRANGEBYSCORE
- ZUNIONSTORE
- ZSCAN


#### todo列表
- 批量接口
- lua脚本
- 多个upstream怎么合并buffer，然后在发送给用户
- 动态upstream问题待跟进

