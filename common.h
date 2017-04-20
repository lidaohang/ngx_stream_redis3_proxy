#ifndef __COMMON_H__
#define __COMMON_H__

/*
#include <assert.h>
#include <stdio.h>
#include <iostream>
#include <list>
#include <stdint.h>
#include <assert.h>
#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/time.h>
#include <dlfcn.h>
#include <tr1/memory>
*/

/******************************************
    *状态
    *
******************************************/

#define REDIS_OK                                                            0
#define REDIS_ERROR                                                         -1
#define REDIS_AGAIN                                                         -2
#define REDIS_MOVED                                                         2
#define REDIS_ASK                                                           3
#define REDIS_ASKING                                                        4
#define REDIS_ASKING_END                                                    5
#define REDIS_TRYAGAIN                                                      6
#define REDIS_CLUSTER                                                       40



/******************************************
    *cluster nodes
    *
******************************************/

#define CLUSTER_NODES_ID                                                    0
#define CLUSTER_NODES_ADDR                                                  1
#define CLUSTER_NODES_ROLE                                                  2
#define CLUSTER_NODES_SID                                                   3
#define CLUSTER_NODES_CONNECTED                                             7
#define CLUSTER_NODES_LENGTH                                                8
#define CLUSTER_NODES_SLAVE_LEN                                             7




#define MSG_TYPE_CODEC(ACTION)                                                                      \
    ACTION( UNKNOWN )                                                                               \
    ACTION( REQ_MC_GET )                       /* memcache retrieval requests */                    \
    ACTION( REQ_MC_GETS )                                                                           \
    ACTION( REQ_MC_DELETE )                    /* memcache delete request */                        \
    ACTION( REQ_MC_CAS )                       /* memcache cas request and storage request */       \
    ACTION( REQ_MC_SET )                       /* memcache storage request */                       \
    ACTION( REQ_MC_ADD )                                                                            \
    ACTION( REQ_MC_REPLACE )                                                                        \
    ACTION( REQ_MC_APPEND )                                                                         \
    ACTION( REQ_MC_PREPEND )                                                                        \
    ACTION( REQ_MC_INCR )                      /* memcache arithmetic request */                    \
    ACTION( REQ_MC_DECR )                                                                           \
    ACTION( REQ_MC_TOUCH )                     /* memcache touch request */                         \
    ACTION( REQ_MC_QUIT )                      /* memcache quit request */                          \
    ACTION( RSP_MC_NUM )                       /* memcache arithmetic response */                   \
    ACTION( RSP_MC_STORED )                    /* memcache cas and storage response */              \
    ACTION( RSP_MC_NOT_STORED )                                                                     \
    ACTION( RSP_MC_EXISTS )                                                                         \
    ACTION( RSP_MC_NOT_FOUND )                                                                      \
    ACTION( RSP_MC_END )                                                                            \
    ACTION( RSP_MC_VALUE )                                                                          \
    ACTION( RSP_MC_DELETED )                   /* memcache delete response */                       \
    ACTION( RSP_MC_TOUCHED )                   /* memcache touch response */                        \
    ACTION( RSP_MC_ERROR )                     /* memcache error responses */                       \
    ACTION( RSP_MC_CLIENT_ERROR )                                                                   \
    ACTION( RSP_MC_SERVER_ERROR )                                                                   \
    ACTION( REQ_REDIS_DEL )                    /* redis commands - keys */                          \
    ACTION( REQ_REDIS_EXISTS )                                                                      \
    ACTION( REQ_REDIS_EXPIRE )                                                                      \
    ACTION( REQ_REDIS_EXPIREAT )                                                                    \
    ACTION( REQ_REDIS_PEXPIRE )                                                                     \
    ACTION( REQ_REDIS_PEXPIREAT )                                                                   \
    ACTION( REQ_REDIS_PERSIST )                                                                     \
    ACTION( REQ_REDIS_PTTL )                                                                        \
    ACTION( REQ_REDIS_SORT )                                                                        \
    ACTION( REQ_REDIS_TTL )                                                                         \
    ACTION( REQ_REDIS_TYPE )                                                                        \
    ACTION( REQ_REDIS_APPEND )                 /* redis requests - string */                        \
    ACTION( REQ_REDIS_BITCOUNT )                                                                    \
    ACTION( REQ_REDIS_BITPOS )                                                                    \
    ACTION( REQ_REDIS_DECR )                                                                        \
    ACTION( REQ_REDIS_DECRBY )                                                                      \
    ACTION( REQ_REDIS_DUMP )                                                                        \
    ACTION( REQ_REDIS_GET )                                                                         \
    ACTION( REQ_REDIS_GETBIT )                                                                      \
    ACTION( REQ_REDIS_GETRANGE )                                                                    \
    ACTION( REQ_REDIS_GETSET )                                                                      \
    ACTION( REQ_REDIS_INCR )                                                                        \
    ACTION( REQ_REDIS_INCRBY )                                                                      \
    ACTION( REQ_REDIS_INCRBYFLOAT )                                                                 \
    ACTION( REQ_REDIS_MGET )                                                                        \
    ACTION( REQ_REDIS_MSET )                                                                        \
    ACTION( REQ_REDIS_PSETEX )                                                                      \
    ACTION( REQ_REDIS_RESTORE )                                                                     \
    ACTION( REQ_REDIS_SET )                                                                         \
    ACTION( REQ_REDIS_SETBIT )                                                                      \
    ACTION( REQ_REDIS_SETEX )                                                                       \
    ACTION( REQ_REDIS_SETNX )                                                                       \
    ACTION( REQ_REDIS_SETRANGE )                                                                    \
    ACTION( REQ_REDIS_STRLEN )                                                                      \
    ACTION( REQ_REDIS_HDEL )                   /* redis requests - hashes */                        \
    ACTION( REQ_REDIS_HEXISTS )                                                                     \
    ACTION( REQ_REDIS_HGET )                                                                        \
    ACTION( REQ_REDIS_HGETALL )                                                                     \
    ACTION( REQ_REDIS_HINCRBY )                                                                     \
    ACTION( REQ_REDIS_HINCRBYFLOAT )                                                                \
    ACTION( REQ_REDIS_HKEYS )                                                                       \
    ACTION( REQ_REDIS_HLEN )                                                                        \
    ACTION( REQ_REDIS_HMGET )                                                                       \
    ACTION( REQ_REDIS_HMSET )                                                                       \
    ACTION( REQ_REDIS_HSET )                                                                        \
    ACTION( REQ_REDIS_HSETNX )                                                                      \
    ACTION( REQ_REDIS_HSCAN)                                                                        \
    ACTION( REQ_REDIS_HVALS )                                                                       \
    ACTION( REQ_REDIS_LINDEX )                 /* redis requests - lists */                         \
    ACTION( REQ_REDIS_LINSERT )                                                                     \
    ACTION( REQ_REDIS_LLEN )                                                                        \
    ACTION( REQ_REDIS_LPOP )                                                                        \
    ACTION( REQ_REDIS_LPUSH )                                                                       \
    ACTION( REQ_REDIS_LPUSHX )                                                                      \
    ACTION( REQ_REDIS_LRANGE )                                                                      \
    ACTION( REQ_REDIS_LREM )                                                                        \
    ACTION( REQ_REDIS_LSET )                                                                        \
    ACTION( REQ_REDIS_LTRIM )                                                                       \
    ACTION( REQ_REDIS_PFADD )                  /* redis requests - hyperloglog */                   \
    ACTION( REQ_REDIS_PFCOUNT )                                                                     \
    ACTION( REQ_REDIS_PFMERGE )                                                                     \
    ACTION( REQ_REDIS_RPOP )                                                                        \
    ACTION( REQ_REDIS_RPOPLPUSH )                                                                   \
    ACTION( REQ_REDIS_RPUSH )                                                                       \
    ACTION( REQ_REDIS_RPUSHX )                                                                      \
    ACTION( REQ_REDIS_SADD )                   /* redis requests - sets */                          \
    ACTION( REQ_REDIS_SCARD )                                                                       \
    ACTION( REQ_REDIS_SDIFF )                                                                       \
    ACTION( REQ_REDIS_SDIFFSTORE )                                                                  \
    ACTION( REQ_REDIS_SINTER )                                                                      \
    ACTION( REQ_REDIS_SINTERSTORE )                                                                 \
    ACTION( REQ_REDIS_SISMEMBER )                                                                   \
    ACTION( REQ_REDIS_SMEMBERS )                                                                    \
    ACTION( REQ_REDIS_SMOVE )                                                                       \
    ACTION( REQ_REDIS_SPOP )                                                                        \
    ACTION( REQ_REDIS_SRANDMEMBER )                                                                 \
    ACTION( REQ_REDIS_SREM )                                                                        \
    ACTION( REQ_REDIS_SUNION )                                                                      \
    ACTION( REQ_REDIS_SUNIONSTORE )                                                                 \
    ACTION( REQ_REDIS_SSCAN)                                                                        \
    ACTION( REQ_REDIS_ZADD )                   /* redis requests - sorted sets */                   \
    ACTION( REQ_REDIS_ZCARD )                                                                       \
    ACTION( REQ_REDIS_ZCOUNT )                                                                      \
    ACTION( REQ_REDIS_ZINCRBY )                                                                     \
    ACTION( REQ_REDIS_ZINTERSTORE )                                                                 \
    ACTION( REQ_REDIS_ZLEXCOUNT )                                                                   \
    ACTION( REQ_REDIS_ZRANGE )                                                                      \
    ACTION( REQ_REDIS_ZRANGEBYLEX )                                                                 \
    ACTION( REQ_REDIS_ZRANGEBYSCORE )                                                               \
    ACTION( REQ_REDIS_ZRANK )                                                                       \
    ACTION( REQ_REDIS_ZREM )                                                                        \
    ACTION( REQ_REDIS_ZREMRANGEBYRANK )                                                             \
    ACTION( REQ_REDIS_ZREMRANGEBYLEX )                                                              \
    ACTION( REQ_REDIS_ZREMRANGEBYSCORE )                                                            \
    ACTION( REQ_REDIS_ZREVRANGE )                                                                   \
    ACTION( REQ_REDIS_ZREVRANGEBYSCORE )                                                            \
    ACTION( REQ_REDIS_ZREVRANK )                                                                    \
    ACTION( REQ_REDIS_ZSCORE )                                                                      \
    ACTION( REQ_REDIS_ZUNIONSTORE )                                                                 \
    ACTION( REQ_REDIS_ZSCAN)                                                                        \
    ACTION( REQ_REDIS_EVAL )                   /* redis requests - eval */                          \
    ACTION( REQ_REDIS_EVALSHA )                                                                     \
    ACTION( REQ_REDIS_PING )                   /* redis requests - ping/quit */                     \
    ACTION( REQ_REDIS_QUIT)                                                                         \
    ACTION( REQ_REDIS_AUTH)                                                                         \
    ACTION( REQ_REDIS_SELECT)                  /* only during init */                               \
    ACTION( RSP_REDIS_STATUS )                 /* redis response */                                 \
    ACTION( RSP_REDIS_ERROR )                                                                       \
    ACTION( RSP_REDIS_ERROR_ERR )                                                                   \
    ACTION( RSP_REDIS_ERROR_ASK )                                                                   \
    ACTION( RSP_REDIS_ERROR_MOVED )                                                                 \
    ACTION( RSP_REDIS_ERROR_TRYAGAIN )                                                              \
    ACTION( RSP_REDIS_ERROR_OOM )                                                                   \
    ACTION( RSP_REDIS_ERROR_BUSY )                                                                  \
    ACTION( RSP_REDIS_ERROR_NOAUTH )                                                                \
    ACTION( RSP_REDIS_ERROR_LOADING )                                                               \
    ACTION( RSP_REDIS_ERROR_BUSYKEY )                                                               \
    ACTION( RSP_REDIS_ERROR_MISCONF )                                                               \
    ACTION( RSP_REDIS_ERROR_NOSCRIPT )                                                              \
    ACTION( RSP_REDIS_ERROR_READONLY )                                                              \
    ACTION( RSP_REDIS_ERROR_WRONGTYPE )                                                             \
    ACTION( RSP_REDIS_ERROR_EXECABORT )                                                             \
    ACTION( RSP_REDIS_ERROR_MASTERDOWN )                                                            \
    ACTION( RSP_REDIS_ERROR_NOREPLICAS )                                                            \
    ACTION( RSP_REDIS_INTEGER )                                                                     \
    ACTION( RSP_REDIS_BULK )                                                                        \
    ACTION( RSP_REDIS_MULTIBULK )                                                                   \
    ACTION( SENTINEL )                                                                              \


#define DEFINE_ACTION(_name) MSG_##_name,
typedef enum msg_type {
    MSG_TYPE_CODEC(DEFINE_ACTION)
} msg_type_t;
#undef DEFINE_ACTION


#ifdef NC_LITTLE_ENDIAN


#define str4cmp(m, c0, c1, c2, c3)                                                          \
    (*(uint32_t *) m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0))

#define str5cmp(m, c0, c1, c2, c3, c4)                                                      \
    (str4cmp(m, c0, c1, c2, c3) && (m[4] == c4))

#define str6cmp(m, c0, c1, c2, c3, c4, c5)                                                  \
    (str4cmp(m, c0, c1, c2, c3) &&                                                          \
        (((uint32_t *) m)[1] & 0xffff) == ((c5 << 8) | c4))

#define str7cmp(m, c0, c1, c2, c3, c4, c5, c6)                                              \
    (str6cmp(m, c0, c1, c2, c3, c4, c5) && (m[6] == c6))

#define str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7)                                          \
    (str4cmp(m, c0, c1, c2, c3) &&                                                          \
        (((uint32_t *) m)[1] == ((c7 << 24) | (c6 << 16) | (c5 << 8) | c4)))

#define str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8)                                      \
    (str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) && m[8] == c8)

#define str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)                                 \
    (str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) &&                                          \
        (((uint32_t *) m)[2] & 0xffff) == ((c9 << 8) | c8))

#define str11cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)                            \
    (str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) && (m[10] == c10))

#define str12cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)                       \
    (str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) &&                                          \
        (((uint32_t *) m)[2] == ((c11 << 24) | (c10 << 16) | (c9 << 8) | c8)))

#else

#define str2cmp(m, c0, c1)                                                                  \
    (m[0] == c0 && m[1] == c1)

#define str3cmp(m, c0, c1, c2)                                                              \
    (m[0] == c0 && m[1] == c1 && m[2] == c2)

#define str4cmp(m, c0, c1, c2, c3)                                                          \
    (m[0] == c0 && m[1] == c1 && m[2] == c2 && m[3] == c3)

#define str5cmp(m, c0, c1, c2, c3, c4)                                                      \
    (str4cmp(m, c0, c1, c2, c3) && (m[4] == c4))

#define str6cmp(m, c0, c1, c2, c3, c4, c5)                                                  \
    (str5cmp(m, c0, c1, c2, c3, c4) && m[5] == c5)

#define str7cmp(m, c0, c1, c2, c3, c4, c5, c6)                                              \
    (str6cmp(m, c0, c1, c2, c3, c4, c5) && m[6] == c6)

#define str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7)                                          \
    (str7cmp(m, c0, c1, c2, c3, c4, c5, c6) && m[7] == c7)

#define str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8)                                      \
    (str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) && m[8] == c8)

#define str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)                                 \
    (str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8) && m[9] == c9)

#define str11cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)                            \
    (str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) && m[10] == c10)

#define str12cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)                       \
    (str11cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) && m[11] == c11)

#endif

#define str3icmp(m, c0, c1, c2)                                                             \
    ((m[0] == c0 || m[0] == (c0 ^ 0x20)) &&                                                 \
     (m[1] == c1 || m[1] == (c1 ^ 0x20)) &&                                                 \
     (m[2] == c2 || m[2] == (c2 ^ 0x20)))

#define str4icmp(m, c0, c1, c2, c3)                                                         \
    (str3icmp(m, c0, c1, c2) && (m[3] == c3 || m[3] == (c3 ^ 0x20)))

#define str5icmp(m, c0, c1, c2, c3, c4)                                                     \
    (str4icmp(m, c0, c1, c2, c3) && (m[4] == c4 || m[4] == (c4 ^ 0x20)))

#define str6icmp(m, c0, c1, c2, c3, c4, c5)                                                 \
    (str5icmp(m, c0, c1, c2, c3, c4) && (m[5] == c5 || m[5] == (c5 ^ 0x20)))

#define str7icmp(m, c0, c1, c2, c3, c4, c5, c6)                                             \
    (str6icmp(m, c0, c1, c2, c3, c4, c5) &&                                                 \
     (m[6] == c6 || m[6] == (c6 ^ 0x20)))

#define str8icmp(m, c0, c1, c2, c3, c4, c5, c6, c7)                                         \
    (str7icmp(m, c0, c1, c2, c3, c4, c5, c6) &&                                             \
     (m[7] == c7 || m[7] == (c7 ^ 0x20)))

#define str9icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8)                                     \
    (str8icmp(m, c0, c1, c2, c3, c4, c5, c6, c7) &&                                         \
     (m[8] == c8 || m[8] == (c8 ^ 0x20)))

#define str10icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)                                \
    (str9icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8) &&                                     \
     (m[9] == c9 || m[9] == (c9 ^ 0x20)))

#define str11icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)                           \
    (str10icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) &&                                \
     (m[10] == c10 || m[10] == (c10 ^ 0x20)))

#define str12icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)                      \
    (str11icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) &&                           \
     (m[11] == c11 || m[11] == (c11 ^ 0x20)))

#define str13icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)                 \
    (str12icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11) &&                      \
     (m[12] == c12 || m[12] == (c12 ^ 0x20)))

#define str14icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)            \
    (str13icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12) &&                 \
     (m[13] == c13 || m[13] == (c13 ^ 0x20)))

#define str15icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)       \
    (str14icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) &&            \
     (m[14] == c14 || m[14] == (c14 ^ 0x20)))

#define str16icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)  \
    (str15icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14) &&       \
     (m[15] == c15 || m[15] == (c15 ^ 0x20)))




#endif //__COMMON_H__
