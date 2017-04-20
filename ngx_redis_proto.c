#include "ngx_redis_proto.h"
#include <hiredis/hiredis.h>
#include <stdbool.h>
#include "common.h"

static ngx_int_t
ngx_redis_mget_slotid(redisReply* replyInfo, size_t argc,  ngx_array_t *slotids);
static ngx_int_t ngx_redis_mget(ngx_stream_session_t *s, redisReply *replyInfo, size_t argc, ngx_int_t slotid);

/* CRC16 implementation according to CCITT standards.
 *
 * Note by @antirez: this is actually the XMODEM CRC 16 algorithm, using the
 * following parameters:
 *
 * Name                       : "XMODEM", also known as "ZMODEM", "CRC-16/ACORN"
 * Width                      : 16 bit
 * Poly                       : 1021 (That is actually x^16 + x^12 + x^5 + 1)
 * Initialization             : 0000
 * Reflect Input byte         : False
 * Reflect Output CRC         : False
 * Xor constant to output CRC : 0000
 * Output for "123456789"     : 31C3
 */

static unsigned short crc16tab[256] =
{
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};



static uint16_t
crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
            crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
    return crc;
}


/* We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress). */
static unsigned int
key_hash_slot(char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key,keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing betweeen {} ? Hash the whole key. */
    if (e == keylen || e == s+1) return crc16(key,keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return crc16(key+s+1,e-s-1) & 0x3FFF;
}

static size_t
redis_get_num_size(uint64_t i)
{
    size_t          n = 0;

    do {
        i = i / 10;
        n++;
    } while (i > 0);

    return n;
}

static ngx_int_t
redis_parse_bulk(char *rdata, ssize_t rdata_len)
{
    //$3\r\nabc\r\n
    int                             len = 1;
    int                             bulklen;
    char                            *rest;

    bulklen = strtol(rdata, &rest, 0);
    if ( bulklen == -1 ) {
        return NGX_OK;
    }

    len += redis_get_num_size(bulklen);
    rest += 2, len += 2;
    rest += (bulklen + 2), len += (bulklen + 2);

    if ( len > rdata_len ) {
        return NGX_AGAIN;
    }

    return NGX_OK;
}

static ngx_int_t
redis_parse_multibulk(char *rdata, ssize_t rdata_len)
{
    //*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n
    int                             len = 1;
    int                             multibulkSize,i,bulklen;
    char                            *rest;

    multibulkSize = strtol(rdata, &rest, 0);

    len += 2;
    rest += 2;
    i = 0;
    while (i < multibulkSize)       /* For each element in multibulk data */
    {
        if ( len > rdata_len ) {
            return NGX_AGAIN;
        }

        rest++,len++;                           /* Step through $ */
        bulklen = strtol(rest, &rest, 0); /* Get an element length */
        if (bulklen == -1)                /* If the element length is -1 then element is NULL */
        {
          i++;
          continue;
        }

        len += redis_get_num_size(bulklen);
        rest += 2, len += 2;                                    /* Step through "\r\n" */
        rest += (bulklen + 2), len += (bulklen + 2);            /* Jump to the end of data bypassing "\r\n" */

        i++;
    }

    return NGX_OK;
}



ngx_int_t
redis_proto_decode(char *rdata, ssize_t len)
{
    switch (*rdata)
    {
        case '-' :
        case '+' :
        case ':' :
            return NGX_OK;
        case '$' :
            return redis_parse_bulk(++rdata, len);
        case '*' :
            return redis_parse_multibulk(++rdata, len);
        default :
            return NGX_ERROR;
    }
}


static msg_type_t
redis_command_req(const char* m, size_t len)
{
    msg_type_t type = MSG_UNKNOWN;

     switch (len) {
        case 3:
            if (str3icmp(m, 'g', 'e', 't')) {
                type = MSG_REQ_REDIS_GET;
                break;
            }

            if (str3icmp(m, 's', 'e', 't')) {
                type = MSG_REQ_REDIS_SET;
                break;
            }

            if (str3icmp(m, 't', 't', 'l')) {
                type = MSG_REQ_REDIS_TTL;
                break;
            }

            if (str3icmp(m, 'd', 'e', 'l')) {
                type = MSG_REQ_REDIS_DEL;
                break;
            }

            break;

        case 4:
            if (str4icmp(m, 'p', 't', 't', 'l')) {
                type = MSG_REQ_REDIS_PTTL;
                break;
            }

            if (str4icmp(m, 'd', 'e', 'c', 'r')) {
                type = MSG_REQ_REDIS_DECR;
                break;
            }

            if (str4icmp(m, 'd', 'u', 'm', 'p')) {
                type = MSG_REQ_REDIS_DUMP;
                break;
            }

            if (str4icmp(m, 'h', 'd', 'e', 'l')) {
                type = MSG_REQ_REDIS_HDEL;
                break;
            }

            if (str4icmp(m, 'h', 'g', 'e', 't')) {
                type = MSG_REQ_REDIS_HGET;
                break;
            }

            if (str4icmp(m, 'h', 'l', 'e', 'n')) {
                type = MSG_REQ_REDIS_HLEN;
                break;
            }

            if (str4icmp(m, 'h', 's', 'e', 't')) {
                type = MSG_REQ_REDIS_HSET;
                break;
            }

            if (str4icmp(m, 'i', 'n', 'c', 'r')) {
                type = MSG_REQ_REDIS_INCR;
                break;
            }

            if (str4icmp(m, 'l', 'l', 'e', 'n')) {
                type = MSG_REQ_REDIS_LLEN;
                break;
            }

            if (str4icmp(m, 'l', 'p', 'o', 'p')) {
                type = MSG_REQ_REDIS_LPOP;
                break;
            }

            if (str4icmp(m, 'l', 'r', 'e', 'm')) {
                type = MSG_REQ_REDIS_LREM;
                break;
            }

            if (str4icmp(m, 'l', 's', 'e', 't')) {
                type = MSG_REQ_REDIS_LSET;
                break;
            }

            if (str4icmp(m, 'r', 'p', 'o', 'p')) {
                type = MSG_REQ_REDIS_RPOP;
                break;
            }

            if (str4icmp(m, 's', 'a', 'd', 'd')) {
                type = MSG_REQ_REDIS_SADD;
                break;
            }

            if (str4icmp(m, 's', 'p', 'o', 'p')) {
                type = MSG_REQ_REDIS_SPOP;
                break;
            }

            if (str4icmp(m, 's', 'r', 'e', 'm')) {
                type = MSG_REQ_REDIS_SREM;
                break;
            }

            if (str4icmp(m, 't', 'y', 'p', 'e')) {
                type = MSG_REQ_REDIS_TYPE;
                break;
            }

            if (str4icmp(m, 'm', 'g', 'e', 't')) {
                type = MSG_REQ_REDIS_MGET;
                break;
            }
            if (str4icmp(m, 'm', 's', 'e', 't')) {
                type = MSG_REQ_REDIS_MSET;
                break;
            }

            if (str4icmp(m, 'z', 'a', 'd', 'd')) {
                type = MSG_REQ_REDIS_ZADD;
                break;
            }

            if (str4icmp(m, 'z', 'r', 'e', 'm')) {
                type = MSG_REQ_REDIS_ZREM;
                break;
            }

            if (str4icmp(m, 'e', 'v', 'a', 'l')) {
                type = MSG_REQ_REDIS_EVAL;
                break;
            }

            if (str4icmp(m, 's', 'o', 'r', 't')) {
                type = MSG_REQ_REDIS_SORT;
                break;
            }

            if (str4icmp(m, 'p', 'i', 'n', 'g')) {
                type = MSG_REQ_REDIS_PING;
                break;
            }

            if (str4icmp(m, 'q', 'u', 'i', 't')) {
                type = MSG_REQ_REDIS_QUIT;
                break;
            }

            if (str4icmp(m, 'a', 'u', 't', 'h')) {
                type = MSG_REQ_REDIS_AUTH;
                break;
            }

            break;

        case 5:
            if (str5icmp(m, 'h', 'k', 'e', 'y', 's')) {
                type = MSG_REQ_REDIS_HKEYS;
                break;
            }

            if (str5icmp(m, 'h', 'm', 'g', 'e', 't')) {
                type = MSG_REQ_REDIS_HMGET;
                break;
            }

            if (str5icmp(m, 'h', 'm', 's', 'e', 't')) {
                type = MSG_REQ_REDIS_HMSET;
                break;
            }

            if (str5icmp(m, 'h', 'v', 'a', 'l', 's')) {
                type = MSG_REQ_REDIS_HVALS;
                break;
            }

            if (str5icmp(m, 'h', 's', 'c', 'a', 'n')) {
                type = MSG_REQ_REDIS_HSCAN;
                break;
            }

            if (str5icmp(m, 'l', 'p', 'u', 's', 'h')) {
                type = MSG_REQ_REDIS_LPUSH;
                break;
            }

            if (str5icmp(m, 'l', 't', 'r', 'i', 'm')) {
                type = MSG_REQ_REDIS_LTRIM;
                break;
            }

            if (str5icmp(m, 'r', 'p', 'u', 's', 'h')) {
                type = MSG_REQ_REDIS_RPUSH;
                break;
            }

            if (str5icmp(m, 's', 'c', 'a', 'r', 'd')) {
                type = MSG_REQ_REDIS_SCARD;
                break;
            }

            if (str5icmp(m, 's', 'd', 'i', 'f', 'f')) {
                type = MSG_REQ_REDIS_SDIFF;
                break;
            }

            if (str5icmp(m, 's', 'e', 't', 'e', 'x')) {
                type = MSG_REQ_REDIS_SETEX;
                break;
            }

            if (str5icmp(m, 's', 'e', 't', 'n', 'x')) {
                type = MSG_REQ_REDIS_SETNX;
                break;
            }

            if (str5icmp(m, 's', 'm', 'o', 'v', 'e')) {
                type = MSG_REQ_REDIS_SMOVE;
                break;
            }

            if (str5icmp(m, 's', 's', 'c', 'a', 'n')) {
                type = MSG_REQ_REDIS_SSCAN;
                break;
            }

            if (str5icmp(m, 'z', 'c', 'a', 'r', 'd')) {
                type = MSG_REQ_REDIS_ZCARD;
                break;
            }

            if (str5icmp(m, 'z', 'r', 'a', 'n', 'k')) {
                type = MSG_REQ_REDIS_ZRANK;
                break;
            }

            if (str5icmp(m, 'z', 's', 'c', 'a', 'n')) {
                type = MSG_REQ_REDIS_ZSCAN;
                break;
            }

            if (str5icmp(m, 'p', 'f', 'a', 'd', 'd')) {
                type = MSG_REQ_REDIS_PFADD;
                break;
            }

            break;

        case 6:
            if (str6icmp(m, 'a', 'p', 'p', 'e', 'n', 'd')) {
                type = MSG_REQ_REDIS_APPEND;
                break;
            }

            if (str6icmp(m, 'b', 'i', 't', 'p', 'o', 's')) {
                type = MSG_REQ_REDIS_BITPOS;
                break;
            }

            if (str6icmp(m, 'd', 'e', 'c', 'r', 'b', 'y')) {
                type = MSG_REQ_REDIS_DECRBY;
                break;
            }

            if (str6icmp(m, 'e', 'x', 'i', 's', 't', 's')) {
                type = MSG_REQ_REDIS_EXISTS;
                break;
            }

            if (str6icmp(m, 'e', 'x', 'p', 'i', 'r', 'e')) {
                type = MSG_REQ_REDIS_EXPIRE;
                break;
            }

            if (str6icmp(m, 'g', 'e', 't', 'b', 'i', 't')) {
                type = MSG_REQ_REDIS_GETBIT;
                break;
            }

            if (str6icmp(m, 'g', 'e', 't', 's', 'e', 't')) {
                type = MSG_REQ_REDIS_GETSET;
                break;
            }

            if (str6icmp(m, 'p', 's', 'e', 't', 'e', 'x')) {
                type = MSG_REQ_REDIS_PSETEX;
                break;
            }

            if (str6icmp(m, 'h', 's', 'e', 't', 'n', 'x')) {
                type = MSG_REQ_REDIS_HSETNX;
                break;
            }

            if (str6icmp(m, 'i', 'n', 'c', 'r', 'b', 'y')) {
                type = MSG_REQ_REDIS_INCRBY;
                break;
            }

            if (str6icmp(m, 'l', 'i', 'n', 'd', 'e', 'x')) {
                type = MSG_REQ_REDIS_LINDEX;
                break;
            }

            if (str6icmp(m, 'l', 'p', 'u', 's', 'h', 'x')) {
                type = MSG_REQ_REDIS_LPUSHX;
                break;
            }

            if (str6icmp(m, 'l', 'r', 'a', 'n', 'g', 'e')) {
                type = MSG_REQ_REDIS_LRANGE;
                break;
            }

            if (str6icmp(m, 'r', 'p', 'u', 's', 'h', 'x')) {
                type = MSG_REQ_REDIS_RPUSHX;
                break;
            }

            if (str6icmp(m, 's', 'e', 't', 'b', 'i', 't')) {
                type = MSG_REQ_REDIS_SETBIT;
                break;
            }

            if (str6icmp(m, 's', 'i', 'n', 't', 'e', 'r')) {
                type = MSG_REQ_REDIS_SINTER;
                break;
            }

            if (str6icmp(m, 's', 't', 'r', 'l', 'e', 'n')) {
                type = MSG_REQ_REDIS_STRLEN;
                break;
            }

            if (str6icmp(m, 's', 'u', 'n', 'i', 'o', 'n')) {
                type = MSG_REQ_REDIS_SUNION;
                break;
            }

            if (str6icmp(m, 'z', 'c', 'o', 'u', 'n', 't')) {
                type = MSG_REQ_REDIS_ZCOUNT;
                break;
            }

            if (str6icmp(m, 'z', 'r', 'a', 'n', 'g', 'e')) {
                type = MSG_REQ_REDIS_ZRANGE;
                break;
            }

            if (str6icmp(m, 'z', 's', 'c', 'o', 'r', 'e')) {
                type = MSG_REQ_REDIS_ZSCORE;
                break;
            }

            break;

        case 7:
            if (str7icmp(m, 'p', 'e', 'r', 's', 'i', 's', 't')) {
                type = MSG_REQ_REDIS_PERSIST;
                break;
            }

            if (str7icmp(m, 'p', 'e', 'x', 'p', 'i', 'r', 'e')) {
                type = MSG_REQ_REDIS_PEXPIRE;
                break;
            }

            if (str7icmp(m, 'h', 'e', 'x', 'i', 's', 't', 's')) {
                type = MSG_REQ_REDIS_HEXISTS;
                break;
            }

            if (str7icmp(m, 'h', 'g', 'e', 't', 'a', 'l', 'l')) {
                type = MSG_REQ_REDIS_HGETALL;
                break;
            }

            if (str7icmp(m, 'h', 'i', 'n', 'c', 'r', 'b', 'y')) {
                type = MSG_REQ_REDIS_HINCRBY;
                break;
            }

            if (str7icmp(m, 'l', 'i', 'n', 's', 'e', 'r', 't')) {
                type = MSG_REQ_REDIS_LINSERT;
                break;
            }

            if (str7icmp(m, 'z', 'i', 'n', 'c', 'r', 'b', 'y')) {
                type = MSG_REQ_REDIS_ZINCRBY;
                break;
            }

            if (str7icmp(m, 'e', 'v', 'a', 'l', 's', 'h', 'a')) {
                type = MSG_REQ_REDIS_EVALSHA;
                break;
            }

            if (str7icmp(m, 'r', 'e', 's', 't', 'o', 'r', 'e')) {
                type = MSG_REQ_REDIS_RESTORE;
                break;
            }

            if (str7icmp(m, 'p', 'f', 'c', 'o', 'u', 'n', 't')) {
                type = MSG_REQ_REDIS_PFCOUNT;
                break;
            }

            if (str7icmp(m, 'p', 'f', 'm', 'e', 'r', 'g', 'e')) {
                type = MSG_REQ_REDIS_PFMERGE;
                break;
            }

            break;

        case 8:
            if (str8icmp(m, 'e', 'x', 'p', 'i', 'r', 'e', 'a', 't')) {
                type = MSG_REQ_REDIS_EXPIREAT;
                break;
            }

            if (str8icmp(m, 'b', 'i', 't', 'c', 'o', 'u', 'n', 't')) {
                type = MSG_REQ_REDIS_BITCOUNT;
                break;
            }

            if (str8icmp(m, 'g', 'e', 't', 'r', 'a', 'n', 'g', 'e')) {
                type = MSG_REQ_REDIS_GETRANGE;
                break;
            }

            if (str8icmp(m, 's', 'e', 't', 'r', 'a', 'n', 'g', 'e')) {
                type = MSG_REQ_REDIS_SETRANGE;
                break;
            }

            if (str8icmp(m, 's', 'm', 'e', 'm', 'b', 'e', 'r', 's')) {
                type = MSG_REQ_REDIS_SMEMBERS;
                break;
            }

            if (str8icmp(m, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'k')) {
                type = MSG_REQ_REDIS_ZREVRANK;
                break;
            }

            break;

        case 9:
            if (str9icmp(m, 'p', 'e', 'x', 'p', 'i', 'r', 'e', 'a', 't')) {
                type = MSG_REQ_REDIS_PEXPIREAT;
                break;
            }

            if (str9icmp(m, 'r', 'p', 'o', 'p', 'l', 'p', 'u', 's', 'h')) {
                type = MSG_REQ_REDIS_RPOPLPUSH;
                break;
            }

            if (str9icmp(m, 's', 'i', 's', 'm', 'e', 'm', 'b', 'e', 'r')) {
                type = MSG_REQ_REDIS_SISMEMBER;
                break;
            }

            if (str9icmp(m, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'g', 'e')) {
                type = MSG_REQ_REDIS_ZREVRANGE;
                break;
            }

            if (str9icmp(m, 'z', 'l', 'e', 'x', 'c', 'o', 'u', 'n', 't')) {
                type = MSG_REQ_REDIS_ZLEXCOUNT;
                break;
            }

            break;

        case 10:
            if (str10icmp(m, 's', 'd', 'i', 'f', 'f', 's', 't', 'o', 'r', 'e')) {
                type = MSG_REQ_REDIS_SDIFFSTORE;
                break;
            }

        case 11:
            if (str11icmp(m, 'i', 'n', 'c', 'r', 'b', 'y', 'f', 'l', 'o', 'a', 't')) {
                type = MSG_REQ_REDIS_INCRBYFLOAT;
                break;
            }

            if (str11icmp(m, 's', 'i', 'n', 't', 'e', 'r', 's', 't', 'o', 'r', 'e')) {
                type = MSG_REQ_REDIS_SINTERSTORE;
                break;
            }

            if (str11icmp(m, 's', 'r', 'a', 'n', 'd', 'm', 'e', 'm', 'b', 'e', 'r')) {
                type = MSG_REQ_REDIS_SRANDMEMBER;
                break;
            }

            if (str11icmp(m, 's', 'u', 'n', 'i', 'o', 'n', 's', 't', 'o', 'r', 'e')) {
                type = MSG_REQ_REDIS_SUNIONSTORE;
                break;
            }

            if (str11icmp(m, 'z', 'i', 'n', 't', 'e', 'r', 's', 't', 'o', 'r', 'e')) {
                type = MSG_REQ_REDIS_ZINTERSTORE;
                break;
            }

            if (str11icmp(m, 'z', 'u', 'n', 'i', 'o', 'n', 's', 't', 'o', 'r', 'e')) {
                type = MSG_REQ_REDIS_ZUNIONSTORE;
                break;
            }

            if (str11icmp(m, 'z', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 'l', 'e', 'x')) {
                type = MSG_REQ_REDIS_ZRANGEBYLEX;
                break;
            }

            break;

        case 12:
            if (str12icmp(m, 'h', 'i', 'n', 'c', 'r', 'b', 'y', 'f', 'l', 'o', 'a', 't')) {
                type = MSG_REQ_REDIS_HINCRBYFLOAT;
                break;
            }


            break;

        case 13:
            if (str13icmp(m, 'z', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 's', 'c', 'o', 'r', 'e')) {
                type = MSG_REQ_REDIS_ZRANGEBYSCORE;
                break;
            }

            break;

        case 14:
            if (str14icmp(m, 'z', 'r', 'e', 'm', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 'l', 'e', 'x')) {
                type = MSG_REQ_REDIS_ZREMRANGEBYLEX;
                break;
            }

            break;

        case 15:
            if (str15icmp(m, 'z', 'r', 'e', 'm', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 'r', 'a', 'n', 'k')) {
                type = MSG_REQ_REDIS_ZREMRANGEBYRANK;
                break;
            }

            break;

        case 16:
            if (str16icmp(m, 'z', 'r', 'e', 'm', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 's', 'c', 'o', 'r', 'e')) {
                type = MSG_REQ_REDIS_ZREMRANGEBYSCORE;
                break;
            }

            if (str16icmp(m, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 's', 'c', 'o', 'r', 'e')) {
                type = MSG_REQ_REDIS_ZREVRANGEBYSCORE;
                break;
            }

            break;

        default:
            break;
        }

    return type;
}


/*
 * Return true, if the redis command take no key, otherwise
 * return false
 */
static bool
redis_argz(msg_type_t type)
{
   switch (type) {
    case MSG_REQ_REDIS_PING:
    case MSG_REQ_REDIS_QUIT:
        return true;

    default:
        break;
    }

    return false;
}


/*
 * Return true, if the redis command accepts no arguments, otherwise
 * return false
 */
static bool
redis_arg0(msg_type_t type)
{
    switch (type) {
    case MSG_REQ_REDIS_EXISTS:
    case MSG_REQ_REDIS_PERSIST:
    case MSG_REQ_REDIS_PTTL:
    case MSG_REQ_REDIS_TTL:
    case MSG_REQ_REDIS_TYPE:
    case MSG_REQ_REDIS_DUMP:

    case MSG_REQ_REDIS_DECR:
    case MSG_REQ_REDIS_GET:
    case MSG_REQ_REDIS_INCR:
    case MSG_REQ_REDIS_STRLEN:

    case MSG_REQ_REDIS_HGETALL:
    case MSG_REQ_REDIS_HKEYS:
    case MSG_REQ_REDIS_HLEN:
    case MSG_REQ_REDIS_HVALS:

    case MSG_REQ_REDIS_LLEN:
    case MSG_REQ_REDIS_LPOP:
    case MSG_REQ_REDIS_RPOP:

    case MSG_REQ_REDIS_SCARD:
    case MSG_REQ_REDIS_SMEMBERS:
    case MSG_REQ_REDIS_SPOP:

    case MSG_REQ_REDIS_ZCARD:
    case MSG_REQ_REDIS_PFCOUNT:
    case MSG_REQ_REDIS_AUTH:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the redis command accepts exactly 1 argument, otherwise
 * return false
 */
static bool
redis_arg1(msg_type_t type)
{
    switch (type) {
    case MSG_REQ_REDIS_EXPIRE:
    case MSG_REQ_REDIS_EXPIREAT:
    case MSG_REQ_REDIS_PEXPIRE:
    case MSG_REQ_REDIS_PEXPIREAT:

    case MSG_REQ_REDIS_APPEND:
    case MSG_REQ_REDIS_DECRBY:
    case MSG_REQ_REDIS_GETBIT:
    case MSG_REQ_REDIS_GETSET:
    case MSG_REQ_REDIS_INCRBY:
    case MSG_REQ_REDIS_INCRBYFLOAT:
    case MSG_REQ_REDIS_SETNX:

    case MSG_REQ_REDIS_HEXISTS:
    case MSG_REQ_REDIS_HGET:

    case MSG_REQ_REDIS_LINDEX:
    case MSG_REQ_REDIS_LPUSHX:
    case MSG_REQ_REDIS_RPOPLPUSH:
    case MSG_REQ_REDIS_RPUSHX:

    case MSG_REQ_REDIS_SISMEMBER:

    case MSG_REQ_REDIS_ZRANK:
    case MSG_REQ_REDIS_ZREVRANK:
    case MSG_REQ_REDIS_ZSCORE:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the redis command accepts exactly 2 arguments, otherwise
 * return false
 */
static bool
redis_arg2(msg_type_t type)
{
    switch (type) {
    case MSG_REQ_REDIS_GETRANGE:
    case MSG_REQ_REDIS_PSETEX:
    case MSG_REQ_REDIS_SETBIT:
    case MSG_REQ_REDIS_SETEX:
    case MSG_REQ_REDIS_SETRANGE:

    case MSG_REQ_REDIS_HINCRBY:
    case MSG_REQ_REDIS_HINCRBYFLOAT:
    case MSG_REQ_REDIS_HSET:
    case MSG_REQ_REDIS_HSETNX:

    case MSG_REQ_REDIS_LRANGE:
    case MSG_REQ_REDIS_LREM:
    case MSG_REQ_REDIS_LSET:
    case MSG_REQ_REDIS_LTRIM:

    case MSG_REQ_REDIS_SMOVE:

    case MSG_REQ_REDIS_ZCOUNT:
    case MSG_REQ_REDIS_ZLEXCOUNT:
    case MSG_REQ_REDIS_ZINCRBY:
    case MSG_REQ_REDIS_ZREMRANGEBYLEX:
    case MSG_REQ_REDIS_ZREMRANGEBYRANK:
    case MSG_REQ_REDIS_ZREMRANGEBYSCORE:

    case MSG_REQ_REDIS_RESTORE:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the redis command accepts exactly 3 arguments, otherwise
 * return false
 */
static bool
redis_arg3(msg_type_t type)
{
    switch (type) {
    case MSG_REQ_REDIS_LINSERT:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the redis command accepts 0 or more arguments, otherwise
 * return false
 */
static bool
redis_argn(msg_type_t type)
{
    switch (type) {
    case MSG_REQ_REDIS_SORT:

    case MSG_REQ_REDIS_BITCOUNT:
    case MSG_REQ_REDIS_BITPOS:

    case MSG_REQ_REDIS_SET:
    case MSG_REQ_REDIS_HDEL:
    case MSG_REQ_REDIS_HMGET:
    case MSG_REQ_REDIS_HMSET:
    case MSG_REQ_REDIS_HSCAN:

    case MSG_REQ_REDIS_LPUSH:
    case MSG_REQ_REDIS_RPUSH:

    case MSG_REQ_REDIS_SADD:
    case MSG_REQ_REDIS_SDIFF:
    case MSG_REQ_REDIS_SDIFFSTORE:
    case MSG_REQ_REDIS_SINTER:
    case MSG_REQ_REDIS_SINTERSTORE:
    case MSG_REQ_REDIS_SREM:
    case MSG_REQ_REDIS_SUNION:
    case MSG_REQ_REDIS_SUNIONSTORE:
    case MSG_REQ_REDIS_SRANDMEMBER:
    case MSG_REQ_REDIS_SSCAN:

    case MSG_REQ_REDIS_PFADD:
    case MSG_REQ_REDIS_PFMERGE:

    case MSG_REQ_REDIS_ZADD:
    case MSG_REQ_REDIS_ZINTERSTORE:
    case MSG_REQ_REDIS_ZRANGE:
    case MSG_REQ_REDIS_ZRANGEBYSCORE:
    case MSG_REQ_REDIS_ZREM:
    case MSG_REQ_REDIS_ZREVRANGE:
    case MSG_REQ_REDIS_ZRANGEBYLEX:
    case MSG_REQ_REDIS_ZREVRANGEBYSCORE:
    case MSG_REQ_REDIS_ZUNIONSTORE:
    case MSG_REQ_REDIS_ZSCAN:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the redis command is a vector command accepting one or
 * more keys, otherwise return false
 */
static bool
redis_argx(msg_type_t type)
{
    switch (type) {
    case MSG_REQ_REDIS_MGET:
    case MSG_REQ_REDIS_DEL:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the redis command is a vector command accepting one or
 * more key-value pairs, otherwise return false
 */
static bool
redis_argkvx(msg_type_t type)
{
    switch (type) {
    case MSG_REQ_REDIS_MSET:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the redis command is either EVAL or EVALSHA. These commands
 * have a special format with exactly 2 arguments, followed by one or more keys,
 * followed by zero or more arguments (the documentation online seems to suggest
 * that at least one argument is required, but that shouldn't be the case).
 */
static bool
redis_argeval(msg_type_t type)
{
    switch (type) {
    case MSG_REQ_REDIS_EVAL:
    case MSG_REQ_REDIS_EVALSHA:
        return true;

    default:
        break;
    }

    return false;
}



ngx_int_t
redis_parse_req(ngx_stream_session_t *s)
{
    ngx_int_t                           rc;
    int                                 ret;
    void                                *reply;
    char                                *data;
    ssize_t                              len, argc;
    ngx_buf_t                           *b;
    redisReader                         *reader;
    redisReply                          *replyInfo;
    ngx_stream_redis_proxy_ctx_t        *ctx;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    b = ctx->buffer_in;
    len = b->last - b->pos;
    data = (char*) b->pos;

    if (str4icmp(data, 'P', 'I', 'N', 'G')) {
        return REDIS_OK;
    }

    rc = redis_proto_decode(data, len);
    if ( rc != NGX_OK ) {
        return rc;
    }

    reader = redisReaderCreate();
    redisReaderFeed(reader,data, len);
    ret = redisReaderGetReply(reader,&reply);
    if ( ret == REDIS_ERR || ((redisReply*)reply)->type != REDIS_REPLY_ARRAY) {
        redisReaderFree(reader);
        return NGX_ERROR;
    }
    // REDIS_OK

    replyInfo = (redisReply*)reply;
    argc = replyInfo->elements;
    if  ( argc <= 0 ) {
        redisReaderFree(reader);
        return NGX_ERROR;
    }

    ctx->type = redis_command_req(replyInfo->element[0]->str, replyInfo->element[0]->len);
    if (ctx->type == MSG_UNKNOWN) {
        //("parsed unsupported command '%.*s'", p - m, m);
        redisReaderFree(reader);
        return NGX_ERROR;
    }

    if (redis_argz(ctx->type)) {
        //PING || QUIT
        redisReaderFree(reader);
        return NGX_OK;
    }

    if (redis_arg0(ctx->type)) {
        goto done;
    }

    if (redis_arg1(ctx->type)) {
        goto done;
    }

    if (redis_arg2(ctx->type)) {
        goto done;
    }

    if (redis_arg3(ctx->type)) {
        goto done;
    }

    if (redis_argn(ctx->type)) {
        goto done;
    }

    if (redis_argx(ctx->type)) {
        //MGET key... || DEL key...
        //char *key =  ((redisReply*)reply)->element[1]->str;
        goto argx;
    }

    if (redis_argkvx(ctx->type)) {
        //MSET key...
        //char *key =  ((redisReply*)reply)->element[1]->str;
        return NGX_OK;
    }

    if (redis_argeval(ctx->type)) {
        //EVAL || EVALSHA

        return NGX_OK;
    }

done:
    ctx->slotid = key_hash_slot(replyInfo->element[1]->str, replyInfo->element[1]->len);
    goto success;

argx:
    ctx->slotids = ngx_array_create(s->connection->pool, argc, sizeof(size_t));
    if ( ctx->slotids == NULL ) {
        goto failed;
    }

    ctx->client_buffers = ngx_array_create(s->connection->pool, argc, sizeof(ngx_str_t));
    if ( ctx->client_buffers == NULL ) {
        goto failed;
    }

    for ( ;; ) {
        rc = ngx_redis_mget_slotid(replyInfo, argc,  ctx->slotids);
        if ( rc < 0 ) {
            return NGX_OK;
        }

        rc = ngx_redis_mget(s, replyInfo, argc, rc);
        if ( rc != NGX_OK ) {
            goto failed;
        }
    }
    goto success;

failed:
    if (reply != NULL) {
        freeReplyObject(reply);
    }
    if ( redisReaderFree != NULL ) {
        redisReaderFree(reader);
    }

    return NGX_ERROR;

success:
    if (reply != NULL) {
        freeReplyObject(reply);
    }
    if ( redisReaderFree != NULL ) {
        redisReaderFree(reader);
    }

    return NGX_OK;
}

static ngx_int_t
ngx_redis_mget_slotid(redisReply* replyInfo, size_t argc,  ngx_array_t *slotids)
{
    size_t                               found;
    size_t                               i, j, slotid;
    size_t                              *slotid_item;

    for ( i = 1; i < argc; i++ ) {
        slotid = key_hash_slot(replyInfo->element[i]->str, replyInfo->element[i]->len);

        if (slotids->nelts == 0) {
            return slotid;
        }
        found = 0;
        for ( j = 0; j < slotids->nelts; j++ ) {
            slotid_item = (size_t*) slotids->elts + j;
            if (slotid == (*slotid_item)) {
                found = 1;
                break;
            }
        }
        if (!found) {
            return slotid;
        }
    }
    return -1;
}

static ngx_int_t
ngx_redis_mget(ngx_stream_session_t *s, redisReply *replyInfo, size_t argc, ngx_int_t slotid)
{
    size_t                              len = 0;
    size_t                              i, *slotid_item;
    ngx_str_t                           *key, *buffer;
    ngx_array_t                         *keys;
    ngx_stream_redis_proxy_ctx_t        *ctx;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    keys = ngx_array_create(s->connection->pool, argc, sizeof(ngx_str_t));
    if ( keys == NULL ) {
        return NGX_ERROR;
    }

    for ( i=1; i < argc; i++ ) {
        if ( slotid == key_hash_slot(replyInfo->element[i]->str, replyInfo->element[i]->len) ) {
            key =  ngx_array_push(keys);
            if (key == NULL) {
                return NGX_ERROR;
            }
            key->len = replyInfo->element[i]->len;
            key->data = (u_char*)replyInfo->element[i]->str;
        }
    }

    //*3\r\n
    len += 3;
    len += redis_get_num_size(keys->nelts + 1);

    //$3\r\nMGET\r\n
    len += 5;
    len += redis_get_num_size(replyInfo->element[0]->len);
    len += replyInfo->element[0]->len;

    for ( i = 0; i < keys->nelts; i++ ) {
        key = (ngx_str_t*)keys->elts + i;
        //$3\r\nabc\r\n
        len += 5;
        len += redis_get_num_size(key->len);
        len += key->len;
    }

    slotid_item = ngx_array_push(ctx->slotids);
    if ( slotid_item == NULL ) {
        return NGX_ERROR;
    }
    *slotid_item = slotid;

    buffer = (ngx_str_t *)ngx_array_push(ctx->client_buffers);
    if ( buffer == NULL ) {
        return NGX_ERROR;
    }
    buffer->len = len;
    buffer->data = (u_char*)ngx_pcalloc(s->connection->pool, len);
    if ( buffer->data == NULL  ) {
        return NGX_ERROR;
    }
    ngx_sprintf(buffer->data,"*%d\r\n", (unsigned long) keys->nelts + 1);

    ngx_sprintf(buffer->data,"%s$%d\r\n", buffer->data, (unsigned long) replyInfo->element[0]->len);
    ngx_sprintf(buffer->data,"%s%s\r\n", buffer->data, (char*)replyInfo->element[0]->str);

    for ( i = 0; i < keys->nelts; i++) {
        key = (ngx_str_t*)keys->elts + i;
        //$3\r\nabc\r\n
        ngx_sprintf(buffer->data,"%s$%d\r\n", buffer->data,  (unsigned long) key->len);
        ngx_sprintf(buffer->data,"%s%s\r\n", buffer->data,  (char*)key->data);
    }

    ngx_array_destroy(keys);

    return NGX_OK;
}

static void
redis_resp_error(ngx_stream_redis_proxy_ctx_t *ctx, u_char  *data, ssize_t len)
{
    ssize_t                   cmd_len;
    u_char                    *p, *m;

    m = data;
    p = (u_char *)ngx_strchr(data, ' ');
    if ( p != NULL ) {
        cmd_len = p - data;
    }

    switch (cmd_len) {
        case 3:
            /*-ASK 1 127.0.0.1 */
            if (str4cmp(m, '-', 'A', 'S', 'K')) {
                ctx->type = MSG_RSP_REDIS_ERROR_ASK;
                break;
            }

            break;

        case 4:
            /*
            * -ERR no such key\r\n
            * -ERR syntax error\r\n
            * -ERR source and destination objects are the same\r\n
            * -ERR index out of range\r\n
            */
            if (str4cmp(m, '-', 'E', 'R', 'R')) {
                ctx->type = MSG_RSP_REDIS_ERROR_ERR;
                break;
            }

            /* -OOM command not allowed when used memory > 'maxmemory'.\r\n */
            if (str4cmp(m, '-', 'O', 'O', 'M')) {
                ctx->type = MSG_RSP_REDIS_ERROR_OOM;
                break;
            }

            break;

        case 5:
            /* -BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n" */
            if (str5cmp(m, '-', 'B', 'U', 'S', 'Y')) {
                ctx->type = MSG_RSP_REDIS_ERROR_BUSY;
                break;
            }

            break;

        case 6:
            /* -MOVED 3999 127.0.0.1:6381 */
            if (str6cmp(m, '-', 'M', 'O', 'V', 'E', 'D')) {
                ctx->type = MSG_RSP_REDIS_ERROR_MOVED;
                break;
            }

            break;

        case 7:
            /* -NOAUTH Authentication required.\r\n */
            if (str7cmp(m, '-', 'N', 'O', 'A', 'U', 'T', 'H')) {
                ctx->type = MSG_RSP_REDIS_ERROR_NOAUTH;
                break;
            }

            break;

        case 8:
            /* rsp: "-LOADING Redis is loading the dataset in memory\r\n" */
            if (str8cmp(m, '-', 'L', 'O', 'A', 'D', 'I', 'N', 'G')) {
                ctx->type = MSG_RSP_REDIS_ERROR_LOADING;
                break;
            }

            /* -BUSYKEY Target key name already exists.\r\n */
            if (str8cmp(m, '-', 'B', 'U', 'S', 'Y', 'K', 'E', 'Y')) {
                ctx->type = MSG_RSP_REDIS_ERROR_BUSYKEY;
                break;
            }

            /* "-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk. Commands that may modify the data set are disabled. Please check Redis logs for details about the error.\r\n" */
            if (str8cmp(m, '-', 'M', 'I', 'S', 'C', 'O', 'N', 'F')) {
                ctx->type = MSG_RSP_REDIS_ERROR_MISCONF;
                break;
            }

            break;

        case 9:
            /*-TRYAGAIN */
            if (str9cmp(m, '-', 'T', 'R', 'Y', 'A', 'G', 'A', 'I', 'N')) {
                ctx->type = MSG_RSP_REDIS_ERROR_TRYAGAIN;
                break;
            }

            /* -NOSCRIPT No matching script. Please use EVAL.\r\n */
            if (str9cmp(m, '-', 'N', 'O', 'S', 'C', 'R', 'I', 'P', 'T')) {
                ctx->type = MSG_RSP_REDIS_ERROR_NOSCRIPT;
                break;
            }

            /* -READONLY You can't write against a read only slave.\r\n */
            if (str9cmp(m, '-', 'R', 'E', 'A', 'D', 'O', 'N', 'L', 'Y')) {
                ctx->type = MSG_RSP_REDIS_ERROR_READONLY;
                break;
            }

            break;

        case 10:
            /* -WRONGTYPE Operation against a key holding the wrong kind of value\r\n */
            if (str10cmp(m, '-', 'W', 'R', 'O', 'N', 'G', 'T', 'Y', 'P', 'E')) {
                ctx->type = MSG_RSP_REDIS_ERROR_WRONGTYPE;
                break;
            }

            /* -EXECABORT Transaction discarded because of previous errors.\r\n" */
            if (str10cmp(m, '-', 'E', 'X', 'E', 'C', 'A', 'B', 'O', 'R', 'T')) {
                ctx->type = MSG_RSP_REDIS_ERROR_EXECABORT;
                break;
            }

            break;

        case 11:
            /* -MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no'.\r\n */
            if (str11cmp(m, '-', 'M', 'A', 'S', 'T', 'E', 'R', 'D', 'O', 'W', 'N')) {
                ctx->type = MSG_RSP_REDIS_ERROR_MASTERDOWN;
                break;
            }

            /* -NOREPLICAS Not enough good slaves to write.\r\n */
            if (str11cmp(m, '-', 'N', 'O', 'R', 'E', 'P', 'L', 'I', 'C', 'A', 'S')) {
                ctx->type = MSG_RSP_REDIS_ERROR_NOREPLICAS;
                break;
            }

            break;
    }
}



ngx_int_t
redis_parse_rsp(ngx_stream_session_t *s)
{
    ngx_int_t                           rc;
    u_char                              *p, *m;
    char                                ch;
    u_char                              *data;
    ssize_t                              len;
    ngx_buf_t                           *b;
    ngx_stream_upstream_t               *u;
    ngx_stream_redis_proxy_ctx_t        *ctx;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    u = s->upstream;
    b = &u->upstream_buf;
    len = b->last - b->pos;
    data = (u_char*) b->pos;

    rc = redis_proto_decode((char*)data, len);
    if ( rc != NGX_OK ) {
        return rc;
    }

    p = data;
    m = data;
    ch = *p;

    switch (ch) {
        case '+':
            ctx->type = MSG_RSP_REDIS_STATUS;
            p = p - 1; /* go back by 1 byte */
            return NGX_OK;

        case '-':
            redis_resp_error(ctx, m, len);
            p = p - 1; /* go back by 1 byte */
            return NGX_OK;

        case ':':
            ctx->type = MSG_RSP_REDIS_INTEGER;
            p = p - 1; /* go back by 1 byte */
            return NGX_OK;

        case '$':
            ctx->type = MSG_RSP_REDIS_BULK;
            p = p - 1; /* go back by 1 byte */
            return NGX_OK;

        case '*':
            ctx->type = MSG_RSP_REDIS_MULTIBULK;
            p = p - 1; /* go back by 1 byte */
            return NGX_OK;

        default:
            return NGX_ERROR;
    }
}

