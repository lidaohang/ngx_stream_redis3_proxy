#include "redis_proto.h"
#include "redis_slot.h"

RedisProto::RedisProto()
{
}

RedisProto::~RedisProto()
{
}

int RedisProto::RedisProtoDecode(std::string &data, int &slot_id)
{
    char                            *rdata;
    ssize_t                         len;

    rdata = (char*) data.c_str();
    len = data.length();

    switch (*rdata)
    {
        case '-' : // ERROR
            return ProtoDecodeGetError(++rdata);
        case '+' : // STATUS
            return ProtoDecodeGetStatus(++rdata);
        case ':' : // INTEGER
            return REDIS_OK;
        case '$' : // STRING
            return ProtoDecodeGetString(++rdata, len);
        case '*' : // ARRAY
            return ProtoDecodeGetArray(++rdata, len, slot_id);
        default  :
            return REDIS_ERROR;
    }
}

int RedisProto::ProtoDecodeGetError(char *rdata)
{
    //-MOVED || -ASK || -ERR  || -TRYAGAIN || -NOSCRIPT
    char                            *p = rdata;

    //MOVED 1 127.0.0.1
    if ( strncmp(p,"MOVED",sizeof("MOVED")-1) == 0 ) {
        return REDIS_MOVED;
    }

    //ASK 1 127.0.0.1
    if ( strncmp(p,"ASK",sizeof("ASK")-1) == 0 ) {
        return REDIS_ASK;
    }

    //TRYAGAIN
    if ( strncmp(p,"TRYAGAIN",sizeof("TRYAGAIN")-1) == 0 ) {
        return REDIS_TRYAGAIN;
    }

    //NOSCRIPT
    if ( strncmp(p,"NOSCRIPT",sizeof("NOSCRIPT")-1) == 0 ) {
        return REDIS_NOSCRIPT;
    }

    //ERR
    while(strncmp(p, "\r\n", 2) != 0) p++;
    int size = p - rdata;
    size = ( size == -1) ? strlen(rdata) : size;

    if ( size < 0 ) {
        return REDIS_ERROR;
    }

    return REDIS_OK;
}

int RedisProto::ProtoDecodeGetStatus(char *rdata)
{
    //+OK\r\n
    int                             size;
    char                            *p = rdata;

    while(strncmp(p, "\r\n", 2) != 0) p++;
    size = p - rdata;
    size = (size == -1) ? strlen(rdata) : size;

    if (size < 0) {
        return REDIS_ERROR;
    }

    return REDIS_OK;
}


int RedisProto::ProtoDecodeGetString(char *rdata, ssize_t rdata_len)
{
    //$3\r\nabc\r\n
    int                             len = 1;
    int                             bulklen;
    char                            *rest;

    bulklen = strtol(rdata, &rest, 0);
    if ( bulklen == -1 ) {
        return REDIS_OK;
    }

    len += GetNumSize(bulklen);
    rest += 2, len += 2;
    rest += (bulklen + 2), len += (bulklen + 2);

    if ( len > rdata_len ) {
        return REDIS_AGAIN;
    }

    return REDIS_OK;
}

int RedisProto::ProtoDecodeGetArray(char *rdata, ssize_t rdata_len, int &slot_id)
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
            return REDIS_AGAIN;
        }

        rest++,len++;                           /* Step through $ */
        bulklen = strtol(rest, &rest, 0); /* Get an element length */
        if (bulklen == -1) {                /* If the element length is -1 then element is NULL */
          i++;
          continue;
        }

        len += GetNumSize(bulklen);
        rest += 2, len += 2;                                    /* Step through "\r\n" */

        if (i == 1) {
            slot_id = key_hash_slot(rest, bulklen);
        }

        rest += (bulklen + 2) , len += (bulklen + 2);            /* Jump to the end of data bypassing "\r\n" */

        i++;
    }

    return REDIS_OK;
}

int RedisProto::GetNumSize(uint64_t i)
{
    size_t          n = 0;

    do {
        i = i / 10;
        n++;
    } while (i > 0);

    return n;
}

