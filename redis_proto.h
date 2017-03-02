#ifndef __REDIS_PROTO_H__
#define __REDIS_PROTO_H__

#include "common.h"


class RedisProto
{
public:
    RedisProto();
    virtual ~RedisProto();

public:

    int RedisProtoDecode(std::string &data, int &slot_id);

protected:

    int ProtoDecodeGetStatus(char *rdata);

    int ProtoDecodeGetError(char *rdata);

    int ProtoDecodeGetString(char *rdata, ssize_t rdata_len);

    int ProtoDecodeGetArray(char *rdata, ssize_t rdata_len, int &slot_id);

    int GetNumSize(uint64_t i);

};


#endif //__REDIS_PROTO_H__

