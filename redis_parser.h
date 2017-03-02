#ifndef __QKPACK_PARSER_H__
#define __QKPACK_PARSER_H__

#include "common.h"
#include "redis_proto.h"

class RedisParser
{
public:
    RedisParser();
    virtual ~RedisParser();

public:

    /**
     *
     * 解析Request Body
     *
    */
    int ParserRequestBody(std::string &data, int &slot_id);


    /**
     *
     * 解析Response Body
     *
    */
    int ParserResponseBody(std::string &data);

private:
    RedisProto *redis_proto_;

};


#endif /* __QKPACK_PARSER_H__ */

