#include "redis_parser.h"

RedisParser::RedisParser()
{
    redis_proto_ = new RedisProto();
}

RedisParser::~RedisParser()
{
    if (redis_proto_) {
        delete redis_proto_;
        redis_proto_ = NULL;
    }
}


/**
 *
 * 解析Request Body
 *
*/
int RedisParser::ParserRequestBody(std::string &data, int &slot_id)
{
    return redis_proto_->RedisProtoDecode(data, slot_id);
}


/**
 *
 * 解析Response Body
 *
*/
int RedisParser::ParserResponseBody(std::string &data)
{
    int slot_id;
    return redis_proto_->RedisProtoDecode(data, slot_id);
}

