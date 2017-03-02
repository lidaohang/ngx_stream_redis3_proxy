#include "redis_node.h"


RedisNode& RedisNode::SetId(const char* id)
{
    id_ = id;
    return *this;
}

RedisNode& RedisNode::SetAddr(const char* addr)
{
    addr_ = addr;
    return *this;
}

RedisNode& RedisNode::SetRole(const char* role)
{
    role_ = role;
    return *this;
}

RedisNode& RedisNode::SetSid(const char* sid)
{
    sid_ = sid;
    return *this;
}

RedisNode& RedisNode::SetMyself(bool yesno)
{
    myself_ = yesno;
    return *this;
}

RedisNode& RedisNode::SetConnected(bool yesno)
{
    connected_ = yesno;
    return *this;
}

void RedisNode::AddSlotRange(size_t min, size_t max)
{
    std::pair<size_t, size_t> range = std::make_pair(min, max);
    slots_.push_back(range);
}

