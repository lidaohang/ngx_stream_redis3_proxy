#ifndef __REDIS_NODE_H__
#define __REDIS_NODE_H__

#include "common.h"

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
class RedisNode {

public:
    RedisNode(){};
    virtual ~RedisNode(){};

public:
    RedisNode& SetId(const char* id);

    RedisNode& SetAddr(const char* addr);

    RedisNode& SetRole(const char* type);

    RedisNode& SetSid(const char* sid);

    RedisNode& SetMyself(bool yesno);

    RedisNode& SetConnected(bool yesno);

    void AddSlotRange(size_t min, size_t max);

    const char* GetId() const
    {
        return id_.c_str();
    }

    const char* GetAddr() const
    {
        return addr_.c_str();
    }

    const char* GetRole() const
    {
        return role_.c_str();
    }

    const char* GetSid() const
    {
        return sid_.c_str();
    }

    bool IsMyself() const
    {
        return myself_;
    }

    bool IsConnected() const
    {
        return connected_;
    }

    const std::vector<std::pair<size_t, size_t> >& GetSlots() const
    {
        return slots_;
    }


private:
    std::string id_;
    std::string addr_;
    std::string role_;
    std::string sid_;
    bool myself_;
    bool connected_;

    std::vector<std::pair<size_t, size_t> > slots_;
};


#endif //__REDIS_NODE_H__

