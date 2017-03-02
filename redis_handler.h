#ifndef __QKPACK_HANDLER_H__
#define __QKPACK_HANDLER_H__

#include "common.h"
#include "redis_cluster.h"
#include "redis_parser.h"


class RedisHandler
{
public:
    RedisHandler(){};
    virtual ~RedisHandler(){};

public:
    /**
     *
     * Initialize Work Process
    */
    int Init();

    /**
     *
     * Exit Work Process
    */
    int Exit();

    /**
     *
     * Process Request Body
    */
    int ProcessRequest(const std::string &cluster_name, std::string &data, std::string &node_ip);

    /**
     *
     * Process Response Body
    */
    int ProcessResponse(const std::string &cluster_name, std::string &data, std::string &node_ip);

    int UpdateRoute(const std::string &cluster_name,const std::string &data);

private:
    RedisCluster     *redis_cluster_;
    RedisParser      *redis_parser_;

};


#endif /* __QKPACK_HANDLER_H__ */

