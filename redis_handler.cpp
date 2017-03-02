#include "redis_handler.h"

int RedisHandler::Init()
{
    redis_cluster_ = new RedisCluster("");
    redis_parser_ = new RedisParser();

    return REDIS_OK;
}

int RedisHandler::Exit()
{
    if ( redis_parser_  ) {
        delete redis_parser_;
        redis_parser_ = NULL;
    }

    if ( redis_cluster_  ) {
        delete redis_cluster_;
        redis_cluster_ = NULL;
    }

    return REDIS_OK;
}

int RedisHandler::ProcessRequest(const std::string &cluster_name, std::string &data, std::string &node_ip)
{
    int                 slot_id;
    int                 rc;

    //解析数据，获取slot_id
    rc = redis_parser_->ParserRequestBody(data, slot_id);
    if (rc == REDIS_AGAIN || rc == REDIS_ERROR) {
        return rc;
    }

    //是否初始化cluster nodes路由信息, 先发送cluster nodes更新路由
    rc = redis_cluster_->InitClusterNodes(cluster_name);
    if (rc == REDIS_CLUSTER_NODES) {
        return rc;
    }

    //从路由中获取node_ip
    rc = redis_cluster_->GetNode(cluster_name, slot_id, node_ip);
    if (rc != REDIS_OK) {
        return rc;
    }

    return REDIS_OK;
}

//更新路由信息
int RedisHandler::UpdateRoute(const std::string &cluster_name,const std::string &data)
{
    return redis_cluster_->UpdateClusterNodes(cluster_name, data);
}

int RedisHandler::ProcessResponse(const std::string &cluster_name, std::string &data, std::string &node_ip)
{
    int                 rc;
    int                 status;

    rc = REDIS_OK;

    //解析redis节点响应的数据
    status = redis_parser_->ParserResponseBody(data);
    if (status == REDIS_OK || status == REDIS_ERROR || status == REDIS_NOSCRIPT || status == REDIS_TRYAGAIN) {
        return rc;
    }


    //跳转重试
    //rc == REDIS_MOVED || rc == REDIS_ASK
    rc = redis_cluster_->Redirection(cluster_name, data, node_ip, status);
    if (rc != REDIS_OK) {
        return REDIS_ERROR;
    }

    return status;
}

