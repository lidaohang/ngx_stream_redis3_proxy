#ifndef __REDIS_CLUSTER_H__
#define __REDIS_CLUSTER_H__

#include "redis_node.h"

class  RedisCluster
{
public:
    RedisCluster(std::string backup_server);
    virtual ~RedisCluster();

public:
    /**
     *
     * 获得节点(key->slotid->node_ip)
    */
    int GetNode(const std::string &cluster_name,int &slot_id, std::string &node_ip);


    /**
     *
     * 处理moved,ask
    */
    int Redirection(const std::string &cluster_name,std::string &data, std::string &node_ip, int status);


    /**
     *
     * 获取cluster name
    */
    std::string GetClusterName();

    int InitClusterNodes(const std::string &cluster_name);

    /**
     *
     * 更新cluster nodes
    */
    int UpdateClusterNodes(const std::string &cluster_name,const std::string &data);

    /**
     *
     * 获取备用节点
    */
    std::string GetBackUpNode()
    {
        return backup_server_;
    }

protected:

    /**
     *
     * 设置cluster name
    */
    void SetClusterName(std::string cluster_name);

    /**
     *
     * 设置map slot
    */
    int SetMemNode(RedisNode *node,std::map<int,std::string> &slot_map);

    RedisNode* GetNode(const std::string& line);

    void AddSlotRange(RedisNode* node, char* slots);


private:
    std::map<std::string, std::map<int,std::string> > map_cluster_;
    std::string cluster_name_;

    std::string backup_server_;
};


#endif //__REDIS_CLUSTER_H__

