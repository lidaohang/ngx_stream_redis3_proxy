#include "redis_cluster.h"
#include "string_util.h"
#include "redis_node.h"

RedisCluster::RedisCluster(std::string backup_server):
    backup_server_(backup_server)
{
}

RedisCluster::~RedisCluster()
{
}

//判断是否初始化cluster nodes信息
int RedisCluster::InitClusterNodes(const std::string &cluster_name)
{
    std::map<std::string, std::map<int, std::string> >::const_iterator it;
    it = map_cluster_.find(cluster_name);
    if( it == map_cluster_.end() ) {
        return REDIS_CLUSTER_NODES;
    }

    return REDIS_OK;
}


//根据请求的key得到节点ip  key->slot->node_ip
int RedisCluster::GetNode(const std::string &cluster_name,int &slot_id, std::string &node_ip)
{
    std::map<std::string, std::map<int, std::string> >::const_iterator it;
    it = map_cluster_.find(cluster_name);
    if( it == map_cluster_.end() ) {
        return REDIS_ERROR;
    }

    std::map<int, std::string>::const_iterator it_slot;
    it_slot = it->second.find(slot_id);
    if( it_slot != it->second.end() ) {
        node_ip = it_slot->second;
        return REDIS_OK;
    }

    return REDIS_ERROR;
}

//process ASK || -MOVED 1 127.0.0.1:7000，update memory slotid
int RedisCluster::Redirection(const std::string &cluster_name,std::string &data, std::string &node_ip, int status)
{
    int                         slot_id;
    std::vector<std::string>    vector_line;

    vector_line = StringUtil::Split(data, " ");
    if ( vector_line.size() < 3 ) {
        return REDIS_ERROR;
    }

    std::vector<std::string> vector_line_node = StringUtil::Split(vector_line[2], "\r\n");
    node_ip =  vector_line_node[0];

    if ( status == REDIS_ASK ) {
        return REDIS_OK;
    }

    // REDIS_MOVED
    StringUtil::StrToInt32(vector_line[1].c_str(),slot_id);
    map_cluster_[cluster_name][slot_id] = node_ip;

    return REDIS_OK;
}

// d52ea3cb4cdf7294ac1fb61c696ae6483377bcfc 127.0.0.1:7000 master - 0 1428410625374 73 connected 5461-10922
// 94e5d32cbcc9539cc1539078ca372094c14f9f49 127.0.0.1:7001 myself,master - 0 0 1 connected 0-9 11-5460
// e7b21f65e8d0d6e82dee026de29e499bb518db36 127.0.0.1:7002 slave d52ea3cb4cdf7294ac1fb61c696ae6483377bcfc 0 1428410625373 73 connected
// 6a78b47b2e150693fc2bed8578a7ca88b8f1e04c 127.0.0.1:7003 myself,slave 94e5d32cbcc9539cc1539078ca372094c14f9f49 0 0 4 connected
// 70a2cd8936a3d28d94b4915afd94ea69a596376a :7004 myself,master - 0 0 0 connected
int RedisCluster::UpdateClusterNodes(const std::string &cluster_name,const std::string &in)
{
    int                         rc;
    std::map<int,std::string>   slot_map;
    std::vector<RedisNode*>     list;

    std::vector<std::string> vector_line = StringUtil::Split(in, "\n");
    size_t vector_line_len = vector_line.size();

    for (size_t i =0; i < vector_line_len; ++i ) {

        RedisNode *node = GetNode(vector_line[i]);
        if ( node == NULL ) {
            continue;
        }

        rc = SetMemNode(node, slot_map);
        if ( rc != REDIS_OK ) {
            return REDIS_ERROR;
        }

        delete node;
        node = NULL;

    }
    map_cluster_.insert(std::make_pair(cluster_name, slot_map));

    return REDIS_OK;
}

int RedisCluster::SetMemNode(RedisNode *node, std::map<int,std::string> &slot_map)
{
    std::string id(node->GetId());
    std::string addr(node->GetAddr());
    std::string sid(node->GetSid());

    if ( !node->IsConnected() ) {
        return REDIS_OK;
    }

    std::vector<std::pair<size_t, size_t> > slots = node->GetSlots();
    size_t slots_len = slots.size();

    for( size_t j =0; j < slots_len; ++j ) {

        for( size_t slot_num = slots[j].first; slot_num <= slots[j].second; ++ slot_num ) {
            //key:slot_num          value:addr
            slot_map[slot_num] = addr;

        }
    }

    return REDIS_OK;
}

RedisNode* RedisCluster::GetNode(const std::string& line)
{
    std::vector<std::string> tokens = StringUtil::Split(line," ");
    if ( tokens.size() < CLUSTER_NODES_LENGTH ) {
        return NULL;
    }

    bool myself = false;
    char* node_role = (char*)tokens[CLUSTER_NODES_ROLE].c_str();
    char* ptr = strchr(node_role, ',');
    if (ptr != NULL && *(ptr + 1) != 0) {
        *ptr++ = 0;
        if (strcasecmp(node_role, "myself") == 0)
            myself = true;
        node_role = ptr;
    }

    RedisNode* node = new RedisNode;
    node->SetId(tokens[CLUSTER_NODES_ID].c_str());
    node->SetAddr(tokens[CLUSTER_NODES_ADDR].c_str());
    node->SetSid(tokens[CLUSTER_NODES_SID].c_str());
    node->SetMyself(myself);
    node->SetConnected(strcasecmp(tokens[CLUSTER_NODES_CONNECTED].c_str(), "connected") == 0);

    if (strcasecmp(node_role, "master") == 0) {

        node->SetRole("master");
        size_t n = tokens.size();
        for (size_t i = CLUSTER_NODES_LENGTH; i < n; i++)
            AddSlotRange(node, (char*)tokens[i].c_str());
    }
    else if (strcasecmp(node_role, "slave") == 0) {
        node->SetRole("slave");
    }
    else if (strcasecmp(node_role, "handshake") == 0) {

        node->SetRole("master"); //handshake == master
        size_t n = tokens.size();
        for (size_t i = CLUSTER_NODES_LENGTH; i < n; i++)
            AddSlotRange(node, (char*)tokens[i].c_str());
    }

    return node;
}

void RedisCluster::AddSlotRange(RedisNode* node, char* slots)
{
    size_t slot_min, slot_max;

    char* ptr = strchr(slots, '-');
    if (ptr != NULL && *(ptr + 1) != 0) {

        *ptr++ = 0;
        slot_min = (size_t) atol(slots);
        slot_max = (size_t) atol(ptr);
        // xxx
        if (slot_max < slot_min)
            slot_max = slot_min;
    }
    else {
        slot_min = (size_t) atol(slots);
        slot_max = slot_min;
    }

    node->AddSlotRange(slot_min, slot_max);
}

void RedisCluster::SetClusterName(std::string cluster_name)
{
    cluster_name_ = cluster_name;
}

std::string RedisCluster::GetClusterName()
{
    return cluster_name_;
}

