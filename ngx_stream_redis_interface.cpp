#include "ngx_stream_redis_interface.h"
#include <hiredis/hiredis.h>
#include "redis_node.h"
#include "ngx_redis_proto.h"

static std::map<std::string, std::map<std::string,ngx_stream_upstream_rr_peer_t *> > _upstream_peer_map;
static std::map<int, std::string> _slots_map;

static ngx_int_t
ngx_parse_cluster_nodes(const std::string &in);

ngx_int_t
ngx_stream_redis_init()
{
    redisContext* c = redisConnect("127.0.0.1", 8000);
    if (c->err) {
        return NGX_ERROR;
    }

    redisReply *reply = (redisReply *)redisCommand(c,"CLUSTER NODES");
    ngx_parse_cluster_nodes(reply->str);

    freeReplyObject(reply);
    redisFree(c);

    return NGX_OK;
}


static std::vector<std::string>
ngx_string_split(const std::string& text, const std::string &sepStr, bool ignoreEmpty)
{
    std::vector<std::string> vec;
    std::string str(text);
    std::string sep(sepStr);
    size_t n = 0, old = 0;
    while (n != std::string::npos)
    {
        n = str.find(sep,n);
        if (n != std::string::npos)
        {
            if (!ignoreEmpty || n != old)
                vec.push_back(str.substr(old, n-old));
            n += sep.length();
            old = n;
        }
    }

    if (!ignoreEmpty || old < str.length()) {
        vec.push_back(str.substr(old, str.length() - old));
    }

    return vec;
}


static ngx_int_t
ngx_set_mem_node(RedisNode *node, std::map<int,std::string> &slot_map)
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


static void
ngx_add_slot_range(RedisNode* node, char* slots)
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



static RedisNode*
ngx_get_node(const std::string& line)
{
    std::vector<std::string> tokens = ngx_string_split(line," ", true);
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
            ngx_add_slot_range(node, (char*)tokens[i].c_str());
    }
    else if (strcasecmp(node_role, "slave") == 0) {
        node->SetRole("slave");
    }
    else if (strcasecmp(node_role, "handshake") == 0) {

        node->SetRole("master"); //handshake == master
        size_t n = tokens.size();
        for (size_t i = CLUSTER_NODES_LENGTH; i < n; i++)
            ngx_add_slot_range(node, (char*)tokens[i].c_str());
    }

    return node;
}

// d52ea3cb4cdf7294ac1fb61c696ae6483377bcfc 127.0.0.1:7000 master - 0 1428410625374 73 connected 5461-10922
// 94e5d32cbcc9539cc1539078ca372094c14f9f49 127.0.0.1:7001 myself,master - 0 0 1 connected 0-9 11-5460
// e7b21f65e8d0d6e82dee026de29e499bb518db36 127.0.0.1:7002 slave d52ea3cb4cdf7294ac1fb61c696ae6483377bcfc 0 1428410625373 73 connected
// 6a78b47b2e150693fc2bed8578a7ca88b8f1e04c 127.0.0.1:7003 myself,slave 94e5d32cbcc9539cc1539078ca372094c14f9f49 0 0 4 connected
// 70a2cd8936a3d28d94b4915afd94ea69a596376a :7004 myself,master - 0 0 0 connected
static ngx_int_t
ngx_parse_cluster_nodes(const std::string &in)
{
    int                         rc;
    std::vector<RedisNode*>     list;

    std::vector<std::string> vector_line = ngx_string_split(in, "\n", true);
    size_t vector_line_len = vector_line.size();

    for (size_t i =0; i < vector_line_len; ++i ) {

        RedisNode *node = ngx_get_node(vector_line[i]);
        if ( node == NULL ) {
            continue;
        }

        rc = ngx_set_mem_node(node, _slots_map);
        if ( rc != REDIS_OK ) {
            return REDIS_ERROR;
        }

        delete node;
        node = NULL;

    }

    return REDIS_OK;
}

ngx_int_t
ngx_stream_redis_destroy()
{
    return NGX_OK;
}


ngx_int_t
ngx_stream_redis_asking(ngx_stream_session_t *s)
{
    size_t                              len;
    static ngx_str_t                    asking = ngx_string("*1\r\n$6\r\nASKING\r\n");
    ngx_connection_t                    *c;
    ngx_stream_redis_proxy_ctx_t        *ctx;

    c = s->connection;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    // 由于redis必须是同一个连接发送asking, 所以这里暂时用管道的方式解决，可能存在的问题就是响应数据的解析问题
    len = asking.len + (ctx->buffer_in->last - ctx->buffer_in->pos);
    ctx->asking = ngx_create_temp_buf(c->pool, len);
    if (ctx->asking == NULL) {
        return NGX_ERROR;
    }
    ngx_memcpy(ctx->asking->last, strcat((char*)asking.data, (char*)ctx->buffer_in->pos), len);

    return NGX_OK;
}


ngx_int_t
ngx_stream_redis_process_request(ngx_stream_session_t *s)
{
//    int                                 rc;
    std::string                         node_ip;
    static ngx_str_t                    upstream_name =  ngx_string("backend");
    ngx_stream_redis_proxy_ctx_t        *ctx;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }
    ctx->cluster_name = upstream_name;
    node_ip = _slots_map[ctx->slotid];

    ctx->node_ip.data = (u_char*)node_ip.c_str();
    ctx->node_ip.len = node_ip.length();

    return NGX_OK;
}

//process ASK || -MOVED 1 127.0.0.1:7000，update memory slotid
static ngx_int_t
ngx_redis_redirection(ngx_stream_session_t *s, ngx_buf_t *b)
{
    char                                *data;
    ssize_t                             len;
    std::vector<std::string>            vector_line;
    std::string                         node_ip;
    ngx_stream_redis_proxy_ctx_t        *ctx;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    len = b->last - b->pos;
    data = (char*) b->pos;

    vector_line = ngx_string_split(std::string(data, len), " ", true);
    if ( vector_line.size() < 3 ) {
        return REDIS_ERROR;
    }

    std::vector<std::string> vector_line_node = ngx_string_split(vector_line[2], "\r\n", true);
    node_ip =  vector_line_node[0];

    if ( ctx->type == MSG_RSP_REDIS_ERROR_ASK ) {
        return REDIS_OK;
    }

    // MSG_RSP_REDIS_ERROR_MOVED
    ctx->slotid = ngx_atoi((u_char*)vector_line[1].c_str(), vector_line[1].length());
    ctx->node_ip.data = (u_char*) node_ip.c_str();
    ctx->node_ip.len = node_ip.length();

    _slots_map[ctx->slotid] = node_ip;

    return REDIS_OK;
}


ngx_int_t
ngx_stream_redis_process_response(ngx_stream_session_t *s, ngx_buf_t *b)
{
    int                                 rc;
    ngx_stream_redis_proxy_ctx_t        *ctx;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    // type == REDIS_TRYAGAIN || type == REDIS_MOVED || type == REDIS_ASK
    if ( ctx->type == MSG_RSP_REDIS_ERROR_ASK  || ctx->type == MSG_RSP_REDIS_ERROR_MOVED ) {
        rc = ngx_redis_redirection(s, b);
        if (rc != REDIS_OK) {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

ngx_int_t
ngx_stream_redis_upstream_set_peer(ngx_str_t cluster_name, ngx_str_t node_ip, ngx_stream_upstream_rr_peer_t *peer)
{
    std::string                         str_cluster_name;
    std::string                         str_node_ip;

    str_cluster_name = std::string((char*)cluster_name.data, cluster_name.len);
    str_node_ip = std::string((char*)node_ip.data, node_ip.len);

    _upstream_peer_map[str_cluster_name][str_node_ip] = peer;

    return NGX_OK;
}

ngx_stream_upstream_rr_peer_t *
ngx_stream_redis_upstream_get_peer(ngx_str_t cluster_name, ngx_str_t node_ip)
{
    std::string                         str_cluster_name;
    std::string                         str_node_ip;

    str_cluster_name = std::string((char*)cluster_name.data, cluster_name.len);
    str_node_ip = std::string((char*)node_ip.data, node_ip.len);

    return _upstream_peer_map[str_cluster_name][str_node_ip];
}


