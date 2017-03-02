#include "ngx_stream_redis_interface.h"
#include "redis_handler.h"

RedisHandler     *redis_handler_;
static std::map<std::string, std::map<std::string,ngx_stream_upstream_rr_peer_t *> > _upstream_peer_map;

ngx_int_t
ngx_stream_redis_init()
{
    if (redis_handler_ == NULL) {
        redis_handler_ = new RedisHandler();
        redis_handler_->Init();
    }

    return NGX_OK;
}

ngx_int_t
ngx_stream_redis_destroy()
{
    if (redis_handler_) {
        redis_handler_->Exit();
        delete redis_handler_;
        redis_handler_ = NULL;
    }

    return NGX_OK;
}

ngx_int_t
ngx_stream_redis_cluster_nodes(ngx_stream_session_t *s)
{
    static ngx_str_t                    cluster_nodes = ngx_string("*2\r\n$7\r\ncluster\r\n$5\r\nnodes\r\n");
    ngx_connection_t                    *c;
    ngx_stream_redis_proxy_ctx_t        *ctx;

    c = s->connection;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ctx->cluster_nodes = ngx_create_temp_buf(c->pool, cluster_nodes.len);
    if (ctx->cluster_nodes == NULL) {
        return NGX_ERROR;
    }
    ngx_memcpy(ctx->cluster_nodes->last, cluster_nodes.data, cluster_nodes.len);

    ctx->cluster_nodes->last += cluster_nodes.len;

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
ngx_stream_redis_process_request(ngx_stream_session_t *s, ngx_buf_t *b)
{
    int                                 rc;
    char                                *data;
    ssize_t                             len;
    std::string                         body;
    std::string                         cluster_name;
    std::string                         node_ip;
    ngx_stream_redis_proxy_ctx_t        *ctx;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    static ngx_str_t  test =  ngx_string("backend");
    ctx->cluster_name = test;

    len = b->last - b->pos;
    data = (char*) b->pos;
    body = std::string(data, len);
    cluster_name = std::string((char*)ctx->cluster_name.data, ctx->cluster_name.len);

    rc = redis_handler_->ProcessRequest(cluster_name, body, node_ip);
    if (rc == REDIS_CLUSTER_NODES) {
        ctx->init_router = 1;
        return NGX_OK;
    }
    ctx->init_router = 0;
    ctx->node_ip.data = (u_char*)node_ip.c_str();
    ctx->node_ip.len = node_ip.length();

    return NGX_OK;
}

ngx_int_t
ngx_stream_redis_process_response(ngx_stream_session_t *s, ngx_buf_t *b)
{
    int                                 rc;
    char                                *data;
    ssize_t                             len;
    std::string                         body;
    std::string                         cluster_name;
    std::string                         node_ip;
    ngx_stream_redis_proxy_ctx_t        *ctx;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    len = b->last - b->pos;
    data = (char*) b->pos;
    body = std::string(data, len);
    cluster_name = std::string((char*)ctx->cluster_name.data, ctx->cluster_name.len);

    //初始化路由表
    if (ctx->init_router) {
        rc =  redis_handler_->UpdateRoute(cluster_name, body);
        if (rc != REDIS_OK) {
            return NGX_ERROR;
        }

        //初始化成功
        ctx->init_router = 0;

        //重试
        ctx->moved = 1;

        return NGX_OK;
    }

    rc = redis_handler_->ProcessResponse("backup_server", body, node_ip);
    if (rc == REDIS_ERROR) {
        //内部出错
        return NGX_ERROR;
    }

    // rc == REDIS_TRYAGAIN || rc == REDIS_MOVED || rc == REDIS_ASK
    //需要重试
    ctx->moved = (rc == REDIS_TRYAGAIN) ? 1 : 0;
    ctx->moved = (rc == REDIS_MOVED) ? 1 : 0;
    ctx->ask = (rc == REDIS_ASK) ? 1 : 0;

    ctx->node_ip.data = (u_char*)node_ip.c_str();
    ctx->node_ip.len = node_ip.length();

    // rc == REDIS_OK || rc == REDIS_NOSCRIPT
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

