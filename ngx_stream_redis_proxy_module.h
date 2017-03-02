
#ifndef NGX_STREAM_REDIS_PROXY_MODULE_H
#define NGX_STREAM_REDIS_PROXY_MODULE_H

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_stream.h>

typedef struct {
    unsigned                            eof:1;
    unsigned                            read:1;
    unsigned                            init_router:1;
    unsigned                            ask:1;
    unsigned                            moved:1;
    unsigned                            upstream:1;
    size_t                              slotid;
    ngx_str_t                           cluster_name;
    ngx_str_t                           node_ip;
    ngx_buf_t                           *asking;
    ngx_buf_t                           *cluster_nodes;
    ngx_buf_t                           *buffer_in;
} ngx_stream_redis_proxy_ctx_t;


extern ngx_module_t ngx_stream_redis_proxy_module;

#endif //NGX_STREAM_REDIS_PROXY_MODULE_H
