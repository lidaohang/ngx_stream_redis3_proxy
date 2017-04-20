/*
 * Author: lihang <lihanglucien@didichuxing.com>
 *
 * File: ngx_redis_protocol.h
 * Create Date: 2017-01-20 12:03:15
 *
 */

#ifndef NGX_STREAM_REDIS_PROXY_MODULE_H
#define NGX_STREAM_REDIS_PROXY_MODULE_H

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_stream.h>
#include "common.h"

typedef struct {
    unsigned                            eof:1;
    unsigned                            client_read:1;
    unsigned                            upstream_read:1;
    unsigned                            init_router:1;
    unsigned                            ask:1;
    unsigned                            moved:1;
    unsigned                            upstream:1;
    unsigned                            upstream_connect:1;
    ngx_int_t                           request_num;
    ngx_int_t                           slotid;
    ngx_str_t                           cluster_name;
    ngx_array_t                         *client_buffers;
    ngx_array_t                         *slotids;
    msg_type_t                          type;            /* message type */
    ngx_str_t                           node_ip;
    ngx_buf_t                           *asking;
    ngx_buf_t                           *cluster_nodes;
    ngx_buf_t                           *buffer_in;
} ngx_stream_redis_proxy_ctx_t;


extern ngx_module_t ngx_stream_redis_proxy_module;

#endif //NGX_STREAM_REDIS_PROXY_MODULE_H
