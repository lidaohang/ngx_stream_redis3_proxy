/*
 * Author: lihang <lihanglucien@didichuxing.com>
 *
 * File: ngx_redis_protocol.h
 * Create Date: 2017-01-20 12:03:15
 *
 */

#ifndef NGX_REDIS_PROTOCOL_H
#define NGX_REDIS_PROTOCOL_H

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_stream.h>

#include "ngx_stream_redis_proxy_module.h"

#define REDIS_CLUSTER  "CLUSTER NODES"








ngx_int_t
ngx_redis_proto_parse(ngx_stream_session_t *s, ngx_buf_t *b);


#endif //NGX_REDIS_PROTOCOL_H
