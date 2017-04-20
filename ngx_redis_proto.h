#ifndef __NGX_REDIS_PROTO_H__
#define __NGX_REDIS_PROTO_H__

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_stream.h>

#include "ngx_stream_redis_proxy_module.h"

ngx_int_t
redis_parse_req(ngx_stream_session_t *s);

ngx_int_t
redis_parse_rsp(ngx_stream_session_t *s);

#endif //__NGX_REDIS_PROTO_H__

