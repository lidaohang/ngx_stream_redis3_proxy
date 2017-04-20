#ifndef NGX_UPSTREAM_INTERFACE_H
#define NGX_UPSTREAM_INTERFACE_H

#include "ngx_stream_redis_interface.h"

#if __cplusplus
extern "C" {
#endif

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_stream.h>

#include "ngx_stream_redis_proxy_module.h"

ngx_int_t ngx_stream_redis_init();
ngx_int_t ngx_stream_redis_destroy();


ngx_int_t ngx_stream_redis_asking(ngx_stream_session_t *s);

ngx_int_t ngx_stream_redis_process_request(ngx_stream_session_t *s);
ngx_int_t ngx_stream_redis_process_response(ngx_stream_session_t *s, ngx_buf_t *b);

ngx_int_t
ngx_stream_redis_upstream_set_peer(ngx_str_t cluster_name, ngx_str_t node_ip, ngx_stream_upstream_rr_peer_t *peer);
ngx_stream_upstream_rr_peer_t *
ngx_stream_redis_upstream_get_peer(ngx_str_t cluster_name, ngx_str_t node_ip);

#if __cplusplus
}
#endif


#endif //NGX_UPSTREAM_INTERFACE_H

