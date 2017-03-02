
#ifndef NGX_STREAM_UPSTREAM_UTIL_H
#define NGX_STREAM_UPSTREAM_UTIL_H


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_stream.h>



#define NGX_DELAY_DELETE 100 * 1000

typedef struct {
    ngx_event_t                              delay_delete_ev;

    time_t                                   start_sec;
    ngx_msec_t                               start_msec;

    void                                    *data;
} ngx_delay_event_t;


ngx_int_t
ngx_stream_upstream_add_server(ngx_stream_session_t *s,
        ngx_str_t upstream_name, ngx_str_t upstream_ip);

ngx_int_t
ngx_stream_upstream_add_peer(ngx_stream_session_t *r,
        ngx_str_t upstream_name, ngx_str_t upstream_ip);

ngx_stream_upstream_rr_peer_t *
ngx_stream_upstream_get_peers(ngx_stream_session_t *s,
        ngx_str_t upstream_name,ngx_str_t upstream_ip);


char *
ngx_stream_upstream_set_complex_value_slot(ngx_conf_t *cf,
        ngx_command_t *cmd, void *conf);

ngx_stream_upstream_srv_conf_t *
ngx_stream_upstream_upstream_add(ngx_stream_session_t *s, ngx_url_t *url);


#endif /* NGX_STREAM_UPSTREAM_UTIL_H */

