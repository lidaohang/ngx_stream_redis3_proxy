/*
 * Author: lihang <lihanglucien@didichuxing.com>
 *
 * File: ngx_redis_protocol.h
 * Create Date: 2017-01-20 12:03:15
 *
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_stream.h>

#include "ngx_stream_upstream_util.h"
#include "ngx_stream_redis_interface.h"

typedef struct {
    /* the round robin data must be first */
    ngx_stream_upstream_rr_peer_data_t    rrp;
    ngx_str_t                             key;
    ngx_uint_t                            tries;
    ngx_event_get_peer_pt                 get_rr_peer;
    ngx_stream_session_t                  *s;
} ngx_stream_upstream_redis_peer_data_t;


static ngx_int_t ngx_stream_upstream_init_redis(ngx_conf_t *cf,
    ngx_stream_upstream_srv_conf_t *us);
static ngx_int_t ngx_stream_upstream_init_redis_peer(ngx_stream_session_t *s,
    ngx_stream_upstream_srv_conf_t *us);
static ngx_int_t ngx_stream_upstream_get_redis_peer(ngx_peer_connection_t *pc,
    void *data);

static char *ngx_stream_upstream_redis(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_command_t  ngx_stream_upstream_redis_commands[] = {

    { ngx_string("redis_cluster"),
      NGX_STREAM_UPS_CONF|NGX_CONF_NOARGS,
      ngx_stream_upstream_redis,
      NGX_STREAM_SRV_CONF_OFFSET,
      0,
      NULL },

      ngx_null_command
};


static ngx_stream_module_t  ngx_stream_upstream_redis_module_ctx = {
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                   /* create server configuration */
    NULL,                                  /* merge server configuration */
};


ngx_module_t  ngx_stream_upstream_redis_module = {
    NGX_MODULE_V1,
    &ngx_stream_upstream_redis_module_ctx,  /* module context */
    ngx_stream_upstream_redis_commands,     /* module directives */
    NGX_STREAM_MODULE,                     /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_stream_upstream_init_redis(ngx_conf_t *cf,
    ngx_stream_upstream_srv_conf_t *us)
{
    if (ngx_stream_upstream_init_round_robin(cf, us) != NGX_OK) {
        return NGX_ERROR;
    }

    us->peer.init = ngx_stream_upstream_init_redis_peer;

    return NGX_OK;
}


static ngx_int_t
ngx_stream_upstream_init_redis_peer(ngx_stream_session_t *s,
    ngx_stream_upstream_srv_conf_t *us)
{
    ngx_stream_upstream_redis_peer_data_t  *hp;

    hp = ngx_palloc(s->connection->pool,
                    sizeof(ngx_stream_upstream_redis_peer_data_t));
    if (hp == NULL) {
        return NGX_ERROR;
    }

    s->upstream->peer.data = &hp->rrp;

    if (ngx_stream_upstream_init_round_robin_peer(s, us) != NGX_OK) {
        return NGX_ERROR;
    }

    s->upstream->peer.get = ngx_stream_upstream_get_redis_peer;

    hp->key = s->connection->addr_text;
    hp->s = s;

    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, s->connection->log, 0,
                   "[redis_proxy] upstream key:\"%V\"", &hp->key);

    hp->tries = 0;
    hp->get_rr_peer = ngx_stream_upstream_get_round_robin_peer;

    return NGX_OK;
}


static ngx_int_t
ngx_stream_upstream_get_redis_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_stream_upstream_redis_peer_data_t *hp = data;

    time_t                          now;
    ngx_time_t                      *tp;
    ngx_uint_t                      rand_num;
    ngx_stream_upstream_rr_peer_t  *peer;
    ngx_stream_upstream_rr_peers_t *peers;
    ngx_stream_session_t           *s;
    ngx_stream_redis_proxy_ctx_t   *ctx;

    s = hp->s;

    ctx = (ngx_stream_redis_proxy_ctx_t *)ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, pc->log, 0,
                   "[redis_proxy] get  peer, try: %ui", pc->tries);

    ngx_stream_upstream_rr_peers_wlock(hp->rrp.peers);

//    if (hp->tries > 20 || hp->rrp.peers->single) {
//        ngx_stream_upstream_rr_peers_unlock(hp->rrp.peers);
//        return hp->get_rr_peer(pc, &hp->rrp);
//    }

    now = ngx_time();

    pc->connection = NULL;
    peers = hp->rrp.peers;

    if (ctx->node_ip.len == 0) {
        tp = ngx_timeofday();
        srand(tp->msec);
        rand_num = rand() % hp->rrp.peers->number;
        peer = &hp->rrp.peers->peer[rand_num];

        ctx->node_ip = peer->name;
        goto found;
    }

    //peer = ngx_stream_redis_upstream_get_peer(ctx->cluster_name, ctx->node_ip);
    peer = ngx_stream_upstream_get_peers(s, ctx->cluster_name, ctx->node_ip);
    if (peer == NULL || peer->name.data == NULL || peer->name.len == 0) {
        peer = ngx_stream_upstream_get_peers(s, ctx->cluster_name, ctx->node_ip);
        if (peer == NULL) {
            ngx_stream_upstream_add_server(s, ctx->cluster_name, ctx->node_ip);
            ngx_stream_upstream_add_peer(s, ctx->cluster_name, ctx->node_ip);

            peer = ngx_stream_upstream_get_peers(s, ctx->cluster_name, ctx->node_ip);
            if (peer == NULL) {
                return NGX_ERROR;
            }
        }

        //ngx_stream_redis_upstream_set_peer(ctx->cluster_name, ctx->node_ip, peer);
    }

found:
    hp->tries++;
    hp->rrp.current = peer;

    pc->sockaddr = peer->sockaddr;
    pc->socklen = peer->socklen;
    pc->name = &peer->name;

    peer->conns++;

    if (now - peer->checked > peer->fail_timeout) {
        peer->checked = now;
    }

    ngx_stream_upstream_rr_peers_unlock(hp->rrp.peers);

    return NGX_OK;
}


static char *
ngx_stream_upstream_redis(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_stream_upstream_srv_conf_t  *uscf;

    uscf = ngx_stream_conf_get_module_srv_conf(cf, ngx_stream_upstream_module);

    if (uscf->peer.init_upstream) {
        ngx_conf_log_error(NGX_LOG_WARN, cf, 0,
                           "[redis_proxy] load balancing method redefined");
    }

    uscf->flags = NGX_STREAM_UPSTREAM_CREATE
                  |NGX_STREAM_UPSTREAM_WEIGHT
                  |NGX_STREAM_UPSTREAM_MAX_FAILS
                  |NGX_STREAM_UPSTREAM_FAIL_TIMEOUT
                  |NGX_STREAM_UPSTREAM_DOWN;

    uscf->peer.init_upstream = ngx_stream_upstream_init_redis;

    return NGX_CONF_OK;
}
