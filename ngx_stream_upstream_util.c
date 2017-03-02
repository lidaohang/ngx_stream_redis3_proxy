

#include "ngx_stream_upstream_util.h"

static ngx_stream_upstream_server_t*
ngx_stream_upstream_compare_server(ngx_stream_upstream_srv_conf_t * us, ngx_url_t u);
static ngx_stream_upstream_srv_conf_t *
ngx_stream_upstream_find_upstream(ngx_stream_session_t *s,  ngx_str_t *host);
static ngx_int_t
ngx_stream_upstream_exist_peer(ngx_stream_upstream_rr_peers_t * peers, ngx_url_t u);

static void *
ngx_prealloc_and_delay(ngx_pool_t *pool, void *p, size_t old_size, size_t new_size);


/*
*add upstream server
*
*upstream_name: upstream_name
*upstream_ip: 127.0.0.1:7005
*/
ngx_int_t
ngx_stream_upstream_add_server(ngx_stream_session_t *s, ngx_str_t upstream_name,
        ngx_str_t upstream_ip)
{
    ngx_int_t                          rc;
    ngx_stream_upstream_srv_conf_t     *uscf;
    ngx_stream_upstream_server_t       *us;
    ngx_url_t                          u;

    if ( upstream_name.len == 0 || upstream_ip.len == 0 ) {
        return NGX_ERROR;
    }

    uscf = ngx_stream_upstream_find_upstream(s, &upstream_name);
    if ( uscf == NULL ) {

        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_add_server upstream not found upstream_name=[%V]", &upstream_name);

        return NGX_ERROR;
    }
    ngx_memzero(&u, sizeof (ngx_url_t));

    u.url.len = upstream_ip.len;
    u.url.data = ngx_pcalloc(uscf->servers->pool, u.url.len);
    ngx_memcpy(u.url.data, upstream_ip.data, u.url.len);

    u.default_port = 80;
    u.uri_part = 0;
    u.no_resolve = 1;

    if (ngx_stream_upstream_compare_server(uscf, u) != NULL) {

        ngx_log_error(NGX_LOG_DEBUG, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_add_server this server is exist");

        return NGX_ERROR;
    }

    if (uscf->servers == NULL || uscf->servers->nelts == 0) {

        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_add_server upstream has no server before!");

        return NGX_ERROR;

    }

    rc = ngx_parse_url(uscf->servers->pool, &u);
    if ( rc != NGX_OK && u.err ) {

        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_add_server url parser error upstream_name=[%v],upstream_ip=[%V]",
                        &upstream_name, &upstream_ip);

        return NGX_ERROR;

    }

    us = ngx_array_push(uscf->servers);
    if (us == NULL) {
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_add_server us push uscf->servers failed");
        return NGX_ERROR;
    }

    ngx_memzero(us, sizeof (ngx_stream_upstream_server_t));

    /*
    us->name = u.url;
    us->addrs = u.addrs;
    us->naddrs = u.naddrs;
    us->weight = 0;
    us->max_fails = 6;
    us->fail_timeout = 0;
    */

    if (u.addrs && u.addrs[0].sockaddr) {

        us->name = u.host;

        //us->name = u.url;
        us->addrs = u.addrs;
        us->naddrs = u.naddrs;
        us->weight = 0;
        us->max_fails = 6;
        us->fail_timeout = 0;

        us->addrs->name = u.addrs[0].name;
        us->addrs->sockaddr = u.addrs[0].sockaddr;
        us->addrs->socklen = u.addrs[0].socklen;

    } else {
        //*err = "no host allowed";
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                "[upstream]   ngx_stream_upstream_add_server nohost allowed");

        return NGX_ERROR;
    }

    ngx_log_error(NGX_LOG_DEBUG, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_add_server server=[%V] is ok",&u.url);

    return NGX_OK;
}


/*
*add upstream server peer
*
*upstream_name: upstream_name
*upstream_ip: 127.0.0.1:7005
*/
ngx_int_t
ngx_stream_upstream_add_peer(ngx_stream_session_t *s, ngx_str_t upstream_name,
        ngx_str_t upstream_ip)
{
    ngx_uint_t                                n;
    ngx_stream_upstream_srv_conf_t            *uscf;
    ngx_stream_upstream_server_t              *us;
    ngx_stream_upstream_rr_peer_t             peer;
    ngx_stream_upstream_rr_peers_t            *peers;
    ngx_url_t                                 u;
    size_t                                    old_size, new_size;

    if ( upstream_name.len == 0 || upstream_ip.len == 0 ) {
        return NGX_ERROR;
    }

    uscf = ngx_stream_upstream_find_upstream(s, &upstream_name);
    if ( uscf == NULL ) {

        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_add_peer upstream not found upstream_name=[%V]", &upstream_name);

        return NGX_ERROR;
    }
    ngx_memzero(&u, sizeof (ngx_url_t));

    u.url.len = upstream_ip.len;
    u.url.data = upstream_ip.data;

    //u.url.data = ngx_pcalloc(uscf->servers->pool, u.url.len+1);
    //ngx_memcpy(u.url.data, upstream_ip.data, u.url.len);

    u.default_port = 80;

    us = ngx_stream_upstream_compare_server(uscf, u);
    if ( us == NULL) {

        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_add_peer server not found upstream_name=[%v],upstream_ip=[%V]",
                        &upstream_name, &upstream_ip);

        return NGX_ERROR;
    }

    peers = uscf->peer.data;
    ngx_memzero(&peer, sizeof (ngx_stream_upstream_rr_peer_t));


    if (ngx_stream_upstream_exist_peer(peers, u)) {

        ngx_log_error(NGX_LOG_DEBUG, s->connection->log, 0,
                    "[upstream]   ngx_stream_upstream_add_peer the peer is exist");

        return NGX_ERROR;
    }

    n = peers != NULL ? (peers->number - 1) : 0;

    old_size = n * sizeof(ngx_stream_upstream_rr_peer_t)
        + sizeof(ngx_stream_upstream_rr_peers_t);
    new_size = old_size + sizeof(ngx_stream_upstream_rr_peer_t);

    peers = ngx_prealloc_and_delay(ngx_cycle->pool, uscf->peer.data, old_size, new_size);
    if (peers == NULL) {

        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                    "[upstream]   ngx_stream_upstream_add_peer peers pcalloc fail");

        return NGX_ERROR;
    }

    peer.weight = us->weight;
    peer.effective_weight = us->weight;
    peer.current_weight= 0;
    peer.max_fails = us->max_fails;
    peer.fail_timeout = us->fail_timeout;
    peer.name = us->name;
    peer.sockaddr = us->addrs->sockaddr;
    peer.socklen = us->addrs->socklen;
    peer.name = us->addrs->name;
    peer.down = us->down;
    peer.fails = 0;
    //peer.server = us->name;

    peers->peer[peers->number++] = peer;
    peers->total_weight += peer.weight;
    peers->single = (peers->number == 1);
    peers->weighted = (peers->total_weight != peers->number);

    uscf->peer.data = peers;

    ngx_log_error(NGX_LOG_DEBUG, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_add_peer server=[%V] is ok",&u.url);

    return NGX_OK;
}


/*
*get upstream server peer
*
*upstream_name: upstream_name
*upstream_ip: 127.0.0.1:7005
*/
ngx_stream_upstream_rr_peer_t *
ngx_stream_upstream_get_peers(ngx_stream_session_t *s, ngx_str_t upstream_name,
        ngx_str_t upstream_ip)
{
    ngx_uint_t                              i;
    ngx_stream_upstream_rr_peers_t          *peers;
    ngx_stream_upstream_srv_conf_t          *us;
    ngx_url_t                               u;
    size_t                                  len;

    if ( upstream_name.len == 0 || upstream_ip.len == 0 ) {
         return NULL;
    }

    us = ngx_stream_upstream_find_upstream(s, &upstream_name);
    if ( us == NULL ) {

        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_get_peers upstream not found upstream_name=[%V]", &upstream_name);

         return NULL;
    }

    u.url.len = upstream_ip.len;
    u.url.data = upstream_ip.data;

    //u.url.data = ngx_pcalloc(us->servers->pool, u.url.len+1);
    //ngx_memcpy(u.url.data, upstream_ip.data, u.url.len);

    u.default_port = 80;

    peers = us->peer.data;

    if (peers == NULL) {

        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                        "[upstream]   ngx_stream_upstream_get_peers peer not found");

         return NULL;
    }

   for (i = 0; i < peers->number; i++) {

        len = peers->peer[i].name.len;
        if (len == u.url.len
            && ngx_memcmp(u.url.data, peers->peer[i].name.data, u.url.len) == 0) {
            return &peers->peer[i];
        }
    }

    return NULL;
}



static ngx_int_t
ngx_stream_upstream_exist_peer(ngx_stream_upstream_rr_peers_t * peers, ngx_url_t u)
{
    ngx_uint_t                          i;
    size_t                              len;
    ngx_stream_upstream_rr_peer_t       peer;

    if ( peers == NULL ) {
        return 0;
    }

    for (i = 0; i < peers->number; i++) {
        peer = peers->peer[i];

        len = peer.name.len;
        if (len == u.url.len
            && ngx_memcmp(u.url.data, peer.name.data, u.url.len) == 0) {
            return 1;;
        }
    }

    return 0;
}


static ngx_stream_upstream_srv_conf_t *
ngx_stream_upstream_find_upstream(ngx_stream_session_t *s,  ngx_str_t *host)
{
    ngx_uint_t                            i;
    ngx_stream_upstream_srv_conf_t        **uscfp, *uscf;
    ngx_stream_upstream_main_conf_t        *umcf;

    umcf = ngx_stream_get_module_main_conf(s, ngx_stream_upstream_module);
    uscfp = umcf->upstreams.elts;

    for (i = 0; i < umcf->upstreams.nelts; i++) {

        uscf = uscfp[i];

        if (uscf->host.len == host->len
            && ngx_memcmp(uscf->host.data, host->data, host->len) == 0) {
            return uscf;
        }
    }

    return NULL;
}

static ngx_stream_upstream_server_t*
ngx_stream_upstream_compare_server(ngx_stream_upstream_srv_conf_t * us, ngx_url_t u)
{
    ngx_uint_t                       i, j;
    size_t                           len;
    ngx_stream_upstream_server_t      *server = NULL;

    if (us->servers == NULL || us->servers->nelts == 0) {
        return NULL;
    }

    server = us->servers->elts;

    for (i = 0; i < us->servers->nelts; ++i) {
        for(j = 0; j < server[i].naddrs; ++j) {

            len = server[i].addrs[j].name.len;
            if (len == u.url.len
                 && ngx_memcmp(u.url.data, server[i].addrs[j].name.data, u.url.len) == 0) {

                return  &server[i];
            }
         }
    }

    return NULL;
}


ngx_stream_upstream_srv_conf_t *
ngx_stream_upstream_upstream_add(ngx_stream_session_t *s, ngx_url_t *url)
{

    ngx_uint_t                          i;
    ngx_stream_upstream_main_conf_t     *umcf;
    ngx_stream_upstream_srv_conf_t      **uscfp;


    umcf = ngx_stream_get_module_main_conf(s, ngx_stream_upstream_module);

    uscfp = umcf->upstreams.elts;

    for (i = 0; i < umcf->upstreams.nelts; i++) {

        if (uscfp[i]->host.len != url->host.len
            || ngx_strncasecmp(uscfp[i]->host.data, url->host.data,
               url->host.len) != 0)
        {
            ngx_log_error(NGX_LOG_DEBUG, s->connection->log, 0,
                    "[upstream] ngx_stream_upstream_upstream_add  upstream_add: host not match");

            continue;
        }

        if (uscfp[i]->port != url->port) {

            ngx_log_error(NGX_LOG_DEBUG, s->connection->log, 0,
                    "[upstream] ngx_stream_upstream_upstream_add  upstream_add: port not match");

            continue;
        }

        return uscfp[i];
    }

    ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
            "[upstream] ngx_stream_upstream_upstream_add  no upstream found");

    return NULL;
}


static void
ngx_stream_upstream_event_init(void *peers)
{
    ngx_time_t                                  *tp;
    ngx_delay_event_t                           *delay_event;


    delay_event = ngx_calloc(sizeof(*delay_event), ngx_cycle->log);
    if (delay_event == NULL) {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                "hngx_stream_upstream_event_init: calloc failed");
        return;
    }

    tp = ngx_timeofday();
    delay_event->start_sec = tp->sec;
    delay_event->start_msec = tp->msec;

    //delay_event->delay_delete_ev.handler = ngx_stream_upstream_add_delay_delete;
    delay_event->delay_delete_ev.log = ngx_cycle->log;
    delay_event->delay_delete_ev.data = delay_event;
    delay_event->delay_delete_ev.timer_set = 0;


    delay_event->data = peers;
    ngx_add_timer(&delay_event->delay_delete_ev, NGX_DELAY_DELETE);

    return;
}


static ngx_int_t
ngx_pfree_and_delay(ngx_pool_t *pool, void *p)
{
    ngx_pool_large_t  *l;

    for (l = pool->large; l; l = l->next) {
        if (p == l->alloc) {
            ngx_log_debug1(NGX_LOG_DEBUG_ALLOC, pool->log, 0,
                    "delay free: %p", l->alloc);

            ngx_stream_upstream_event_init(l->alloc);

            return NGX_OK;
        }
    }

    return NGX_DECLINED;
}



static void *
ngx_prealloc_and_delay(ngx_pool_t *pool, void *p, size_t old_size, size_t new_size)
{
    void                *new;
    ngx_pool_t          *node;

    if (p == NULL) {
        return ngx_palloc(pool, new_size);
    }

    if (new_size == 0) {
        if ((u_char *) p + old_size == pool->d.last) {
           pool->d.last = p;

        } else {
           ngx_pfree(pool, p);
        }

        return NULL;
    }

    if (old_size <= pool->max) {
        for (node = pool; node; node = node->d.next) {
            if ((u_char *)p + old_size == node->d.last
                && (u_char *)p + new_size <= node->d.end) {
                node->d.last = (u_char *)p + new_size;
                return p;
            }
        }
    }

    if (new_size <= old_size) {
       return p;
    }

    new = ngx_palloc(pool, new_size);
    if (new == NULL) {
        return NULL;
    }

    ngx_memcpy(new, p, old_size);

    ngx_pfree_and_delay(pool, p);

    return new;
}

