/*
 * Author: lihang <lihanglucien@didichuxing.com>
 *
 * File: ngx_redis_protocol.h
 * Create Date: 2017-01-20 12:03:15
 *
 */

#include "ngx_stream_redis_proxy_module.h"
#include "ngx_stream_redis_interface.h"
#include "common.h"
#include "ngx_redis_proto.h"

typedef struct {

    ngx_msec_t                       client_read_timeout;
    ngx_msec_t                       upstream_read_timeout;
    ngx_msec_t                       connect_timeout;
    ngx_msec_t                       timeout;
    ngx_msec_t                       next_upstream_timeout;
    size_t                           buffer_size;
    size_t                           upload_rate;
    size_t                           download_rate;
    ngx_uint_t                       responses;
    ngx_uint_t                       next_upstream_tries;
    ngx_flag_t                       next_upstream;
    ngx_flag_t                       proxy_protocol;
    ngx_addr_t                      *local;

    ngx_stream_upstream_srv_conf_t  *upstream;
} ngx_stream_redis_proxy_srv_conf_t;


static ngx_int_t
ngx_stream_redis_proxy_init_process(ngx_cycle_t *cycle);
static void
ngx_stream_redis_proxy_exit_process(ngx_cycle_t *cycle);


static void
ngx_stream_redis_read_request_handler(ngx_event_t *rev);
static void
ngx_stream_redis_write_request_handler(ngx_event_t *wev);

static void ngx_stream_redis_proxy_connect_handler(ngx_event_t *ev);

static void ngx_stream_redis_proxy_upstream_read_handler(ngx_event_t *rev);
static void ngx_stream_redis_proxy_upstream_write_handler(ngx_event_t *rev);


static void ngx_stream_redis_proxy_handler(ngx_stream_session_t *s);
static void ngx_stream_redis_proxy_connect(ngx_stream_session_t *s);
static void ngx_stream_redis_proxy_init_upstream(ngx_stream_session_t *s);
static void ngx_stream_redis_proxy_process_connection(ngx_event_t *ev,
    ngx_uint_t from_upstream);
static ngx_int_t ngx_stream_redis_proxy_test_connect(ngx_connection_t *c);

static void ngx_stream_redis_proxy_next_upstream(ngx_stream_session_t *s);
static void ngx_stream_redis_proxy_finalize(ngx_stream_session_t *s, ngx_int_t rc);
static u_char *ngx_stream_redis_proxy_log_error(ngx_log_t *log, u_char *buf,
    size_t len);

static void *ngx_stream_redis_proxy_create_srv_conf(ngx_conf_t *cf);
static char *ngx_stream_redis_proxy_merge_srv_conf(ngx_conf_t *cf, void *parent,
    void *child);
static char *ngx_stream_redis_proxy_pass(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static ngx_conf_deprecated_t  ngx_conf_deprecated_proxy_downstream_buffer = {
    ngx_conf_deprecated, "proxy_downstream_buffer", "proxy_buffer_size"
};

static ngx_conf_deprecated_t  ngx_conf_deprecated_proxy_upstream_buffer = {
    ngx_conf_deprecated, "proxy_upstream_buffer", "proxy_buffer_size"
};


static ngx_command_t  ngx_stream_redis_proxy_commands[] = {

    { ngx_string("redis_proxy_pass"),
      NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_stream_redis_proxy_pass,
      NGX_STREAM_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("redis_proxy_connect_timeout"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_redis_proxy_srv_conf_t, connect_timeout),
      NULL },

    { ngx_string("redis_client_read_timeout"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_redis_proxy_srv_conf_t, client_read_timeout),
      NULL },

    { ngx_string("redis_proxy_timeout"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_redis_proxy_srv_conf_t, timeout),
      NULL },

    { ngx_string("redis_proxy_read_timeout"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_redis_proxy_srv_conf_t, upstream_read_timeout),
      NULL },

    { ngx_string("redis_proxy_buffer_size"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_redis_proxy_srv_conf_t, buffer_size),
      NULL },

    { ngx_string("redis_proxy_downstream_buffer"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_redis_proxy_srv_conf_t, buffer_size),
      &ngx_conf_deprecated_proxy_downstream_buffer },

    { ngx_string("redis_proxy_upstream_buffer"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_redis_proxy_srv_conf_t, buffer_size),
      &ngx_conf_deprecated_proxy_upstream_buffer },

    { ngx_string("redis_proxy_next_upstream"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_FLAG,
      ngx_conf_set_flag_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_redis_proxy_srv_conf_t, next_upstream),
      NULL },

    { ngx_string("redis_proxy_next_upstream_tries"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_redis_proxy_srv_conf_t, next_upstream_tries),
      NULL },

    { ngx_string("redis_proxy_next_upstream_timeout"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_redis_proxy_srv_conf_t, next_upstream_timeout),
      NULL },

      ngx_null_command
};


static ngx_stream_module_t  ngx_stream_redis_proxy_module_ctx = {
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    ngx_stream_redis_proxy_create_srv_conf,      /* create server configuration */
    ngx_stream_redis_proxy_merge_srv_conf        /* merge server configuration */
};


ngx_module_t  ngx_stream_redis_proxy_module = {
    NGX_MODULE_V1,
    &ngx_stream_redis_proxy_module_ctx,          /* module context */
    ngx_stream_redis_proxy_commands,             /* module directives */
    NGX_STREAM_MODULE,                     /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    ngx_stream_redis_proxy_init_process,   /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    ngx_stream_redis_proxy_exit_process,   /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_stream_redis_proxy_send_buffer(ngx_stream_session_t *s,ngx_connection_t *dst,
        ngx_buf_t *b, ngx_msec_t send_timeout)
{
    size_t                          size;
    ssize_t                         n = 0;
    ngx_connection_t                *c;

    c = s->connection;

    for ( ;; ) {

        size = b->last - b->pos;

        // 条件是有数据，且dst连接是可写的
        //if (size && dst && dst->write->ready) {
        if (size && dst ) {

            if (size <= 0) {
                 ngx_log_error(NGX_LOG_ERR, dst->log, 0,
                         "[stream_redis_proxy] send buffer_size is too small for"
                         "the request");

                 return NGX_ERROR;
             }

             n = dst->send(dst, b->pos, size);

             ngx_log_debug1(NGX_LOG_DEBUG_STREAM, dst->log, 0,
                     "[stream_redis_proxy] send returns %z", n);

             if (n > 0) {
                 b->pos += n;

                 if (b->pos == b->last) {

                     b->pos = b->start;
                     b->last = b->start;

                    break;
                 }

                 continue;
             }
        }
        break;
    }//

    if (n == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, dst->log, 0,
                "[stream_redis_proxy] n is  error");

        ngx_stream_redis_proxy_finalize(s, NGX_DECLINED);
        return NGX_DONE;
    }

    if ( n == NGX_AGAIN ) {

        if (!dst->write->timer_set) {
            ngx_add_timer(dst->write, send_timeout);
        }

        if ( !dst->shared && ngx_handle_write_event(dst->write, 0) != NGX_OK) {
            return NGX_ERROR;
        }

        return NGX_AGAIN;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_stream_redis_proxy_read_request(ngx_stream_session_t *s)
{
    off_t                                   *received;
    ngx_buf_t                               *b;
    size_t                                  size;
    ssize_t                                 n;
    ngx_int_t                               rc;
    ngx_connection_t                        *c, *src;
    ngx_stream_redis_proxy_ctx_t            *ctx;
    ngx_stream_redis_proxy_srv_conf_t       *pscf;

    c = s->connection;
    src = c;

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);
    if (pscf == NULL) {
        return NGX_ERROR;
    }

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    b = ctx->buffer_in;
    received = &s->received;

    for ( ;; ) {

        size = b->end - b->last;

        if (size <= 0) {
            ngx_log_error(NGX_LOG_ERR, src->log, 0,
                    "[stream_redis_proxy] read request_buffer_size is too small for"
                    "the request");

            return NGX_ERROR;
        }

        n = src->recv(src, b->last, size);

        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, src->log, 0,
                "[stream_redis_proxy] recv returns %z", n);

        if (n > 0) {

            ctx->client_read = 0;

            //增加接收的数据字节数
            *received += n;

            //缓冲区的末尾指针移动，表示收到了n字节新数据
            b->last += n;

            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, src->log, 0,
                    "[stream_redis_proxy] recv buffer=[%s]", b->pos);

            rc = redis_parse_req(s);
            if (rc == NGX_ERROR) {
                ngx_log_error(NGX_LOG_ERR, src->log, 0,
                        "[stream_redis_proxy] redis_parse_req data=[%s]", b->pos);
                return NGX_ERROR;
            }

            if (rc == NGX_AGAIN) {
                //NGX_AGAIN 协议不完整
                continue;
            }

            rc = ngx_stream_redis_process_request(s);
            if (rc == NGX_ERROR) {
                ngx_log_error(NGX_LOG_ERR, src->log, 0,
                        "[stream_redis_proxy] ngx_stream_redis_process_request data=[%s]", b->pos);
                return NGX_ERROR;
            }

            ctx->client_read = 1;
            break;
        }

        break;
    }

    if (n == NGX_AGAIN) {

        if (!src->read->timer_set) {
            ngx_add_timer(src->read, pscf->client_read_timeout);
        }

        if (ngx_handle_read_event(src->read, 0) != NGX_OK) {
             return NGX_ERROR;
        }

        return NGX_AGAIN;
    }

    if (n == NGX_ERROR) {
        src->read->eof = 1;
        return NGX_OK;
    }

    return NGX_OK;
}

static ngx_int_t
ngx_stream_redis_proxy_read_upstream(ngx_stream_session_t *s)
{
    ngx_int_t                               rc;
    off_t                                   *received;
    ssize_t                                 size, n;
    ngx_buf_t                               *b;
    ngx_connection_t                        *c, *dst, *pc, *src;
    ngx_stream_upstream_t                   *u;
    ngx_stream_redis_proxy_ctx_t            *ctx;
    ngx_stream_redis_proxy_srv_conf_t       *pscf;

    u = s->upstream;
    c = s->connection;
    pc = u->connected ? u->peer.connection : NULL;
    src = pc;
    dst = c;

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);
    if (pscf == NULL) {
        return NGX_ERROR;
    }

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    b = &u->upstream_buf;
    received = &u->received;

    for ( ;; ) {

        size = b->end - b->last;

        if (size <= 0) {
            ngx_log_error(NGX_LOG_ERR, src->log, 0,
                    "[stream_redis_proxy] read upstream_buffer_size is too small for"
                    "the request");

            return NGX_ERROR;
        }

        n = src->recv(src, b->last, size);

        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, src->log, 0,
                "[stream_redis_proxy] recv returns %z", n);

        if (n > 0) {

            ctx->upstream_read = 0;

            //增加接收的数据字节数
             *received += n;

            // 缓冲区的末尾指针移动，表示收到了n字节新数据
            b->last += n;

            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, src->log, 0,
                    "[stream_redis_proxy] recv buffer=[%s]", b->pos);

            rc = redis_parse_rsp(s);
            if (rc == NGX_ERROR) {
                ngx_log_error(NGX_LOG_ERR, src->log, 0,
                        "[stream_redis_proxy] redis_parse_rsp data=[%s]", b->pos);
                return NGX_ERROR;
            }

            if (rc == NGX_AGAIN) {
                //NGX_AGAIN 协议不完整
                continue;
            }

            rc = ngx_stream_redis_process_response(s, b);
            if (rc == NGX_ERROR) {
                ngx_log_error(NGX_LOG_ERR, src->log, 0,
                        "[stream_redis_proxy] ngx_stream_redis_process_response data=[%s]", b->pos);
                return NGX_ERROR;
            }

            ctx->upstream_read = 1;
            break;
        }

        break;
    }

    if (n == NGX_ERROR) {
        src->read->eof = 1;
        return NGX_OK;
    }

    if (n == NGX_AGAIN) {

        if (!src->read->timer_set) {
            ngx_add_timer(src->read, pscf->timeout);
        }

        if ( !src->shared &&  ngx_handle_read_event(src->read, 0) != NGX_OK) {
            return NGX_ERROR;
        }

        return NGX_AGAIN;
    }

    //重试
    if ( ctx->type == MSG_RSP_REDIS_ERROR_ASK  || ctx->type == MSG_RSP_REDIS_ERROR_MOVED  ||
            ctx->type == MSG_RSP_REDIS_ERROR_TRYAGAIN ) {
        ngx_stream_redis_proxy_next_upstream(s);
        return NGX_OK;
    }

    return NGX_OK;
}


static void
ngx_stream_redis_proxy_handler(ngx_stream_session_t *s)
{
    ngx_connection_t                        *c;
    ngx_stream_redis_proxy_ctx_t            *ctx;
    ngx_stream_redis_proxy_srv_conf_t       *pscf;

    ctx = NULL;
    c = s->connection;

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);
    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0,
                   "proxy connection handler");

    ctx = ngx_pcalloc(c->pool, sizeof(ngx_stream_redis_proxy_ctx_t));
    if (ctx == NULL) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }
    ngx_stream_set_ctx(s, ctx, ngx_stream_redis_proxy_module);

    ctx->upstream_connect = 1;
    ctx->buffer_in = ngx_create_temp_buf(c->pool, pscf->buffer_size);
    if (ctx->buffer_in == NULL) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }

    c->write->handler = ngx_stream_redis_write_request_handler;
    c->read->handler = ngx_stream_redis_read_request_handler;

    if (c->read->ready) {
        ngx_post_event(c->read, &ngx_posted_events);
    }

    ngx_add_timer(s->connection->write, 60000);

    if (!c->shared && ngx_handle_read_event(c->read, 0) != NGX_OK) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }

}

static ngx_int_t
ngx_stream_redis_read_request_check(ngx_stream_session_t *s, ngx_connection_t *src, ngx_connection_t *dst, ngx_stream_upstream_t *u)
{
    // 这时应该是src已经读完，数据也发送完
    if (src && src->read->eof && u &&  (u->upstream_buf.pos == u->upstream_buf.last || (dst && dst->read->eof))) {

        // 这时应该是src已经读完，数据也发送完
        ngx_log_error(NGX_LOG_INFO, src->log, 0,
                            "%s%s disconnected"
                            ", bytes from/to client:%O/%O"
                            ", bytes from/to upstream:%O/%O",
                            "",
                            "client",
                            s->received, src->sent, u->received, dst->sent);

        // 在这里记录访问日志
        return NGX_OK;
    }

    return NGX_AGAIN;
}




static void
ngx_stream_redis_read_request_handler(ngx_event_t *rev)
{
    ngx_int_t                           rc;
    ngx_connection_t                    *c, *pc = NULL, *src = NULL, *dst = NULL;
    ngx_stream_session_t                *s;
    ngx_stream_upstream_t                   *u;
    ngx_stream_redis_proxy_ctx_t        *ctx;
    ngx_stream_redis_proxy_srv_conf_t       *pscf;

    c = rev->data;
    s = c->data;
    u = s->upstream;
    src = c;

    if ( u ) {
        pc = u->connected ? u->peer.connection : NULL;
        dst = pc;
    }

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0, "ngx_stream_redis_read_request_handler");

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);
    ctx = ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        return;
    }

    if (rev->timedout) {
        c->timedout = 1;

        ngx_connection_error(c, NGX_ETIMEDOUT, "connection timed out");
        ngx_stream_redis_proxy_finalize(s, NGX_DECLINED);
        return;
    }

    rc = ngx_stream_redis_proxy_read_request(s);
    if ( rc == NGX_ERROR ) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }

    if ( rc == NGX_AGAIN ) {
        return;
    }

    if ( ctx->slotids == NULL || ctx->slotids->nelts == 0 ) {
        rc = ngx_stream_redis_read_request_check(s, src, dst, u);
        if ( rc == NGX_OK ) {
            ngx_stream_redis_proxy_finalize(s, NGX_OK);
            return;
        }

        ngx_stream_redis_proxy_connect(s);
        return;
    }

    if ( ctx->slotids->nelts == 1 ) {
        rc = ngx_stream_redis_read_request_check(s, src, dst, u);
        if ( rc == NGX_OK ) {
            ngx_stream_redis_proxy_finalize(s, NGX_OK);
            return;
        }

        ctx->slotid = *(size_t *)ctx->slotids->elts;
        rc = ngx_stream_redis_process_request(s);
        if ( rc != NGX_OK ) {
            ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
            return;
        }
        ngx_stream_redis_proxy_connect(s);
        return;
    }

    ngx_stream_redis_proxy_finalize(s, NGX_OK);
    return;

/*
    size_t i = 0;
    for ( i = ctx->request_num; i < ctx->client_buffers->nelts; i++) {
        rc = ngx_stream_redis_read_request_check(s, src, dst, u);
        if ( rc != NGX_OK && ctx->request_num > 0) {
             return;
         }

        ctx->slotid = *(size_t *)ctx->slotids->elts + i;
        rc = ngx_stream_redis_process_request(s);
        if ( rc != NGX_OK ) {
            ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
            return;
        }
        ngx_str_t *buffers = (ngx_str_t *) ctx->client_buffers->elts + i;

        ngx_log_error(NGX_LOG_INFO, c->log, 0, "%V", buffers);

        ctx->buffer_in->pos = buffers->data;
        ctx->buffer_in->last = buffers->data + buffers->len;
        ctx->buffer_in->memory = 1;
        ctx->buffer_in->last_buf = 1;

        ngx_stream_redis_proxy_connect(s);
        ctx->request_num++;
    }
*/
}


static void
ngx_stream_redis_write_request_handler(ngx_event_t *wev)
{
    ngx_connection_t                    *c;
    ngx_stream_session_t                *s;
    ngx_stream_redis_proxy_srv_conf_t       *pscf;

    c = wev->data;
    s = c->data;


    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0, "ngx_stream_redis_write_request_handler");

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);

    if (wev->timedout) {
        c->timedout = 1;

        ngx_connection_error(c, NGX_ETIMEDOUT, "connection timed out");
        ngx_stream_redis_proxy_finalize(s, NGX_DECLINED);
        return;
    }

    ngx_stream_redis_proxy_process_connection(wev, 1);
}



static void
ngx_stream_redis_proxy_connect(ngx_stream_session_t *s)
{
    ngx_int_t                     rc;
    ngx_connection_t             *c, *pc;
    ngx_stream_upstream_t        *u;
    ngx_stream_redis_proxy_ctx_t            *ctx;
    ngx_stream_redis_proxy_srv_conf_t       *pscf;
    ngx_stream_upstream_srv_conf_t          *uscf;

    ctx = NULL;
    c = s->connection;
    u = s->upstream;

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);
    if (pscf == NULL) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }

    // 创建连接上游的结构体
    // 里面有如何获取负载均衡server、上下游buf等
    u = ngx_pcalloc(c->pool, sizeof(ngx_stream_upstream_t));
    if (u == NULL) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }
    s->upstream = u;
    u->downstream_buf = *ctx->buffer_in;

    s->log_handler = ngx_stream_redis_proxy_log_error;
    u->peer.log = c->log;
    u->peer.log_error = NGX_ERROR_ERR;

    u->peer.local = pscf->local;
    u->peer.type = c->type;

    // 获取上游的配置结构体
    // 在ngx_stream_proxy_pass里设置的
    uscf = pscf->upstream;

    // 负载均衡算法初始化
    if (uscf->peer.init(s, uscf) != NGX_OK) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }

    // 准备开始连接，设置开始时间，毫秒
    u->peer.start_time = ngx_current_msec;

    // 设置负载均衡的重试次数
    if (pscf->next_upstream_tries
        && u->peer.tries > pscf->next_upstream_tries)
    {
        u->peer.tries = pscf->next_upstream_tries;
    }

    u->proxy_protocol = pscf->proxy_protocol;

    // 准备开始连接，设置开始时间，秒数，没有毫秒
    u->start_sec = ngx_time();

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);
    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0,
                   "proxy connection handler");

    c->log->action = "connecting to upstream";

    u = s->upstream;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0, "ngx_sream_redis_proxy_connect ");

    rc = ngx_event_connect_peer(&u->peer);

    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, c->log, 0, "proxy connect: %i", rc);

    if (rc == NGX_ERROR) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }

    if (rc == NGX_BUSY) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0, "no live upstreams");
        ngx_stream_redis_proxy_finalize(s, NGX_DECLINED);
        return;
    }

    if (rc == NGX_DECLINED) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0, "rc  NGX_DECLINED");
        ngx_stream_redis_proxy_next_upstream(s);
        return;
    }

    /* rc == NGX_OK || rc == NGX_AGAIN || rc == NGX_DONE */
    pc = u->peer.connection;

    pc->data = s;
    pc->log = c->log;
    pc->pool = c->pool;
    pc->read->log = c->log;
    pc->write->log = c->log;

    if (rc != NGX_AGAIN) {
        ngx_stream_redis_proxy_init_upstream(s);
        return;
    }

    pc->read->handler = ngx_stream_redis_proxy_connect_handler;
    pc->write->handler = ngx_stream_redis_proxy_connect_handler;

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);

    ngx_add_timer(pc->write, pscf->connect_timeout);
}

static void
ngx_stream_redis_proxy_connect_handler(ngx_event_t *ev)
{
    ngx_connection_t      *c;
    ngx_stream_session_t  *s;
    ngx_stream_redis_proxy_ctx_t *ctx;

    c = ev->data;
    s = c->data;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0, "ngx_stream_redis_proxy_connect_handler");

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }

    if (ev->timedout) {
        ngx_log_error(NGX_LOG_ERR, c->log, NGX_ETIMEDOUT, "upstream timed out");
        ngx_stream_redis_proxy_next_upstream(s);
        return;
    }

    ngx_del_timer(c->write);

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0,
                   "stream proxy connect upstream");

    if (ngx_stream_redis_proxy_test_connect(c) != NGX_OK) {
        ctx->upstream_connect = 0;
        ngx_stream_redis_proxy_next_upstream(s);
        return;
    }

    ctx->upstream_connect = 1;
    ngx_stream_redis_proxy_init_upstream(s);
}


static void
ngx_stream_redis_proxy_init_upstream(ngx_stream_session_t *s)
{
    ngx_int_t                    rc;
    int                           tcp_nodelay;
    u_char                       *p;
    ngx_connection_t             *c, *pc;
    ngx_log_handler_pt            handler;
    ngx_stream_upstream_t        *u;
    ngx_stream_core_srv_conf_t   *cscf;
    ngx_stream_redis_proxy_srv_conf_t  *pscf;

    u = s->upstream;
    pc = u->peer.connection;

    cscf = ngx_stream_get_module_srv_conf(s, ngx_stream_core_module);

    if (pc->type == SOCK_STREAM
        && cscf->tcp_nodelay
        && pc->tcp_nodelay == NGX_TCP_NODELAY_UNSET)
    {
        ngx_log_debug0(NGX_LOG_DEBUG_STREAM, pc->log, 0, "tcp_nodelay");

        tcp_nodelay = 1;

        if (setsockopt(pc->fd, IPPROTO_TCP, TCP_NODELAY,
                       (const void *) &tcp_nodelay, sizeof(int)) == -1)
        {
            ngx_connection_error(pc, ngx_socket_errno,
                                 "setsockopt(TCP_NODELAY) failed");
            ngx_stream_redis_proxy_next_upstream(s);
            return;
        }

        pc->tcp_nodelay = NGX_TCP_NODELAY_SET;
    }

    if (u->proxy_protocol) {

        u->proxy_protocol = 0;
    }

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);

    c = s->connection;

    if (c->log->log_level >= NGX_LOG_INFO) {
        ngx_str_t  str;
        u_char     addr[NGX_SOCKADDR_STRLEN];

        str.len = NGX_SOCKADDR_STRLEN;
        str.data = addr;

        if (ngx_connection_local_sockaddr(pc, &str, 1) == NGX_OK) {
            handler = c->log->handler;
            c->log->handler = NULL;
/*
            ngx_log_error(NGX_LOG_INFO, c->log, 0,
                          "%sproxy %V connected to %V",
                          pc->type == SOCK_DGRAM ? "udp " : "",
                          &str, u->peer.name);
*/
            c->log->handler = handler;
        }
    }

    c->log->action = "proxying connection";

    if (u->upstream_buf.start == NULL) {
        p = ngx_pnalloc(c->pool, pscf->buffer_size);
        if (p == NULL) {
            ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
            return;
        }

        u->upstream_buf.start = p;
        u->upstream_buf.end = p + pscf->buffer_size;
        u->upstream_buf.pos = p;
        u->upstream_buf.last = p;
    }

    u->connected = 1;

    pc->read->handler = ngx_stream_redis_proxy_upstream_read_handler;
    pc->write->handler = ngx_stream_redis_proxy_upstream_write_handler;

    if (pc->read->ready || pc->read->eof) {
        ngx_post_event(pc->read, &ngx_posted_events);
    }

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0, "ngx_stream_redis_proxy_init_upstream");
    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, c->log, 0, "[redis_proxy] send buffer downstream_buf=[%s]", u->downstream_buf.pos);

    rc = ngx_stream_redis_proxy_send_buffer(s, pc, &u->downstream_buf, pscf->timeout);
    if (rc == NGX_ERROR ) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }
}

static void
ngx_stream_redis_proxy_upstream_write_handler(ngx_event_t *wev)
{
    ngx_stream_redis_proxy_process_connection(wev, 0);
}

static void
ngx_stream_redis_proxy_upstream_read_handler(ngx_event_t *rev)
{
    ngx_stream_redis_proxy_process_connection(rev, 1);
}


static void
ngx_stream_redis_proxy_process_connection(ngx_event_t *ev, ngx_uint_t from_upstream)
{
    ngx_int_t                    rc;
    ngx_connection_t             *c, *pc, *src, *dst;
    ngx_stream_session_t         *s;
    ngx_stream_upstream_t        *u;
    ngx_stream_redis_proxy_ctx_t *ctx;
    ngx_stream_redis_proxy_srv_conf_t  *pscf;

    c = ev->data;
    s = c->data;
    u = s->upstream;
    c = s->connection;
    pc = u->peer.connection;
    src = pc;
    dst = c;

    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, c->log, 0, "ngx_stream_redis_upstream ev %d", ev->write);

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }

    if (ev->timedout) {
        ev->timedout = 0;
        if (ev->delayed) {
            ev->delayed = 0;
            if (!ev->ready) {
                if (ngx_handle_read_event(ev, 0) != NGX_OK) {
                    ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
                    return;
                }
                if (u->connected && !c->read->delayed && !pc->read->delayed) {
                    ngx_add_timer(c->write, pscf->timeout);
                }
                return;
            }
        } else {
            ngx_connection_error(c, NGX_ETIMEDOUT, "connection timed out");
            ngx_stream_redis_proxy_finalize(s, NGX_DECLINED);
            return;
        }
    } else if (ev->delayed) {
        ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0,
                       "stream connection delayed");
        if (ngx_handle_read_event(ev, 0) != NGX_OK) {
            ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        }
        return;
    }
    if (from_upstream && !u->connected) {
        return;
    }

    if (from_upstream && !ev->write) {

        rc = ngx_stream_redis_proxy_read_upstream(s);
        if ( rc == NGX_ERROR ) {
             ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
            return;
        }
        if ( rc == NGX_AGAIN ) {
            return;
        }
        return;
    }

    if (ctx->upstream_read) {
        rc = ngx_stream_redis_proxy_send_buffer(s, c, &u->upstream_buf, pscf->timeout);
        if (rc == NGX_ERROR ) {
            ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
            return;
        }
        if ( rc != NGX_OK ) {
            return;
        }
        ctx->buffer_in->pos = ctx->buffer_in->start;
        ctx->buffer_in->last = ctx->buffer_in->start;
    }

    // 这时应该是src已经读完，数据也发送完
    if (src->read->eof && (u->upstream_buf.pos == u->upstream_buf.last || (dst && dst->read->eof))) {

        // 这时应该是src已经读完，数据也发送完
        ngx_log_error(NGX_LOG_INFO, c->log, 0,
                            "%s%s disconnected"
                            ", bytes from/to client:%O/%O"
                            ", bytes from/to upstream:%O/%O",
                            c->type == SOCK_DGRAM ? "udp " : "",
                            from_upstream ? "upstream" : "client",
                            s->received, c->sent, u->received, pc ? pc->sent : 0);

        // 在这里记录访问日志
        ngx_stream_redis_proxy_finalize(s, NGX_OK);
    }
}

static ngx_int_t
ngx_stream_redis_proxy_test_connect(ngx_connection_t *c)
{
    int        err;
    socklen_t  len;

#if (NGX_HAVE_KQUEUE)

    if (ngx_event_flags & NGX_USE_KQUEUE_EVENT)  {
        err = c->write->kq_errno ? c->write->kq_errno : c->read->kq_errno;

        if (err) {
            (void) ngx_connection_error(c, err,
                                    "kevent() reported that connect() failed");
            return NGX_ERROR;
        }

    } else
#endif
    {
        err = 0;
        len = sizeof(int);

        /*
         * BSDs and Linux return 0 and set a pending error in err
         * Solaris returns -1 and sets errno
         */

        if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, (void *) &err, &len)
            == -1)
        {
            err = ngx_socket_errno;
        }

        if (err) {
            (void) ngx_connection_error(c, err, "connect() failed");
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

static void
ngx_stream_redis_proxy_next_upstream(ngx_stream_session_t *s)
{
    ngx_msec_t                    timeout;
    ngx_connection_t             *pc;
    ngx_stream_upstream_t        *u;
    ngx_stream_redis_proxy_srv_conf_t  *pscf;
    ngx_stream_redis_proxy_ctx_t        *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, s->connection->log, 0,
                   "stream proxy next upstream");

    u = s->upstream;

    pscf = ngx_stream_get_module_srv_conf(s, ngx_stream_redis_proxy_module);

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_redis_proxy_module);
    if (ctx == NULL) {
        ngx_stream_redis_proxy_finalize(s, NGX_ERROR);
        return;
    }

    if (u->peer.sockaddr) {
        u->peer.free(&u->peer, u->peer.data, NGX_PEER_FAILED);
        u->peer.sockaddr = NULL;
    }

    timeout = pscf->next_upstream_timeout;
    pc = u->peer.connection;

    if (pc) {
        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, s->connection->log, 0,
                       "close proxy upstream connection: %d", pc->fd);

        ngx_close_connection(pc);
        u->peer.connection = NULL;
    }

    ngx_stream_redis_proxy_connect(s);
}


static void
ngx_stream_redis_proxy_finalize(ngx_stream_session_t *s, ngx_int_t rc)
{
    ngx_connection_t       *pc;
    ngx_stream_upstream_t  *u;

    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, s->connection->log, 0,
                   "finalize stream proxy: %i", rc);

    u = s->upstream;

    if (u == NULL) {
        goto noupstream;
    }

    if (u->peer.free && u->peer.sockaddr) {
        u->peer.free(&u->peer, u->peer.data, 0);
        u->peer.sockaddr = NULL;
    }

    pc = u->peer.connection;

    if (pc) {
        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, s->connection->log, 0,
                       "close stream proxy upstream connection: %d", pc->fd);

        ngx_close_connection(pc);
        u->peer.connection = NULL;
    }

noupstream:

    ngx_stream_close_connection(s->connection);
}


static u_char *
ngx_stream_redis_proxy_log_error(ngx_log_t *log, u_char *buf, size_t len)
{
    u_char                 *p;
    ngx_connection_t       *pc;
    ngx_stream_session_t   *s;
    ngx_stream_upstream_t  *u;

    s = log->data;

    u = s->upstream;

    p = buf;

    if (u->peer.name) {
        p = ngx_snprintf(p, len, ", upstream: \"%V\"", u->peer.name);
        len -= p - buf;
    }

    pc = u->peer.connection;

    p = ngx_snprintf(p, len,
                     ", bytes from/to client:%O/%O"
                     ", bytes from/to upstream:%O/%O",
                     s->received, s->connection->sent,
                     u->received, pc ? pc->sent : 0);

    return p;
}


static void *
ngx_stream_redis_proxy_create_srv_conf(ngx_conf_t *cf)
{
    ngx_stream_redis_proxy_srv_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_stream_redis_proxy_srv_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    /*
     * set by ngx_pcalloc():
     *
     *     conf->ssl_protocols = 0;
     *     conf->ssl_ciphers = { 0, NULL };
     *     conf->ssl_name = { 0, NULL };
     *     conf->ssl_trusted_certificate = { 0, NULL };
     *     conf->ssl_crl = { 0, NULL };
     *     conf->ssl_certificate = { 0, NULL };
     *     conf->ssl_certificate_key = { 0, NULL };
     *
     *     conf->ssl = NULL;
     *     conf->upstream = NULL;
     */

    conf->client_read_timeout = NGX_CONF_UNSET_MSEC;

    conf->connect_timeout = NGX_CONF_UNSET_MSEC;
    conf->timeout = NGX_CONF_UNSET_MSEC;
    conf->next_upstream_timeout = NGX_CONF_UNSET_MSEC;
    conf->buffer_size = NGX_CONF_UNSET_SIZE;
    conf->upload_rate = NGX_CONF_UNSET_SIZE;
    conf->download_rate = NGX_CONF_UNSET_SIZE;
    conf->responses = NGX_CONF_UNSET_UINT;
    conf->next_upstream_tries = NGX_CONF_UNSET_UINT;
    conf->next_upstream = NGX_CONF_UNSET;
    conf->proxy_protocol = NGX_CONF_UNSET;
    conf->local = NGX_CONF_UNSET_PTR;

    return conf;
}


static char *
ngx_stream_redis_proxy_merge_srv_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_stream_redis_proxy_srv_conf_t *prev = parent;
    ngx_stream_redis_proxy_srv_conf_t *conf = child;

    ngx_conf_merge_msec_value(conf->client_read_timeout,
                              prev->client_read_timeout, 60000);

    ngx_conf_merge_msec_value(conf->connect_timeout,
                              prev->connect_timeout, 60000);

    ngx_conf_merge_msec_value(conf->timeout,
                              prev->timeout, 10 * 60000);

    ngx_conf_merge_msec_value(conf->next_upstream_timeout,
                              prev->next_upstream_timeout, 0);

    ngx_conf_merge_size_value(conf->buffer_size,
                              prev->buffer_size, 1638400);

    ngx_conf_merge_size_value(conf->upload_rate,
                              prev->upload_rate, 0);

    ngx_conf_merge_size_value(conf->download_rate,
                              prev->download_rate, 0);

    ngx_conf_merge_uint_value(conf->responses,
                              prev->responses, NGX_MAX_INT32_VALUE);

    ngx_conf_merge_uint_value(conf->next_upstream_tries,
                              prev->next_upstream_tries, 0);

    ngx_conf_merge_value(conf->next_upstream, prev->next_upstream, 1);

    ngx_conf_merge_value(conf->proxy_protocol, prev->proxy_protocol, 0);

    ngx_conf_merge_ptr_value(conf->local, prev->local, NULL);


    return NGX_CONF_OK;
}


static char *
ngx_stream_redis_proxy_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_stream_redis_proxy_srv_conf_t *pscf = conf;

    ngx_url_t                    u;
    ngx_str_t                   *value, *url;
    ngx_stream_core_srv_conf_t  *cscf;

    if (pscf->upstream) {
        return "is duplicate";
    }

    cscf = ngx_stream_conf_get_module_srv_conf(cf, ngx_stream_core_module);

    cscf->handler = ngx_stream_redis_proxy_handler;

    value = cf->args->elts;

    url = &value[1];

    ngx_memzero(&u, sizeof(ngx_url_t));

    u.url = *url;
    u.no_resolve = 1;

    pscf->upstream = ngx_stream_upstream_add(cf, &u, 0);
    if (pscf->upstream == NULL) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}

static ngx_int_t
ngx_stream_redis_proxy_init_process(ngx_cycle_t *cycle)
{
    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, cycle->log, 0, "[redis_proxy] init process");

    ngx_stream_redis_init();

    return NGX_OK;
}


static void
ngx_stream_redis_proxy_exit_process(ngx_cycle_t *cycle)
{
    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, cycle->log, 0, "[redis_proxy] exit process");

    ngx_stream_redis_destroy();
}



