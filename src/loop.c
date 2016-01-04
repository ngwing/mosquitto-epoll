/*
Copyright (c) 2009-2012 Roger Light <roger@atchoo.org>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
3. Neither the name of mosquitto nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/

#define _GNU_SOURCE

#include <config.h>
#include <assert.h>
#include <epoll.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>

#include <mosquitto_broker.h>
#include <memory_mosq.h>
#include <util_mosq.h>

#ifndef POLLRDHUP
/* Ignore POLLRDHUP flag on systems where it doesn't exist. */
#define POLLRDHUP 0
#endif

extern bool flag_reload;
#ifdef WITH_PERSISTENCE
extern bool flag_db_backup;
#endif
extern bool flag_tree_print;
extern int run;
extern int g_clients_expired;

static void loop_handle_errors(struct mosquitto_db* db, struct epoll_event* events, int efd);
static void loop_handle_reads_writes(struct mosquitto_db* db, struct epoll_event* events, int efd);


static int nonblocking (int fd)
{
    int f, s;

    f = fcntl (fd, F_GETFL, 0);
    if (f == -1) {
        return -1;
    }

    f |= O_NONBLOCK;

    s = fcntl (fd, F_SETFL, f);
    if (s == -1) {
        return -1;
    }

    return 0;
}


struct epoll_event* init_events(
    struct epoll_event* events,
    int                 listensock_count,
    int                 db_context_count,
    int*                epollfd_count)
{
    if (listensock_count + db_context_count > *epollfd_count){
        *epollfd_count = listensock_count + db_context_count;
        return _mosquitto_realloc (
            events,
            sizeof (struct epoll_event) * *epollfd_count);
    } else {
        return events;
    }

}

int create_event(
    struct epoll_event* events,
    int*                listensock,
    int                 listensock_count,
    int*                epollfd_index
    )
{
    int i;
    int efd;

    efd = epoll_create1 (0);

    for(i = 0; i < listensock_count; i++){
        nonblocking (listensock[i]);
        events[*epollfd_index].data.fd = listensock[i];
        events[*epollfd_index].events = EPOLLIN | EPOLLET;
        epoll_ctl (efd, EPOLL_CTL_ADD, listensock[i], &events[*epollfd_index]);
        *epollfd_index = *epollfd_index + 1;
    }

    return efd;
}

static void create_context_events(
    struct mosquitto_db* db,
    struct epoll_event*  events,
    int*                 epollfd_index,
    int                  efd,
    time_t               now,
    int                  i)
{

    /* Local bridges never time out in this fashion. */
    if (!(db->contexts[i]->keepalive) ||
        db->contexts[i]->bridge ||
        now - db->contexts[i]->last_msg_in < (time_t)(db->contexts[i]->keepalive) * 3 / 2) {
        if (mqtt3_db_message_write(db->contexts[i]) == MOSQ_ERR_SUCCESS) {
            nonblocking (db->contexts[i]->sock);
            events[*epollfd_index].data.fd = db->contexts[i]->sock;
            events[*epollfd_index].events = EPOLLIN | EPOLLRDHUP;
            if(db->contexts[i]->out_packet) {
                events[*epollfd_index].events |= EPOLLOUT;
            }
            epoll_ctl (efd, EPOLL_CTL_ADD, db->contexts[i]->sock, &events[*epollfd_index]);
            db->contexts[i]->pollfd_index = *epollfd_index;
            *epollfd_index = *epollfd_index + 1;

        } else {
            mqtt3_context_disconnect(db, db->contexts[i]);
        }
    } else{
        if (db->config->connection_messages == true) {
            _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s has exceeded timeout, disconnecting.", db->contexts[i]->id);
        }
        /* Client has exceeded keepalive*1.5 */
        mqtt3_context_disconnect(db, db->contexts[i]);
    }
}

static void create_bridge_events(
    struct mosquitto_db* db,
    struct epoll_event*  events,
    int*                 epollfd_index,
    int                  efd,
    int                  i)
{

    /* Want to try to restart the bridge connection */
    if(!db->contexts[i]->bridge->restart_t){
        db->contexts[i]->bridge->restart_t = time(NULL)+db->contexts[i]->bridge->restart_timeout;
    } else {
        if(db->contexts[i]->bridge->start_type == bst_automatic && time(NULL) > db->contexts[i]->bridge->restart_t){
            db->contexts[i]->bridge->restart_t = 0;
            if(mqtt3_bridge_connect(db, db->contexts[i]) == MOSQ_ERR_SUCCESS){
                nonblocking (db->contexts[i]->sock);
                events[*epollfd_index].data.fd = db->contexts[i]->sock;
                events[*epollfd_index].events = EPOLLIN | POLLRDHUP;
                if(db->contexts[i]->out_packet){
                    events[*epollfd_index].events |= EPOLLOUT;
                }
                epoll_ctl (efd, EPOLL_CTL_ADD, db->contexts[i]->sock, &events[*epollfd_index]);
                db->contexts[i]->pollfd_index = *epollfd_index;
                *epollfd_index = *epollfd_index + 1;
            } else {
                /* Retry later. */
                db->contexts[i]->bridge->restart_t = time(NULL)+db->contexts[i]->bridge->restart_timeout;
            }
        }
    }
}

static void reg_context_events(
    struct mosquitto_db* db,
    struct epoll_event*  events,
    int*                 epollfd_index,
    int                  efd,
    time_t               now)
{
    int i, s;

    for(i = 0; i < db->context_count; i++) {
        if (! db->contexts[i]) continue;

        db->contexts[i]->pollfd_index = -1;

        if (db->contexts[i]->sock != INVALID_SOCKET) {
#ifdef WITH_BRIDGE
            if (db->contexts[i]->bridge){
                _mosquitto_check_keepalive (db->contexts[i]);
            }
#endif
            create_context_events(db, events, epollfd_index, efd, now, i);
        } else {
#ifdef WITH_BRIDGE
            if(db->contexts[i]->bridge){
                create_bridge_events(db, events, epollfd_index, efd, i);
            } else {
#endif
                if(db->contexts[i]->clean_session == true) {
                    mqtt3_context_cleanup(db, db->contexts[i], true);
                    db->contexts[i] = NULL;
                } else if (db->config->persistent_client_expiration > 0) {
                    /* This is a persistent client, check to see if the
                     * last time it connected was longer than
                     * persistent_client_expiration seconds ago. If so,
                     * expire it and clean up.
                     */
                    if (time(NULL) > db->contexts[i]->disconnect_t+db->config->persistent_client_expiration){
                        _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Expiring persistent client %s due to timeout.", db->contexts[i]->id);
                        g_clients_expired++;
                        db->contexts[i]->clean_session = true;
                        mqtt3_context_cleanup(db, db->contexts[i], true);
                        db->contexts[i] = NULL;
                    }
                }
#ifdef WITH_BRIDGE
            }
#endif
        }
    }
}


int mosquitto_main_loop(struct mosquitto_db *db, int *listensock, int listensock_count, int listener_max)
{
    time_t start_time = time(NULL);
    time_t last_backup = time(NULL);
    time_t last_store_clean = time(NULL);
    time_t now;

    sigset_t sigblock, origsig;

    int i, s;  /* temp variables */
    struct epoll_event* events = NULL;
    int epollfd_count = 0;
    int epollfd_index;
    int efd;  /* epoll fd */



    while (run) {

    mqtt3_db_sys_update(db, db->config->sys_interval, start_time);
    events = init_events(events, listensock_count, db->context_count, &epollfd_count);

    if (!events) {
        _mosquitto_log_printf (NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
        return MOSQ_ERR_NOMEM;
    }

    memset(events, -1, sizeof(struct epoll_event) * epollfd_count);

    epollfd_index = 0;
    efd = create_event(events, listensock, listensock_count, &epollfd_index);

    now = time (NULL);
    reg_context_events(db, events, &epollfd_index, efd, now);


        mqtt3_db_message_timeout_check(db, db->config->retry_interval);

        sigprocmask(SIG_SETMASK, &sigblock, &origsig);

        s = epoll_wait(efd, events, 64, -1);

        if (s == -1) {
            printf("errno value: %d, it means: %s", errno, strerror(errno));
            loop_handle_errors(db, events, efd);
        }

        if (s == -1) {
            printf("errno value: %d, it means: %s", errno, strerror(errno));
            loop_handle_errors(db, events, efd);
        } else{
            loop_handle_reads_writes(db, events, efd);

            for(i=0; i<listensock_count; i++){
                if(events[i].events & (EPOLLIN | EPOLLPRI)){
                    while((s = mqtt3_socket_accept(db, listensock[i], &events[i], efd)) != -1){

                    }
                }
            }
        }

        sigprocmask(SIG_SETMASK, &origsig, NULL);

#ifdef WITH_PERSISTENCE
        if(db->config->persistence && db->config->autosave_interval){
            if(db->config->autosave_on_changes){
                if(db->persistence_changes > db->config->autosave_interval){
                    mqtt3_db_backup(db, false, false);
                    db->persistence_changes = 0;
                }
            }else{
                if(last_backup + db->config->autosave_interval < now){
                    mqtt3_db_backup(db, false, false);
                    last_backup = time(NULL);
                }
            }
        }
#endif
        if(!db->config->store_clean_interval || last_store_clean + db->config->store_clean_interval < now){
            mqtt3_db_store_clean(db);
            last_store_clean = time(NULL);
        }
#ifdef WITH_PERSISTENCE
        if(flag_db_backup){
            mqtt3_db_backup(db, false, false);
            flag_db_backup = false;
        }
#endif
        if(flag_reload){
            _mosquitto_log_printf(NULL, MOSQ_LOG_INFO, "Reloading config.");
            mqtt3_config_read(db->config, true);
            mosquitto_security_cleanup(db, true);
            mosquitto_security_init(db, true);
            mosquitto_security_apply(db);
            mqtt3_log_init(db->config->log_type, db->config->log_dest);
            flag_reload = false;
        }
        if(flag_tree_print){
            mqtt3_sub_tree_print(&db->subs, 0);
            flag_tree_print = false;
        }
    }

    if(events) _mosquitto_free(events);
    return MOSQ_ERR_SUCCESS;
}


static void do_disconnect(struct mosquitto_db *db, int context_index)
{
    if (db->config->connection_messages == true) {
        if(db->contexts[context_index]->state != mosq_cs_disconnecting){
            _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Socket error on client %s, disconnecting.", db->contexts[context_index]->id);
        }else{
            _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s disconnected.", db->contexts[context_index]->id);
        }
    }
    mqtt3_context_disconnect(db, db->contexts[context_index]);
}

/* Error ocurred, probably an fd has been closed.
 * Loop through and check them all.
 */
static void loop_handle_errors(struct mosquitto_db *db, struct epoll_event *events, int efd)
{
    int i;
    int s;

    for(i=0; i<db->context_count; i++){
        if(db->contexts[i] && db->contexts[i]->sock != INVALID_SOCKET){
            if(events[db->contexts[i]->pollfd_index].events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)){
                s = epoll_ctl (efd, EPOLL_CTL_DEL, events[db->contexts[i]->pollfd_index].data.fd, &events[db->contexts[i]->pollfd_index]);
                do_disconnect(db, i);
            }
        }
    }
}


static void loop_handle_reads_writes(struct mosquitto_db *db, struct epoll_event *events, int efd)
{
    int i, s;

    for (i = 0; i < db->context_count; i++) {
        if (db->contexts[i] && db->contexts[i]->sock != INVALID_SOCKET){
            /* if (events[db->contexts[i]->pollfd_index].data.fd != db->contexts[i]->sock) { */
            /*     printf ("%d---%d\n", events[db->contexts[i]->pollfd_index].data.fd, db->contexts[i]->sock); */
            /*     printf ("%d\n", i); */
            /*     int n; */
            /*     for (n = 0; n < sizeof (db->contexts); ++n) */
            /*     { */
            /*         printf ("%d----%d\n", db->contexts[n]->pollfd_index, events[db->contexts[i]->pollfd_index].data.fd); */
            /*     } */
            /* } */
            /* assert(events[db->contexts[i]->pollfd_index].data.fd == db->contexts[i]->sock); */
#ifdef WITH_TLS
            if (events[db->contexts[i]->pollfd_index].events & EPOLLOUT ||
               db->contexts[i]->want_write ||
               (db->contexts[i]->ssl && db->contexts[i]->state == mosq_cs_new)) {
#else
                if(events[db->contexts[i]->epollfd_index].events & EPOLLOUT){
#endif
                    if(_mosquitto_packet_write(db->contexts[i])){
                        if(db->config->connection_messages == true){
                            if(db->contexts[i]->state != mosq_cs_disconnecting){
                                _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Socket write error on client %s, disconnecting.", db->contexts[i]->id);
                            }else{
                                _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s disconnected.", db->contexts[i]->id);
                            }
                        }
                        /* Write error or other that means we should disconnect */
                        mqtt3_context_disconnect(db, db->contexts[i]);
                    }
                }
            }
            if(db->contexts[i] && db->contexts[i]->sock != INVALID_SOCKET){
                /* assert(events[db->contexts[i]->pollfd_index].data.fd == db->contexts[i]->sock); */
#ifdef WITH_TLS
                if(events[db->contexts[i]->pollfd_index].events & EPOLLIN ||
                   db->contexts[i]->want_read ||
                   (db->contexts[i]->ssl && db->contexts[i]->state == mosq_cs_new)){
#else
                    if(events[db->contexts[i]->pollfd_index].events & EPOLLIN){
#endif
                        if(_mosquitto_packet_read(db, db->contexts[i])){
                            if(db->config->connection_messages == true){
                                if(db->contexts[i]->state != mosq_cs_disconnecting){
                                    _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Socket read error on client %s, disconnecting.", db->contexts[i]->id);
                                }else{
                                    _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s disconnected.", db->contexts[i]->id);
                                }
                            }
                            /* Read error or other that means we should disconnect */
                            mqtt3_context_disconnect(db, db->contexts[i]);
                        }
                    }
                }

                if(db->contexts[i] && db->contexts[i]->sock != INVALID_SOCKET){
                    if(events[db->contexts[i]->pollfd_index].events & (EPOLLHUP | POLLRDHUP | EPOLLERR)){
                        s = epoll_ctl (efd, EPOLL_CTL_DEL, events[db->contexts[i]->pollfd_index].data.fd, &events[db->contexts[i]->pollfd_index]);
                        do_disconnect(db, i);
                    }
                }
       }
}
