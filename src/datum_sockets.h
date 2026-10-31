/*
 *
 * DATUM Gateway
 * Decentralized Alternative Templates for Universal Mining
 *
 * This file is part of CONVOY's Bitcoin mining decentralization
 * project, DATUM.
 *
 * https://convoy.xyz
 *
 * ---
 *
 * Copyright (c) 2024-2025 Bitcoin Ocean, LLC, Jason Hughes, and individual contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#ifndef _DATUM_SOCKETS_H_
#define _DATUM_SOCKETS_H_

#ifndef T_DATUM_TEMPLATE_DATA
	#include "datum_blocktemplates.h"
#endif

#include <stdatomic.h>
#include <stdint.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/eventfd.h>

typedef struct T_DATUM_THREAD_DATA T_DATUM_THREAD_DATA;
typedef struct T_DATUM_CLIENT_DATA T_DATUM_CLIENT_DATA;

typedef void (*DATUM_ThreadPool_Init_Func)(T_DATUM_THREAD_DATA *);
typedef void (*DATUM_ThreadPool_Loop_Func)(T_DATUM_THREAD_DATA *);
typedef int (*DATUM_ThreadPool_ClientCmd_Func)(T_DATUM_CLIENT_DATA *, char *);

typedef void (*DATUM_ThreadPool_ClientClosed_Func)(T_DATUM_CLIENT_DATA *, const char *);
typedef void (*DATUM_ThreadPool_ClientNew_Func)(T_DATUM_CLIENT_DATA *);

#define DATUM_MAX_IP_LEN 64

// TODO: Make these dynamic
// These are hard coded buffer related values, not directly related to the config file values.
// We avoid dynamic memory allocation to prevent fragmentation and other hassles, currently.
#define MAX_CLIENTS_THREAD 4096
#define MAX_THREADS 64
#define MAX_EVENTS (MAX_CLIENTS_THREAD*2)

typedef struct T_DATUM_CLIENT_DATA {
	bool new_connection;
	int fd;
	int cid;
	
	char buffer[CLIENT_BUFFER];
	int in_buf;
	
	char w_buffer[CLIENT_BUFFER];
	int out_buf;
	
	char rem_host[DATUM_MAX_IP_LEN+1];
	
	bool kill_request;
	
	bool write_queued;
	
	void *app_client_data;
	
	int proxy_line_read;
	
	T_DATUM_THREAD_DATA *datum_thread;
	
} T_DATUM_CLIENT_DATA;

typedef struct {
	char name[32];
	
	// Called when a new threadpool thread is started
	DATUM_ThreadPool_Init_Func init_func;
	
	// Called at the beginning of each loop of the threadpool thread
	DATUM_ThreadPool_Loop_Func loop_func;
	
	// Called for each command the threadpool thread receives from a client
	DATUM_ThreadPool_ClientCmd_Func client_cmd_func;
	
	// Called each time a client is disconnected (either on purpose or via an error)
	DATUM_ThreadPool_ClientClosed_Func closed_client_func;
	
	// Called each time a new client connects and is assigned to a threadpool thread
	DATUM_ThreadPool_ClientNew_Func new_client_func;
	
	// TCP port this server will listen on
	int listen_port;
	
	// Maximum clients each thread can handle
	int max_clients_thread;
	
	// Maximum threads in the thread pool for this server
	int max_threads;
	
	// Maximum number of total clients for the server
	int max_clients;
	
	// Memory allocated by the app before starting the listener for max_threads worth of thread data
	// TODO: Dynamically allocate client_data and events
	T_DATUM_THREAD_DATA *datum_threads;
	
	atomic_int datum_active_threads;
} T_DATUM_SOCKET_APP;

typedef struct {
	// app functions and global data for socket app
	T_DATUM_SOCKET_APP *config;
	
	// application specific data for this thread
	void *data;
} T_DATUM_SOCKET_APP_THREAD_DATA;

typedef struct T_DATUM_THREAD_DATA {
	pthread_t pthread;
	
	bool is_active;
	
	bool has_new_clients;
	
	bool empty_request;
	bool has_client_kill_request;
	
	int thread_id;
	//int newBlockCount;
	
	// each client slot should have a pre-allocated chunk of memory
	// do not clear this entire structure!
	T_DATUM_CLIENT_DATA client_data[MAX_CLIENTS_THREAD];
	
	pthread_mutex_t thread_data_lock;
	
	int connected_clients;
	int next_open_client_index;
	
	// Clients with out_buf > 0. The threadpool write pass walks this, not every slot.
	int write_list[MAX_CLIENTS_THREAD];
	int pending_writes;
	
	struct epoll_event ev, events[MAX_CLIENTS_THREAD*2];
	int epollfd;
	int wake_fd;
	
	// information for this socket application
	// this is global to the socket application
	T_DATUM_SOCKET_APP *app;
	
	// Socket application data for threadpool
	// remember, this is per thread
	void *app_thread_data;
} T_DATUM_THREAD_DATA;

bool datum_sockets_setup_listening_sockets(const char * const purpose, const char * const addr, const uint16_t port, int * const out_socks, size_t * const inout_socks_n, const int backlog);
void *datum_gateway_listener_thread(void *arg);
[[nodiscard]] bool datum_socket_set_nonblock(int sock);
[[nodiscard]] bool datum_socket_setoptions(int sock);

int datum_socket_send_string_to_client(T_DATUM_CLIENT_DATA *c, char *s);
int datum_socket_send_chars_to_client(T_DATUM_CLIENT_DATA *c, char *s, int len);

int assign_to_thread(T_DATUM_SOCKET_APP *app, int fd);
void *datum_threadpool_thread(void *arg);

static inline void datum_socket_wake_thread(T_DATUM_THREAD_DATA *my) {
	uint64_t one = 1;
	if (my && my->wake_fd >= 0) {
		(void)write(my->wake_fd, &one, sizeof(one));
	}
}

static inline void datum_socket_note_pending_write(T_DATUM_THREAD_DATA *my, int cid) {
	if (!my || cid < 0 || cid >= MAX_CLIENTS_THREAD) return;
	if (my->client_data[cid].write_queued) return;
	if (my->pending_writes >= MAX_CLIENTS_THREAD) return;
	my->client_data[cid].write_queued = true;
	my->write_list[my->pending_writes++] = cid;
}

static inline void datum_socket_drop_pending_write(T_DATUM_THREAD_DATA *my, int cid) {
	int i;
	if (!my || cid < 0 || cid >= MAX_CLIENTS_THREAD) return;
	if (!my->client_data[cid].write_queued) return;
	my->client_data[cid].write_queued = false;
	for (i = 0; i < my->pending_writes; i++) {
		if (my->write_list[i] != cid) continue;
		my->write_list[i] = my->write_list[--my->pending_writes];
		return;
	}
}

static inline void datum_socket_thread_client_count_decrement(T_DATUM_THREAD_DATA *my, int cid_who_left, bool not_already_locked) {
	// compiler will optimize the if's away in most cases, since this is inline
	if (not_already_locked) pthread_mutex_lock(&my->thread_data_lock);
	
	// decrement connected client count for the thread
	my->connected_clients--;
	
	// if the ID we dropped is less than the expected next, drop it down to speed that up
	if (cid_who_left < my->next_open_client_index) {
		my->next_open_client_index = cid_who_left;
	}
	my->client_data[cid_who_left].fd = 0;
	my->client_data[cid_who_left].out_buf = 0;
	datum_socket_drop_pending_write(my, cid_who_left);
	if (not_already_locked) pthread_mutex_unlock(&my->thread_data_lock);
}

static inline void datum_socket_flush_writes(T_DATUM_THREAD_DATA *my) {
	if (!my) return;
	for (int n = 0; n < my->pending_writes; ) {
		int j = my->write_list[n];
		if (j < 0 || j >= my->app->max_clients_thread ||
		    my->client_data[j].fd == 0 || my->client_data[j].out_buf <= 0) {
			if (j >= 0 && j < my->app->max_clients_thread)
				my->client_data[j].write_queued = false;
			my->write_list[n] = my->write_list[--my->pending_writes];
			continue;
		}
		int sent = send(my->client_data[j].fd, my->client_data[j].w_buffer,
				my->client_data[j].out_buf, MSG_DONTWAIT);
		if (sent > 0) {
			if (sent < my->client_data[j].out_buf) {
				memmove(my->client_data[j].w_buffer,
					my->client_data[j].w_buffer + sent,
					my->client_data[j].out_buf - sent);
			}
			if (sent <= my->client_data[j].out_buf) {
				my->client_data[j].out_buf -= sent;
			} else {
				my->client_data[j].out_buf = 0;
			}
			if (my->client_data[j].out_buf <= 0) {
				my->client_data[j].out_buf = 0;
				my->client_data[j].write_queued = false;
				my->write_list[n] = my->write_list[--my->pending_writes];
				continue;
			}
		} else {
			if (!(errno == EAGAIN || errno == EWOULDBLOCK)) {
				epoll_ctl(my->epollfd, EPOLL_CTL_DEL, my->client_data[j].fd, NULL);
				close(my->client_data[j].fd);
				if (my->app->closed_client_func)
					my->app->closed_client_func(&my->client_data[j], "send error");
				datum_socket_thread_client_count_decrement(my, j, true);
				continue;
			}
		}
		n++;
	}
}

#endif
