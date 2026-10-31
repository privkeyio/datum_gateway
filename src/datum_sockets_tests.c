#include "datum_sockets.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>

static uint64_t now_us(void) {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000ull + ts.tv_nsec / 1000ull;
}

struct lat_result {
	uint64_t min_us;
	uint64_t max_us;
	uint64_t sum_us;
	int count;
};

static void lat_record(struct lat_result *r, uint64_t us) {
	if (r->count == 0 || us < r->min_us) r->min_us = us;
	if (us > r->max_us) r->max_us = us;
	r->sum_us += us;
	r->count++;
}

static void lat_print(const char *label, const struct lat_result *r) {
	if (r->count == 0) return;
	printf("%s: min=%llu avg=%llu max=%llu us (n=%d)\n",
	       label,
	       (unsigned long long)r->min_us,
	       (unsigned long long)(r->sum_us / r->count),
	       (unsigned long long)r->max_us,
	       r->count);
}

/* ---- Real wake path harness ---- */
struct wake_harness {
	T_DATUM_THREAD_DATA td;
	volatile int saw_new_client;
	volatile int saw_kill;
	volatile int saw_write;
	volatile uint64_t saw_us;
	volatile int stop;
};

static void *wake_harness_thread(void *arg) {
	struct wake_harness *h = arg;
	T_DATUM_THREAD_DATA *my = &h->td;
	my->epollfd = epoll_create1(EPOLL_CLOEXEC);
	assert(my->epollfd >= 0);
	assert(my->wake_fd >= 0);
	struct epoll_event wev = { .events = EPOLLIN, .data.u64 = (uint64_t)-1 };
	if (epoll_ctl(my->epollfd, EPOLL_CTL_ADD, my->wake_fd, &wev) != 0) {
		fprintf(stderr, "epoll_ctl failed: %s\n", strerror(errno));
		return NULL;
	}
	while (!h->stop) {
		struct epoll_event ev;
		int nfds = epoll_wait(my->epollfd, &ev, 1, 100);
		if (nfds > 0 && ev.data.u64 == (uint64_t)-1) {
			uint64_t v;
			while (read(my->wake_fd, &v, sizeof(v)) > 0) {}
			h->saw_us = now_us();
			if (my->has_new_clients) { h->saw_new_client = 1; my->has_new_clients = false; }
			if (my->has_client_kill_request) { h->saw_kill = 1; my->has_client_kill_request = false; }
			if (my->pending_writes > 0) { h->saw_write = 1; my->pending_writes = 0; }
		}
	}
	close(my->wake_fd);
	close(my->epollfd);
	return NULL;
}

static void run_real_wake_samples(struct lat_result *r_new, struct lat_result *r_kill,
				  struct lat_result *r_write, int seconds) {
	struct wake_harness *h = calloc(1, sizeof(*h));
	assert(h);
	pthread_mutex_init(&h->td.thread_data_lock, NULL);
	pthread_t th;
	h->td.wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	pthread_create(&th, NULL, wake_harness_thread, h);
	usleep(20000);
	uint64_t deadline = now_us() + (uint64_t)seconds * 1000000ull;
	while (now_us() < deadline) {
		usleep(rand() % 5000);
		uint64_t start = now_us();
		h->td.has_new_clients = true;
		datum_socket_wake_thread(&h->td);
		while (!h->saw_new_client && now_us() < deadline + 1000000ull) usleep(50);
		if (h->saw_new_client) {
			lat_record(r_new, h->saw_us - start);
			h->saw_new_client = 0;
		}
		usleep(rand() % 5000);
		start = now_us();
		h->td.has_client_kill_request = true;
		datum_socket_wake_thread(&h->td);
		while (!h->saw_kill && now_us() < deadline + 1000000ull) usleep(50);
		if (h->saw_kill) {
			lat_record(r_kill, h->saw_us - start);
			h->saw_kill = 0;
		}
		usleep(rand() % 5000);
		start = now_us();
		h->td.pending_writes = 1;
		datum_socket_wake_thread(&h->td);
		while (!h->saw_write && now_us() < deadline + 1000000ull) usleep(50);
		if (h->saw_write) {
			lat_record(r_write, h->saw_us - start);
			h->saw_write = 0;
		}
	}
	h->stop = 1;
	datum_socket_wake_thread(&h->td);
	pthread_join(th, NULL);
	pthread_mutex_destroy(&h->td.thread_data_lock);
	free(h);
}

/* ---- Old 7ms simulation (long test only) ---- */
struct old_harness {
	volatile int flag;
	volatile uint64_t saw_us;
	volatile int stop;
};

static void *old_harness_thread(void *arg) {
	struct old_harness *h = arg;
	int epfd = epoll_create1(EPOLL_CLOEXEC);
	assert(epfd >= 0);
	while (!h->stop) {
		struct epoll_event ev;
		epoll_wait(epfd, &ev, 1, 7);
		if (h->flag) {
			h->saw_us = now_us();
			h->flag = 0;
		}
	}
	close(epfd);
	return NULL;
}

static void run_old_path_samples(struct lat_result *r, int seconds) {
	struct old_harness h = {0};
	pthread_t th;
	pthread_create(&th, NULL, old_harness_thread, &h);
	usleep(20000);
	uint64_t deadline = now_us() + (uint64_t)seconds * 1000000ull;
	while (now_us() < deadline) {
		usleep(rand() % 10000);
		uint64_t start = now_us();
		h.flag = 1;
		while (h.flag && now_us() < deadline + 1000000ull)
			usleep(50);
		if (!h.flag)
			lat_record(r, h.saw_us - start);
	}
	h.stop = 1;
	pthread_join(th, NULL);
}

/* ---- Entry points ---- */
void datum_sockets_eventfd_tests(void) {
	srand(time(NULL));
	struct lat_result r_new = {0}, r_kill = {0}, r_write = {0};
	run_real_wake_samples(&r_new, &r_kill, &r_write, 1);
	lat_print("real wake path new-client (1s)", &r_new);
	lat_print("real wake path kill (1s)", &r_kill);
	lat_print("real wake path write (1s)", &r_write);
}

void datum_sockets_latency_long_test(void) {
	srand(time(NULL));
	struct lat_result r_new = {0}, r_kill = {0}, r_write = {0}, r_old = {0};
	printf("Running 20s real wake path...\n");
	run_real_wake_samples(&r_new, &r_kill, &r_write, 20);
	lat_print("real wake path new-client (20s)", &r_new);
	lat_print("real wake path kill (20s)", &r_kill);
	lat_print("real wake path write (20s)", &r_write);
	printf("Running 20s old 7ms simulation...\n");
	run_old_path_samples(&r_old, 20);
	lat_print("old 7ms path (20s)", &r_old);
}
