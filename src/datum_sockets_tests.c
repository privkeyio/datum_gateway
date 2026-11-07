#include "datum_sockets.h"
#include <sys/epoll.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <poll.h>
#include <sys/eventfd.h>

#define LAT_MAX_SAMPLES 10000

static atomic_int test_cmd_count;
static atomic_int test_close_count;
static atomic_int test_new_client_count;
static atomic_int test_kill_count;

static inline void datum_socket_request_stop(T_DATUM_THREAD_DATA *my) {
	if (!my) return;
	atomic_store_explicit(&my->stop_requested, true, memory_order_release);
	datum_socket_wake_thread(my);
}

static void test_new_client(T_DATUM_CLIENT_DATA *c) {
	(void)c;
	atomic_fetch_add_explicit(&test_new_client_count, 1, memory_order_relaxed);
}

static void test_closed_client(T_DATUM_CLIENT_DATA *c, const char *reason) {
	(void)c; (void)reason;
	atomic_fetch_add_explicit(&test_close_count, 1, memory_order_relaxed);
}
static int test_cmd(T_DATUM_CLIENT_DATA *c, char *line) {
	(void)c; (void)line;
	atomic_fetch_add_explicit(&test_cmd_count, 1, memory_order_relaxed);
	return 0;
}

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
	uint64_t samples[LAT_MAX_SAMPLES];
};

static void lat_record(struct lat_result *r, uint64_t us) {
	if (r->count == 0 || us < r->min_us) r->min_us = us;
	if (us > r->max_us) r->max_us = us;
	r->sum_us += us;
	if (r->count < LAT_MAX_SAMPLES)
		r->samples[r->count] = us;
	r->count++;
}

static int lat_cmp(const void *a, const void *b) {
	uint64_t ua = *(const uint64_t *)a;
	uint64_t ub = *(const uint64_t *)b;
	return (ua > ub) - (ua < ub);
}

static void lat_print(const char *label, const struct lat_result *r) {
	if (r->count == 0) return;
	int n = r->count < LAT_MAX_SAMPLES ? r->count : LAT_MAX_SAMPLES;
	uint64_t *s = malloc(n * sizeof(uint64_t));
	if (!s) return;
	memcpy(s, r->samples, n * sizeof(uint64_t));
	qsort(s, n, sizeof(uint64_t), lat_cmp);
	uint64_t p50  = s[n * 50 / 100];
	uint64_t p90  = s[n * 90 / 100];
	uint64_t p99  = s[n * 99 / 100];
	uint64_t p999 = s[n * 999 / 1000];
	printf("%s: min=%llu p50=%llu p90=%llu p99=%llu p99.9=%llu max=%llu avg=%llu us (n=%d)\n",
	       label,
	       (unsigned long long)r->min_us,
	       (unsigned long long)p50,
	       (unsigned long long)p90,
	       (unsigned long long)p99,
	       (unsigned long long)p999,
	       (unsigned long long)r->max_us,
	       (unsigned long long)(r->sum_us / r->count),
	       r->count);
	free(s);
}

static void run_real_wake_samples(struct lat_result *r_new, struct lat_result *r_kill,
                                  struct lat_result *r_write, int seconds) {
	T_DATUM_SOCKET_APP app = {0};
	T_DATUM_THREAD_DATA *td = calloc(1, sizeof(*td));
	assert(td);
	int sp[2] = {-1, -1};
	int sp2[2] = {-1, -1};

	app.max_clients_thread = 4;
	app.client_cmd_func = test_cmd;
	app.new_client_func = test_new_client;
	app.closed_client_func = test_closed_client;
	app.datum_threads = td;
	app.max_threads = 1;

	pthread_mutex_init(&td->thread_data_lock, NULL);
	pthread_mutex_init(&td->wake_lock, NULL);
	atomic_init(&td->worker_state, DATUM_WORKER_STARTING);
	atomic_init(&td->stop_requested, false);
	atomic_init(&td->empty_request, false);
	atomic_init(&td->has_client_kill_request, false);
	td->app = &app;
	td->wake_fd = -1;
	td->wake_write_fd = -1;
	td->epollfd = -1;

	errno = 0;
	int spret = socketpair(AF_UNIX, SOCK_STREAM, 0, sp);
	if (spret != 0 || !datum_socket_set_nonblock(sp[0])) {
		fprintf(stderr, "socketpair/nonblock setup failed: %s\n", strerror(errno));
		free(td);
		exit(1);
	}

	assert(datum_socket_set_nonblock(sp[0]));
	td->client_data[0].fd = sp[0];
	td->client_data[0].cid = 0;
	td->client_data[0].datum_thread = td;
	td->client_data[0].new_connection = true;
	td->client_data[0].kill_request = false;
	td->connected_clients = 1;
	td->has_new_clients = true;

	pthread_t th;
	int rc = pthread_create(&th, NULL, datum_threadpool_thread, td);
	if (rc != 0) {
		fprintf(stderr, "pthread_create failed: %s\n", strerror(rc));
		free(td);
		exit(1);
	}
	for (int i = 0; i < 200; i++) {
		if (atomic_load_explicit(&td->worker_state, memory_order_acquire) == DATUM_WORKER_RUNNING)
			break;
		usleep(10000);
	}
	int state = atomic_load_explicit(&td->worker_state, memory_order_acquire);
	if (state != DATUM_WORKER_RUNNING) {
		datum_socket_request_stop(td);
		pthread_join(th, NULL);
		free(td);
		fprintf(stderr, "worker did not reach RUNNING\n");
		exit(1);
	}

	usleep(50000);
	atomic_store(&test_cmd_count, 0);
	atomic_store(&test_new_client_count, 0);
	atomic_store(&test_kill_count, 0);

	uint64_t deadline = now_us() + (uint64_t)seconds * 1000000ull;

	/* write samples while the client is still alive */
	while (now_us() < deadline) {
		usleep(rand() % 5000);
		atomic_store(&test_cmd_count, 0);
		uint64_t start = now_us();
		ssize_t w = write(sp[1], "x\n", 2);
		(void)w;
		while (atomic_load(&test_cmd_count) == 0 && now_us() < deadline + 1000000ull)
			usleep(1);
		if (atomic_load(&test_cmd_count) == 0) {
			fprintf(stderr, "wake never arrived for write sample\n");
			exit(1);
		}
		lat_record(r_write, now_us() - start);
	}

	/* one new-client */
	atomic_store(&test_new_client_count, 0);

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, sp2) == 0) {
		(void)datum_socket_set_nonblock(sp2[0]);

		pthread_mutex_lock(&td->thread_data_lock);

		td->client_data[1].fd = sp2[0];
		td->client_data[1].cid = 1;
		td->client_data[1].datum_thread = td;
		td->client_data[1].new_connection = true;
		td->client_data[1].kill_request = false;
		td->client_data[1].write_queued = false;

		td->connected_clients++;
		td->has_new_clients = true;

		uint64_t start = now_us();

		datum_socket_wake_thread(td);

		pthread_mutex_unlock(&td->thread_data_lock);

		while (atomic_load(&test_new_client_count) == 0 && now_us() < start + 50000ull) {
			usleep(1);
		}

		if (atomic_load(&test_new_client_count) == 0) {
			fprintf(stderr, "wake never arrived for new-client\n");
			exit(1);
		}

		lat_record(r_new, now_us() - start);
	}

	/* one kill */
	atomic_store(&test_close_count, 0);

	pthread_mutex_lock(&td->thread_data_lock);

	td->client_data[0].kill_request = true;

	atomic_store_explicit(&td->has_client_kill_request, true, memory_order_release);

	uint64_t start = now_us();

	datum_socket_wake_thread(td);

	pthread_mutex_unlock(&td->thread_data_lock);

	while (atomic_load(&test_close_count) == 0 && now_us() < start + 50000ull) {
		usleep(1);
	}

	if (atomic_load(&test_close_count) == 0) {
		fprintf(stderr, "wake never arrived for kill\n");
		exit(1);
	}

	lat_record(r_kill, now_us() - start);

	datum_socket_request_stop(td);
	pthread_join(th, NULL);
	if (sp[1] >= 0) close(sp[1]);
	if (sp2[1] >= 0) close(sp2[1]);
	pthread_mutex_destroy(&td->thread_data_lock);
	pthread_mutex_destroy(&td->wake_lock);
	free(td);
	return;
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
