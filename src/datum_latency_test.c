#include <stdio.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <errno.h>

/* Declared in the file that currently owns the harness. */
void datum_sockets_latency_long_test(void);

int main(void) {
	printf("Running 20s latency comparison...\n");
	datum_sockets_latency_long_test();
	return 0;
}
