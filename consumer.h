/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include <stdio.h>
#include "ring_buffer.h"
#include "packet.h"

typedef struct {
    so_action_t *action;
    unsigned long *hash;
    unsigned long *timestamp;
    size_t used;
    size_t cap;
    pthread_mutex_t access_lock;
} sorted_log_buffer_t;

typedef struct so_consumer_ctx_t {
	struct so_ring_buffer_t *producer_rb;

    /* TODO: add synchronization primitives for timestamp ordering */
	sorted_log_buffer_t *log_buffer;

    pthread_mutex_t write_lock;
    pthread_cond_t write_ready;

    const char *output_file;
    int done;
    int total;
} so_consumer_ctx_t;

int create_consumers(pthread_t *tids,
					int num_consumers,
					so_ring_buffer_t *rb,
					const char *out_filename);

#endif /* __SO_CONSUMER_H__ */
