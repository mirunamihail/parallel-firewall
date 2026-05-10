// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

int init_log_buffer(sorted_log_buffer_t *buf)
{
	buf->cap = 16;
	buf->used = 0;

	buf->action = malloc(buf->cap * sizeof(so_action_t));
	buf->hash = malloc(buf->cap * sizeof(unsigned long));
	buf->timestamp = malloc(buf->cap * sizeof(unsigned long));

	if (!buf->action || !buf->hash || !buf->timestamp) {
		free(buf->action);
		free(buf->hash);
		free(buf->timestamp);
		return -1;
	}

	pthread_mutex_init(&buf->access_lock, NULL);
	return 0;
}

size_t find_insert_pos(sorted_log_buffer_t *buf, unsigned long t)
{
	size_t left = 0;
	size_t right = buf->used;

	while (left < right) {
		size_t mid = left + (right - left) / 2;

		if (buf->timestamp[mid] < t)
			left = mid + 1;
		else
			right = mid;
	}

	return left;
}

void insert_sorted(sorted_log_buffer_t *buf, so_action_t action, unsigned long hash, unsigned long ts)
{
	pthread_mutex_lock(&buf->access_lock);

	if (buf->used >= buf->cap) {
		size_t new_size = buf->cap * 2;

		so_action_t *new_action = realloc(buf->action, new_size * sizeof(so_action_t));
		unsigned long *new_hash = realloc(buf->hash, new_size * sizeof(unsigned long));
		unsigned long *new_ts = realloc(buf->timestamp, new_size * sizeof(unsigned long));

		if (new_action && new_hash && new_ts) {
			buf->action = new_action;
			buf->hash = new_hash;
			buf->timestamp = new_ts;
			buf->cap = new_size;
		}
	}

	size_t pos = find_insert_pos(buf, ts);

	if (pos < buf->used) {
		memmove(&buf->action[pos + 1], &buf->action[pos],
				(buf->used - pos) * sizeof(so_action_t));
		memmove(&buf->hash[pos + 1], &buf->hash[pos],
				(buf->used - pos) * sizeof(unsigned long));
		memmove(&buf->timestamp[pos + 1], &buf->timestamp[pos],
				(buf->used - pos) * sizeof(unsigned long));
	}

	buf->action[pos] = action;
	buf->hash[pos] = hash;
	buf->timestamp[pos] = ts;
	buf->used++;

	pthread_mutex_unlock(&buf->access_lock);
}

void consumer_thread(so_consumer_ctx_t *ctx)
{
	/* TODO: implement consumer thread */
	so_packet_t packet;
	ssize_t result;
	//char line[128];

	while (1) {
		result = ring_buffer_dequeue(ctx->producer_rb, &packet, PKT_SZ);

		if (result == -1)
			break;

		so_action_t drop = process_packet(&packet);
		unsigned long pkt_hash = packet_hash(&packet);
		unsigned long pkt_time = packet.hdr.timestamp;

		insert_sorted(ctx->log_buffer, drop, pkt_hash, pkt_time);
	}

	pthread_mutex_lock(&ctx->write_lock);
	ctx->done++;

	if (ctx->done == ctx->total)
		pthread_cond_broadcast(&ctx->write_ready);
	pthread_mutex_unlock(&ctx->write_lock);

	pthread_mutex_lock(&ctx->write_lock);
	while (ctx->done < ctx->total)
		pthread_cond_wait(&ctx->write_ready, &ctx->write_lock);

	static int file_written;

	if (!file_written) {
		file_written = 1;
		int file = open(ctx->output_file, O_WRONLY | O_CREAT | O_APPEND, 0644);

		if (file < 0)
			return;
		char line[128];

		for (size_t i = 0; i < ctx->log_buffer->used; i++) {
			int out = snprintf(line, sizeof(line), "%s %016lx %lu\n",
							RES_TO_STR(ctx->log_buffer->action[i]),
							ctx->log_buffer->hash[i],
							ctx->log_buffer->timestamp[i]);
			write(file, line, out);
		}
		close(file);
	}

	pthread_mutex_unlock(&ctx->write_lock);

	//free(ctx);

	// (void) ctx;
}

void *consumer_thread_wrapper(void *arg)
{
	so_consumer_ctx_t *ctx = (so_consumer_ctx_t *)arg;

	consumer_thread(ctx);
	return NULL;
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{
	// (void) tids;
	// (void) num_consumers;
	// (void) rb;
	// (void) out_filename;

	so_consumer_ctx_t *shared = malloc(sizeof(so_consumer_ctx_t));

	if (!shared)
		return -1;

	shared->log_buffer = malloc(sizeof(sorted_log_buffer_t));
	if (!shared->log_buffer) {
		free(shared);
		return -1;
	}

	if (init_log_buffer(shared->log_buffer) != 0) {
		free(shared->log_buffer);
		free(shared);
		return -1;
	}

	shared->producer_rb = rb;
	shared->output_file = out_filename;
	shared->done = 0;
	shared->total = num_consumers;

	pthread_mutex_init(&shared->write_lock, NULL);
	pthread_cond_init(&shared->write_ready, NULL);

	int created = 0;

	for (int i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/

		if (pthread_create(&tids[i], NULL, (void *(*)(void *))consumer_thread_wrapper, shared) == 0)
			created++;
	}

	return created;
}
