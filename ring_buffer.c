// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	/* TODO: implement ring_buffer_init */
	// (void) ring;
	// (void) cap;

	if (ring == NULL || cap == 0)
		return -1;

	ring->data = malloc(cap);
	if (ring->data == NULL)
		return -1;

	ring->cap = cap;
	ring->write_pos = 0;
	ring->read_pos = 0;
	ring->len = 0;
	ring->stopped = 0;

	if (pthread_mutex_init(&ring->mutex, NULL) != 0) {
		free(ring->data);
		return -1;
	}

	if (pthread_cond_init(&ring->not_full, NULL) != 0) {
		pthread_mutex_destroy(&ring->mutex);
		free(ring->data);
		return -1;
	}

	if (pthread_cond_init(&ring->not_empty, NULL) != 0) {
		pthread_cond_destroy(&ring->not_full);
		pthread_mutex_destroy(&ring->mutex);
		free(ring->data);
		return -1;
	}

	return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: implement ring_buffer_enqueue */
	// (void) ring;
	// (void) data;
	// (void) size;

	if (ring == NULL || size == 0)
		return -1;

	pthread_mutex_lock(&ring->mutex);

	while (ring->len + size > ring->cap && !ring->stopped)
		pthread_cond_wait(&ring->not_full, &ring->mutex);

	if (ring->stopped) {
		pthread_mutex_unlock(&ring->mutex);
		return -1;
	}

	size_t i = 0;
	unsigned char *src = (unsigned char *)data;

	while (i < size) {
		size_t w_index = (ring->write_pos + i) % ring->cap;
		size_t chunk_size = size - i;
		size_t until_end = ring->cap - w_index;

		if (chunk_size > until_end)
			chunk_size = until_end;

		memcpy((unsigned char *)ring->data + w_index, src + i, chunk_size);
		i += chunk_size;
	}

	ring->write_pos = (ring->write_pos + size) % ring->cap;
	ring->len += size;

	pthread_cond_signal(&ring->not_empty);
	pthread_mutex_unlock(&ring->mutex);

	return (ssize_t)size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: Implement ring_buffer_dequeue */

	// (void) ring;
	// (void) data;
	// (void) size;

	if (ring == NULL || size == 0)
		return -1;

	pthread_mutex_lock(&ring->mutex);

	while (ring->len < size && !ring->stopped)
		pthread_cond_wait(&ring->not_empty, &ring->mutex);

	if (ring->stopped && ring->len < size) {
		pthread_mutex_unlock(&ring->mutex);
		return -1;
	}

	size_t i = 0;
	unsigned char *dest = (unsigned char *)data;

	while (i < size) {
		size_t r_index = (ring->read_pos + i) % ring->cap;
		size_t chunk_size = size - i;
		size_t until_end = ring->cap - r_index;

		if (chunk_size > until_end)
			chunk_size = until_end;

		memcpy(dest + i, (unsigned char *)ring->data + r_index, chunk_size);
		i += chunk_size;
	}

	ring->read_pos = (ring->read_pos + size) % ring->cap;
	ring->len -= size;

	pthread_cond_signal(&ring->not_full);
	pthread_mutex_unlock(&ring->mutex);

	return (ssize_t)size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_destroy */
	// (void) ring;

	if (ring == NULL)
		return;

	pthread_cond_destroy(&ring->not_empty);
	pthread_cond_destroy(&ring->not_full);
	pthread_mutex_destroy(&ring->mutex);

	if (ring->data) {
		free(ring->data);
		ring->data = NULL;
	}

	ring->cap = 0;
	ring->read_pos = 0;
	ring->write_pos = 0;
	ring->len = 0;
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_stop */
	// (void) ring;


	if (ring == NULL)
		return;


	ring->stopped = 1;
	pthread_mutex_lock(&ring->mutex);

	pthread_cond_broadcast(&ring->not_empty);
	pthread_cond_broadcast(&ring->not_full);
	pthread_mutex_unlock(&ring->mutex);
}
