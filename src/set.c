#include <string.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <sys/time.h>
#include <errno.h>
#include "set.h"
#include "serialize.h"
#include "sparse.h"
#include "type_compat.h"

/*
 * Static delarations
 */
static int thread_safe_fault(struct hlld_set *f);
static int timediff_msec(struct timeval *t1, struct timeval *t2);

// Link the external murmur hash in
extern void MurmurHash3_x64_128(const void * key, const int len, const uint32_t seed, void *out);

/**
 * Initializes a set wrapper.
 * @arg config The configuration to use
 * @arg full_key The name of the set
 * @arg discover Should existing data files be discovered. Otherwise
 * they will be faulted in on-demand.
 * @arg set Output parameter, the new set
 * @return 0 on success
 */
int init_set(
    struct hlld_config *config, char *full_key,
    int discover, struct hlld_set **set
) {
    // Allocate the buffers
    struct hlld_set *s = *set = (struct hlld_set*)calloc(1, sizeof(struct hlld_set));

    // Initialize
    s->is_dirty = 1;
    s->is_proxied = 1;
    s->hll.precision = config->default_precision;

    s->is_config_dirty = 0;

    // Store the things
    s->config = config;
    s->full_key = strdup(full_key);
    s->full_key_len = strlen(full_key);

    // Copy set configs
    s->set_config.default_eps = config->default_eps;
    s->set_config.default_precision = config->default_precision;
    s->set_config.in_memory = config->in_memory;
    s->set_config.sliding_period = config->sliding_period;
    s->set_config.sliding_precision = config->sliding_precision;

    // Initialize the locks
    INIT_HLLD_SPIN(&s->hll_update);
    pthread_mutex_init(&s->hll_lock, NULL);

    // Discover the existing set if we need to
    int res = 0;
    if (discover) {
        res = thread_safe_fault(s);
        if (res) {
            syslog(LOG_ERR, "Failed to fault in the set '%s'. Err: %d", s->full_key, res);
        }
    }

    return res;
}

/**
 * Destroys a set
 * @arg set The set to destroy
 * @return 0 on success
 */
int destroy_set(struct hlld_set *set) {
    // Close first
    hset_close(set);

    // Cleanup
    free(set->full_key);
    free(set);
    return 0;
}

/**
 * Gets the counters that belong to a set
 * @notes Thread safe, but may be inconsistent.
 * @arg set The set
 * @return A reference to the counters of a set
 */
set_counters* hset_counters(struct hlld_set *set) {
    return &set->counters;
}

/**
 * Checks if a set is currectly mapped into
 * memory or if it is proxied.
 * @notes Thread safe.
 * @return 0 if in-memory, 1 if proxied.
 */
int hset_is_proxied(struct hlld_set *set) {
    return set->is_proxied;
}

/**
 * Flushes the set. Idempotent if the
 * set is proxied or not dirty.
 * @arg set The set to close
 * @return 0 on success.
 */
int hset_flush(struct hlld_set *set) {
    // Only do things if we are non-proxied
    if (set->is_proxied)
        return 0;

    // Time how long this takes
    struct timeval start, end;
    gettimeofday(&start, NULL);

    // If we are not dirty, nothing to do
    if (!set->is_dirty)
        return 0;

    // Turn dirty off
    set->is_dirty = 0;

    // Flush the set
    int res = 0;
    if (!set->set_config.in_memory) {
        res = serialize_hll_to_sparsedb(
            sparse_get_global(), &set->hll,
            set->full_key, set->full_key_len
        );
    }

    // Compute the elapsed time
    gettimeofday(&end, NULL);
    syslog(LOG_DEBUG, "Flushed set '%s'. Total time: %d msec.",
            set->full_key, timediff_msec(&start, &end));
    return res;
}

/**
 * Gracefully closes a set.
 * @arg set The set to close
 * @return 0 on success.
 */
int hset_close(struct hlld_set *set) {
    // Acquire lock
    pthread_mutex_lock(&set->hll_lock);

    // Only act if we are non-proxied
    if (!set->is_proxied) {
        hset_flush(set);
        hll_destroy(&set->hll);
        set->is_proxied = 1;
        set->counters.page_outs += 1;
    }

    // Release lock
    pthread_mutex_unlock(&set->hll_lock);
    return 0;
}

/**
 * Deletes the set with extreme prejudice.
 * @arg set The set to delete
 * @return 0 on success.
 */
int hset_delete(struct hlld_set *set) {
    // Close first
    hset_close(set);

    // @TODO: This should delete the key rather than writing an empty set
    if (sparse_write_dense_data(
        sparse_get_global(),
        set->full_key, set->full_key_len,
        (unsigned char *)"", 0
    )) {
        syslog(LOG_ERR, "Failed to delete: %s. %s", set->full_key, strerror(errno));
    }

    return 0;
}

/**
 * Adds a key to the given set
 * @arg set The set to add to
 * @arg key The key to add
 * @return 0 on success.
 */
int hset_add_hash(struct hlld_set *set, uint64_t hash, time_t timestamp) {
    if (set->is_proxied) {
        if (thread_safe_fault(set) != 0) return -1;
    }

    // Add the hashed value and update the
    // counters
    LOCK_HLLD_SPIN(&set->hll_update);
    hll_add_hash_at_time(&set->hll, hash, timestamp);
    set->counters.sets += 1;
    UNLOCK_HLLD_SPIN(&set->hll_update);

    // Mark as dirty
    set->is_dirty = 1;
    return 0;
}

int hset_add(struct hlld_set *set, char *key, time_t timestamp) {
    // Compute the hash value of the key. We do this
    // so that we can use the hll_add_hash instead of
    // hll_add. This way, the expensive CPU bit can
    // be done without holding a lock
    uint64_t out[2];
    MurmurHash3_x64_128(key, strlen(key), 0, &out);

    return hset_add_hash(set, out[1], timestamp);
}

/**
 * Gets the size of the set
 * @note Thread safe.
 * @arg set The set to check
 * @return The estimated size of the set
 */
uint64_t hset_size_total(struct hlld_set *set) {
    if (set->is_proxied) {
        if (thread_safe_fault(set) != 0) return -1;
    }
    return hll_size_total(&set->hll);
}

uint64_t hset_size(struct hlld_set *set, time_t timestamp, uint64_t time_window) {
    if (set->is_proxied) {
        if (thread_safe_fault(set) != 0) return -1;
    }
    return hll_size(&set->hll, timestamp, (int)time_window);
}

/**
 * Gets the size of the union of a few sets
 * @arg sets num_sets number of sets that we take the union of
 * @arg num_sets number of sets we're taking the union of
 * @arg time_window the amount of time we're counting
 * @arg timestamp the current time
 */
uint64_t hset_size_union(struct hlld_set **sets, int num_sets, time_t timestamp, uint64_t time_window) {
    hll_t **hlls = (hll_t **)malloc(sizeof(hll_t)*num_sets);
    for(int i=0; i<num_sets; i++) {
        hlls[i] = &sets[i]->hll;
    }
    uint64_t result = hll_union_size(hlls, num_sets, timestamp,  (int)time_window);
    free(hlls);
    return result;
}

/**
 * Gets the byte size of the set
 * @note Thread safe.
 * @arg set The set
 * @return The total byte size of the set
 */
uint64_t hset_byte_size(struct hlld_set *set) {
    return hll_bytes_for_precision(set->set_config.default_precision);
}

/**
 * Provides a thread safe faulting of the set.
 */
static int thread_safe_fault(struct hlld_set *s) {
    // Acquire lock
    int res = 0;
    char *full_key = NULL;
    pthread_mutex_lock(&s->hll_lock);

    // Bail if we already faulted in
    if (!s->is_proxied)
        goto LEAVE;

    // Get the mode for our bitmap
    if (s->set_config.in_memory) {

        s->is_proxied = 0;
        res = hll_init(
                s->set_config.default_precision,
                s->set_config.sliding_period, 
                s->set_config.sliding_precision,
                &s->hll); 
        // Skip the fault in
        goto LEAVE;

    }

    res = unserialize_hll_from_sparsedb(
        sparse_get_global(), &s->hll,
        s->full_key, s->full_key_len
    );

    // If the hll was not available, setup a new one
    if (res == -2) {
      syslog(LOG_ERR, "hll not found in sparsedb: %s", s->full_key);
      s->is_proxied = 0;
      res = hll_init(
              s->set_config.default_precision,
              s->set_config.sliding_period, 
              s->set_config.sliding_precision,
              &s->hll); 
      goto LEAVE;
    }

    if (res) {
        syslog(LOG_ERR, "Failed to load hll: %s. %s", s->full_key, strerror(errno));
        goto LEAVE;
    }

    s->is_proxied = 0;
    s->counters.page_ins += 1;

LEAVE:
    // Release lock
    pthread_mutex_unlock(&s->hll_lock);
    if (full_key) free(full_key);

    return res;
}

/**
 * Computes the difference in time in milliseconds
 * between two timeval structures.
 */
static int timediff_msec(struct timeval *t1, struct timeval *t2) {
    uint64_t micro1 = t1->tv_sec * 1000000 + t1->tv_usec;
    uint64_t micro2= t2->tv_sec * 1000000 + t2->tv_usec;
    return (micro2-micro1) / 1000;
}

