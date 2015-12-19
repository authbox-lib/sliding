#ifndef SPARSE_H
#define SPARSE_H

#include "config.h"

#define HLL_IS_DENSE -2
/**
 * Opaque handle to the set manager
 */
typedef struct slidingd_sparsedb slidingd_sparsedb;

typedef struct {
    time_t timestamp;
    uint64_t hash;
} hll_sparse_point;

typedef struct {
    int capacity;
    int size;
    hll_sparse_point *points;
} hll_sparse;

/**
 * Initializer
 * @return 0 on success.
 */
int init_sparse(struct hlld_config *config, struct slidingd_sparsedb **sparsedb);
int destroy_sparse(struct slidingd_sparsedb *sparsedb);
char *sparse_get_stats(struct slidingd_sparsedb *sparsedb);

int sparse_drop(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len
);
int sparse_get_points(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len
);
int sparse_is_dense(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len
);
int sparse_size_total(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len
);
int sparse_size(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len,
    time_t timestamp, unsigned int time_window
);
int sparse_add(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len,
    uint64_t *hashes, int hash_count,
    time_t timestamp
);
int sparse_convert_dense(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len,
    struct hlld_set *set
);

struct slidingd_sparsedb *sparse_get_global(void);

int sparse_write_dense_data(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len,
    const unsigned char *data, size_t data_len
);
int sparse_read_dense_data(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len,
    unsigned char **output, size_t *data_len
);
int sparse_get_points(
    struct slidingd_sparsedb *sparsedb,
    const char *set_name, int set_name_len,
    hll_sparse_point **points, size_t *size
);
#endif
