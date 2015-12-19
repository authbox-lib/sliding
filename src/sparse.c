#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <rocksdb/c.h>

#include "hll.h"
#include "set.h"
#include "sparse.h"

static const char DENSE_PREFIX[] = "dense~";
static const int DENSE_PREFIX_LEN = sizeof(DENSE_PREFIX) - 1;


struct slidingd_sparsedb {
    struct hlld_config *config;
    rocksdb_options_t *options;
    rocksdb_t *db;
};

struct slidingd_sparsedb *global_sparse = NULL;

struct slidingd_sparsedb *sparse_get_global(void) {
  return global_sparse;
}

int init_sparse(struct hlld_config *config, struct slidingd_sparsedb **sparsedb) {
  // Allocate a new object
  *sparsedb = (struct slidingd_sparsedb*)calloc(1, sizeof(struct slidingd_sparsedb));

  // Copy the config
  (*sparsedb)->config = config;

  (*sparsedb)->options = rocksdb_options_create();

  // Optimize RocksDB. This is the easiest way to
  // get RocksDB to perform well
  rocksdb_options_increase_parallelism(
      (*sparsedb)->options,
      (int)(sysconf(_SC_NPROCESSORS_ONLN))
  );
  rocksdb_options_optimize_level_style_compaction(
      (*sparsedb)->options,
      config->memtable_memory
  );

  // create the DB if it's not already present
  rocksdb_options_set_create_if_missing((*sparsedb)->options, 1);

  // open DB
  char *err = NULL;
  (*sparsedb)->db = rocksdb_open((*sparsedb)->options, config->data_dir, &err);
  if (err) {
      syslog(LOG_ERR, "failed to open sliding sparse rocksdb");
      return -1;
  }

  global_sparse = (*sparsedb);

  return 0;
}

int destroy_sparse(struct slidingd_sparsedb *sparsedb) {
  rocksdb_options_destroy(sparsedb->options);
  rocksdb_close(sparsedb->db);

  if (global_sparse == sparsedb) {
    global_sparse = NULL;
  }

  return 0;
}

char *sparse_get_stats(struct slidingd_sparsedb *sparsedb) {
    return rocksdb_property_value(sparsedb->db, "rocksdb.stats");
}

/**
 * Drop a sparse hyperloglog
 * @return 0 if success
 *         HLL_IS_DENSE if the we should use a dense set
 */
int sparse_drop(
    struct slidingd_sparsedb *sparsedb,
    const char *set_name, int set_name_len
) {
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();

    char *err = NULL;
    rocksdb_delete(
        sparsedb->db, writeoptions,
        set_name, set_name_len,
        &err
    );
    if (err) {
        syslog(LOG_ERR, "failed to delete sparse key");
        return -1;
    }

    rocksdb_writeoptions_destroy(writeoptions);
    return 0;
}

/**
 * Check if a set is dense
 * @return 1 if dense
 *         0 if sparse
 *         -1 if missing
 *         -2 on error
 */
int sparse_is_dense(
    struct slidingd_sparsedb *sparsedb,
    const char *set_name, int set_name_len
) {
    size_t len;
    char *err = NULL;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    hll_sparse_point *points = (hll_sparse_point *)
        rocksdb_get(sparsedb->db, readoptions, set_name, set_name_len, &len, &err);
    if (err) {
        syslog(LOG_ERR, "failed to fetch sparse points from rocksdb");
        return -2;
    }

    // If the length is 1 this is a dense set
    rocksdb_readoptions_destroy(readoptions);
    free(points);

    if (len == 0) {
      return -1;
    } else if (len == 1) {
        return 1;
    } else {
        return 0;
    }
}

/**
 * Fetch all the points in a hll
 * @return 0 if success
 *         -1 on error
 *         HLL_IS_DENSE if the we should use a dense set
 */
int sparse_get_points(
    struct slidingd_sparsedb *sparsedb,
    const char *set_name, int set_name_len,
    hll_sparse_point **points, size_t *size
) {
  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();

  size_t len;
  char *err = NULL;
  *points = (hll_sparse_point *)
      rocksdb_get(sparsedb->db, readoptions, set_name, set_name_len, &len, &err);
  rocksdb_readoptions_destroy(readoptions);
  if (err) {
      syslog(LOG_ERR, "failed to fetch sparse points from rocksdb");
      return -1;
  }

  if (len == 0) {
    if (*points) free(*points);
    *points = NULL;
    *size = 0;
    return 0;
  }

  // If the length is 1 this is a dense set
  if (len == 1) {
    free(*points);
    return HLL_IS_DENSE;
  }

  *size = len / sizeof(hll_sparse_point);
  return 0;
}

/**
 * Fetch total size of sparse hyperloglog
 * @arg h The hll to query
 * @return 0 if success
 *         HLL_IS_DENSE if the we should use a dense set
 */
int sparse_size_total(
    struct slidingd_sparsedb *sparsedb,
    const char *set_name, int set_name_len
) {
  hll_sparse_point *points;
  size_t size;

  int err = sparse_get_points(sparsedb, set_name, set_name_len, &points, &size);
  if (err) return err;
  free(points);
  return size;
}
/**
 * Estimates the cardinality of the HLL
 * @arg h The hll to query
 * @return 0 if success
 *         HLL_IS_DENSE if the we should use a dense set
 */
int sparse_size(
    struct slidingd_sparsedb *sparsedb,
    const char *set_name, int set_name_len,
    time_t timestamp, unsigned int time_window
) {
  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();

  size_t len;
  char *err = NULL;
  hll_sparse_point *points = (hll_sparse_point *)
      rocksdb_get(sparsedb->db, readoptions, set_name, set_name_len, &len, &err);
  if (err) {
      syslog(LOG_ERR, "failed to fetch sparse points from rocksdb");
      return -1;
  }

  // If the length is 1 this is a dense set
  if (len == 1) {
    rocksdb_readoptions_destroy(readoptions);
    free(points);
    return HLL_IS_DENSE;
  }

  int size = len / sizeof(hll_sparse_point);
  int count = 0;

  for(int i = 0; i < size; i++) {
      if (points[i].timestamp >= timestamp - (time_t)time_window &&
          points[i].timestamp <= timestamp)
      {
          count++;
      }
  }

  rocksdb_readoptions_destroy(readoptions);
  free(points);

  return count;
}


int sparse_add(
    struct slidingd_sparsedb *sparsedb,
    const char *set_name, int set_name_len,
    uint64_t *hashes, int hash_count,
    time_t timestamp
) {
  char *err = NULL;

  size_t len;
  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  hll_sparse_point *points = (hll_sparse_point *)
      rocksdb_get(sparsedb->db, readoptions, set_name, set_name_len, &len, &err);
  rocksdb_readoptions_destroy(readoptions);
  if (err) {
      syslog(LOG_ERR, "failed to fetch sparse points from rocksdb");
      return -1;
  }

  if (len == 1) {
    free(points);
    return HLL_IS_DENSE;
  }

  int size = len / sizeof(hll_sparse_point);

  // If any hashes are already taken try just update the timestamps
  for (int hash = 0; hash < hash_count; hash++) {
      for(int i = 0; i < size; i++) {
          if (points[i].hash == hashes[hash]) {
              points[i].timestamp = timestamp;

              // Not finished, swap order before reducing
              if (hash + 1 < hash_count) {
                uint64_t temp = hashes[hash_count - 1];
                hashes[hash_count - 1] = hashes[hash];
                hashes[hash] = temp;
              }
              hash_count--;
          }
      }
  }

  // We might still have some hashes to insert at this point
  if (hash_count > 0) {
    size += hash_count;

    // Grow the list of points
    hll_sparse_point *new_points = (hll_sparse_point *)realloc(points, sizeof(hll_sparse_point) * size);
    if (new_points) {
      points = new_points;
    } else {
      free(points);
      syslog(LOG_ERR, "Failed to allocate memory for additional points");
      return -1;
    }

    for (int hash = 0; hash < hash_count; hash++) {
      points[(size - hash_count) + hash].timestamp = timestamp;
      points[(size - hash_count) + hash].hash = hashes[hash];
    }
  }

  rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
  rocksdb_put(
      sparsedb->db, writeoptions,
      set_name, set_name_len,
      (char *)points, sizeof(hll_sparse_point) * size,
      &err
  );
  rocksdb_writeoptions_destroy(writeoptions);

  if (points) free(points);

  if (err) {
    syslog(LOG_ERR, "Sparse rocksdb error: %s", err);
    return -1;
  }

  return size;
}

int sparse_convert_dense(
    struct slidingd_sparsedb *sparsedb,
    const char *set_name, int set_name_len,
    struct hlld_set *set
) {

    size_t len;
    char *err = NULL;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    hll_sparse_point *points = (hll_sparse_point *)
        rocksdb_get(sparsedb->db, readoptions, set_name, set_name_len, &len, &err);
    rocksdb_readoptions_destroy(readoptions);

    if (err) {
        syslog(LOG_ERR, "failed to fetch sparse points from rocksdb");
        return -1;
    }

    // Set was already dense
    if (len == 0) {
      free(points);
      return 0;
    }

    int size = len / sizeof(hll_sparse_point);
    for(int i = 0; i < size; i++) {
        hset_add_hash(set, points[i].hash, points[i].timestamp);
    }
    free(points);

    // Write '-' set (= dense)
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_put(
        sparsedb->db, writeoptions,
        set_name, set_name_len,
        "-", 1,
        &err
    );
    rocksdb_writeoptions_destroy(writeoptions);
    return 0;
}

char *alloc_dense_key(const char *full_key, int full_key_len, int *output_len) {
    *output_len = full_key_len + DENSE_PREFIX_LEN;
    char *buffer = (char *)malloc(sizeof(char) * (*output_len));
    if (!buffer) {
      return NULL;
    }
    memcpy(buffer, DENSE_PREFIX, sizeof(char) * DENSE_PREFIX_LEN); 
    memcpy(buffer + DENSE_PREFIX_LEN, full_key, sizeof(char) * full_key_len); 
    return buffer;
}

int sparse_read_dense_data(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len,
    unsigned char **output, size_t *data_len
) {
    int key_len;
    char *key = alloc_dense_key(full_key, full_key_len, &key_len);
    if (!key) {
        syslog(LOG_ERR, "failed to allocate memory for dense key");
        return -1;
    }

    char *err = NULL;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    *output = (unsigned char *)
        rocksdb_get(sparsedb->db, readoptions, key, key_len, data_len, &err);
    rocksdb_readoptions_destroy(readoptions);

    free(key);

    if (err) {
        syslog(LOG_ERR, "rocksdb dense read fail: %p\n", err);
        return -1;
    }
    if (*data_len == 0) {
        if (*output) {
            free(*output);
            *output = NULL;
        }
    }
    return 0;
}

int sparse_write_dense_data(
    struct slidingd_sparsedb *sparsedb,
    const char *full_key, int full_key_len,
    const unsigned char *data, size_t data_len
) {
    int key_len;
    char *key = alloc_dense_key(full_key, full_key_len, &key_len);
    if (!key) {
        syslog(LOG_ERR, "failed to allocate memory for dense key");
        return -1;
    }

    char *err = NULL;
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_put(sparsedb->db, writeoptions, key, key_len, (char *)data, data_len, &err);
    rocksdb_writeoptions_destroy(writeoptions);

    free(key);
    if (err) {
        syslog(LOG_ERR, "rocksdb dense write fail: %s", err);
        return -1;
    }
    return 0;
}
