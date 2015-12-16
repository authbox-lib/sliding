#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>

#include "hll.h"
#include "serialize.h"

#include "config.h"
#include "sparse.h"

START_TEST(test_sparse_init_destroy)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    slidingd_sparsedb *sparsedb;
    res = init_sparse(&config, &sparsedb);
    fail_unless(res == 0);

    res = destroy_sparse(sparsedb);
    fail_unless(res == 0);
}
END_TEST
START_TEST(test_sparse_insert) {
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    slidingd_sparsedb *sparsedb;
    res = init_sparse(&config, &sparsedb);
    fail_unless(res == 0);


    uint64_t hashes[] = {123, 456, 789};
    fail_unless(sparse_add(
        sparsedb, "abc", 3,
        hashes, 1,
        10
    ) == 1);

    // Make sure it's visible from within the time window
    fail_unless(sparse_size(sparsedb, "abc", 3, 15, 5) == 1);
    fail_unless(sparse_size(sparsedb, "abc", 3, 20, 5) == 0);
    fail_unless(sparse_size(sparsedb, "abc", 3, 20, 10) == 1);

    // Bump up the time window and make sure it's visible
    fail_unless(sparse_add(
        sparsedb, "abc", 3,
        hashes, 1,
        15
    ) == 1);

    fail_unless(sparse_size(sparsedb, "abc", 3, 20, 5) == 1);

    // Add the other two values a bit later
    fail_unless(sparse_add(
        sparsedb, "abc", 3,
        hashes + 1, 2,
        20
    ) == 3);

    fail_unless(sparse_size(sparsedb, "abc", 3, 25, 5) == 2);
    fail_unless(sparse_size(sparsedb, "abc", 3, 25, 10) == 3);

    // Cleanup
    res = destroy_sparse(sparsedb);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_sparse_convert) {
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    slidingd_sparsedb *sparsedb;
    res = init_sparse(&config, &sparsedb);
    fail_unless(res == 0);

    // Need full hashes here so that they don't collide
    uint64_t hashes[] = {
      519865238786025774,
      308106866941458606,
      2187992749178668892,
    };
    fail_unless(sparse_add(
        sparsedb, "def", 3,
        hashes, 3,
        10
    ) == 3);
    fail_unless(sparse_add(
        sparsedb, "def", 3,
        hashes, 1,
        30
    ) == 3);

    fail_unless(sparse_size(sparsedb, "def", 3, 30, 10) == 1);
    fail_unless(sparse_size(sparsedb, "def", 3, 30, 30) == 3);

    hll_t h;
    fail_unless(hll_init(12, 100, 1, &h) == 0);
    fail_unless(sparse_convert_dense(
        sparsedb, "def", 3,
        &h
    ) == 0);

    fail_unless(sparse_size(sparsedb, "def", 3, 30, 30) == HLL_IS_DENSE);

    double est_f = hll_size_total(&h);
    fail_unless(est_f > 2.99 && est_f < 3.01);

    fail_unless(hll_destroy(&h) == 0);

    res = destroy_sparse(sparsedb);
    fail_unless(res == 0);
}
END_TEST
