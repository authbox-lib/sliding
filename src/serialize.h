#ifndef SERIALIZE_H
#define SERIALIZE_H

#include "stdint.h"
#include "hll.h"

typedef struct {
    unsigned char *memory;
    size_t offset;
    size_t size;
} serialize_t;

int serialize_hll(serialize_t *s, hll_t *h, char *name, int name_len);
int unserialize_hll(serialize_t *s, hll_t *h, char **name, int *name_len);
int serialize_hll_register(serialize_t *s, hll_register *h);
int unserialize_hll_register(serialize_t *s, hll_register *h);

int serialize_int(serialize_t *s, int i);
int unserialize_int(serialize_t *s, int *i);
int serialize_long(serialize_t *s, long i);
int unserialize_long(serialize_t *s, long *i);
int serialize_unsigned_char(serialize_t *s, unsigned char c);
int unserialize_unsigned_char(serialize_t *s, unsigned char *c);
int serialize_time(serialize_t *s, time_t i);
int unserialize_time(serialize_t *s, time_t *i);
int serialize_ulong_long(serialize_t *s, uint64_t i);
int unserialize_ulong_long(serialize_t *s, uint64_t *i);


size_t serialized_hll_size(hll_t *h, int name_len);

int unserialize_hll_from_sparsedb(struct slidingd_sparsedb *sparsedb, hll_t *h, char *full_key, int full_key_len);
int serialize_hll_to_sparsedb(struct slidingd_sparsedb *sparsedb, hll_t *h, char *full_key, int full_key_len);
#endif
