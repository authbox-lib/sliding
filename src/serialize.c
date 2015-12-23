#include <assert.h>
#include <string.h>
#include <syslog.h>
#include <unistd.h>
#include <memory.h>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include "hll.h"
#include "serialize.h"
#include "sparse.h"

#define SERIAL_VERSION 2
#define ERR(err) if (err == -1) { return -1; }

// After the start of serialization the hll might fgrow a little more, tack on
// some extra space in the buffer to handle this.
#define SERIALIZE_BUFFER_EXTRA 256

int serialize_int(serialize_t *s, int i) {
    if (s->offset + sizeof(int) >= s->size)
        return -1;
    *(int*)(s->memory+s->offset) = i;
    s->offset += sizeof(int);
    return 0;
}

int unserialize_int(serialize_t *s, int *i) {
    if (s->offset + sizeof(int) >= s->size)
        return -1;
    *i = *(int*)(s->memory+s->offset);
    s->offset += sizeof(int);
    return 0;
}

int serialize_long(serialize_t *s, long i) {
    if (s->offset + sizeof(long) >= s->size)
        return -1;
    *(long*)(s->memory+s->offset) = i;
    s->offset += sizeof(long);
    return 0;
}

int serialize_time(serialize_t *s, time_t t) {
    assert(sizeof(time_t) == 8);
    unsigned char *time_char = (unsigned char*)&t;
    for(int i=0; i<8; i++)
        ERR(serialize_unsigned_char(s, time_char[i]));
    return 0;
}

int unserialize_time(serialize_t *s, time_t *time) {
    unsigned char t2[8];
    unsigned char *t = t2;
    for(int i=0; i<8; i++)
        ERR(unserialize_unsigned_char(s, &t[i]));
    *time = *((time_t*)t);
    return 0;
}

int serialize_ulong_long(serialize_t *s, uint64_t i) {
    unsigned char *cchar = (unsigned char*)&i;
    for(int j=0; j<8; j++)
        ERR(serialize_unsigned_char(s, cchar[j]));
    return 0;
}

int unserialize_ulong_long(serialize_t *s, uint64_t *i) {
    unsigned char t2[8];
    unsigned char *t = t2;
    for(int j=0; j<8; j++)
        ERR(unserialize_unsigned_char(s, &t[j]));
    *i = *((uint64_t*)t);
    return 0;
}

 int unserialize_long(serialize_t *s, long *i) {
    if (s->offset + sizeof(long) > s->size)
        return -1;
    *i = *(long*)(s->memory+s->offset);
    s->offset += sizeof(long);
    return 0;
}

int serialize_unsigned_char(serialize_t *s, unsigned char c) {
    if (s->offset + sizeof(char) > s->size)
        return -1;
    s->memory[s->offset] = c;
    s->offset += sizeof(char);
    return 0;
}

int unserialize_unsigned_char(serialize_t *s, unsigned char *c) {
    if (s->offset + sizeof(char) > s->size)
        return -1;
    *c = s->memory[s->offset];
    s->offset += sizeof(char);
    return 0;
}

int serialize_string(serialize_t *s, char *str, int len) {
    if (len < 0) {
        return -1;
    }
    ERR(serialize_int(s, len));
    if (s->offset + sizeof(char) * len >= s->size)
        return -1;

    memcpy(s->memory + s->offset, str, sizeof(char) * len);
    s->offset += sizeof(char) * len;
    return 0;
}
int unserialize_string(serialize_t *s, char **out_str, int *out_len) {
    int len;
    ERR(unserialize_int(s, &len));

    if (len < 0)
        return -1;
    if (s->offset + sizeof(char) * len > s->size)
        return -1;

    *out_len = len;
    *out_str = (char *)malloc(sizeof(char) * len);
    if (!*out_str)
      return -1;

    memcpy(*out_str, s->memory + s->offset, sizeof(char) * len);
    s->offset += sizeof(char) * len;

    return 0;
}


int serialize_hll_register(serialize_t *s, hll_register *h) {
    ERR(serialize_long(s, h->size));
    for (long i=0; i<h->size; i++) {
        ERR(serialize_time(s, h->points[i].timestamp));
        ERR(serialize_long(s, h->points[i].register_));
    }
    return 0;
}

int unserialize_hll_register(serialize_t *s, hll_register *h) {
    ERR(unserialize_long(s, &h->size));
    h->capacity = h->size;
    if (h->capacity == 0) {
        h->points = NULL;
    } else {
        h->points = (hll_dense_point*)malloc(h->capacity*sizeof(hll_dense_point));
    }
    for (long i=0; i<h->size; i++) {
        ERR(unserialize_time(s, &h->points[i].timestamp));
        ERR(unserialize_long(s, &h->points[i].register_));
    }
    return 0;
}

int serialize_hll(serialize_t *s, hll_t *h) {
    ERR(serialize_int(s, SERIAL_VERSION));
    ERR(serialize_int(s, (int)h->precision));
    ERR(serialize_int(s, h->window_period));
    ERR(serialize_int(s, h->window_precision));
    int num_regs = NUM_REG(h->precision);
    for(int i=0; i<num_regs; i++) {
        ERR(serialize_hll_register(s, &h->dense_registers[i]));
    }
    return 0;
}

int unserialize_hll(serialize_t *s, hll_t *h) {
    int version;
    ERR(unserialize_int(s, &version));
    if (version != SERIAL_VERSION) {
        return -1;
    }

    int temp;
    ERR(unserialize_int(s, &temp));
    h->precision = (unsigned char)temp;
    ERR(unserialize_int(s, &h->window_period));
    ERR(unserialize_int(s, &h->window_precision));
    int num_regs = NUM_REG(h->precision);
    h->dense_registers = (hll_register*)malloc(num_regs*sizeof(hll_register));
    for(int i=0; i<num_regs; i++) {
        ERR(unserialize_hll_register(s, &h->dense_registers[i]));
    }
    return 0;
}

/***
 * @return 0 on success
 *        -1 on failure
 *        -2 on set missing from sparsedb
 */
int unserialize_hll_from_sparsedb(
    struct slidingd_sparsedb *sparsedb, hll_t *h, char *full_key, int full_key_len
) {
    size_t len;
    unsigned char *buffer;
    
    int res = sparse_read_dense_data(sparsedb, full_key, full_key_len, &buffer, &len);
    if (res) {
        syslog(LOG_ERR, "failed to read data from sparsedb");
        return -1;
    }

    if (len == 0) {
        if (buffer) free(buffer);
        return -2;
    }

    serialize_t s = {buffer, 0, len};
    res = unserialize_hll(&s, h);
    if (res != 0) {
        perror("Failed to unserialize hll");
    }

    return res;
}

size_t serialized_hll_size(hll_t *h) {
    // VERSION
    size_t size = sizeof(int);

    // precision, window_period, window_precision
    size += sizeof(int) * 3;

    for(int i=0; i<NUM_REG(h->precision); i++) {
        // size, size*(timestamp, register)
        size += sizeof(long) + (sizeof(long)+sizeof(time_t))*h->dense_registers[i].size; 
    }
    return size;
}

int serialize_hll_to_sparsedb(
    struct slidingd_sparsedb *sparsedb, hll_t *h, char *full_key, int full_key_len
  ) {
    size_t serialized_size = serialized_hll_size(h);
    size_t max_size = serialized_size + SERIALIZE_BUFFER_EXTRA;

    unsigned char* addr = (unsigned char*)malloc(sizeof(char)*max_size);
    serialize_t s = {addr, 0, max_size};
    int res = serialize_hll(&s, h);
    if (res == -1) {
        syslog(LOG_ERR, "unable to serialize hl");
        return -1;
    }

    // Grab the serialized size from the data we actually wrote
    serialized_size = s.offset;

    res = sparse_write_dense_data(
        sparsedb, full_key, full_key_len,
        addr, serialized_size
    );
    free(addr);

    return res;
}
