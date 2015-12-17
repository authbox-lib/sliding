#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <regex.h>
#include <assert.h>
#include <ctype.h>
#include "hll.h"
#include "conn_handler.h"
#include "convert.h"
#include "handler_constants.c"

/**
 * Maximum number of arguments in a single command
 */
#define MAX_ARGS 256

/**
 * Invoked in any context with a hlld_conn_handler
 * to send out an INTERNAL_ERROR message to the client.
 */
#define INTERNAL_ERROR() (handle_client_resp(handle->conn, (char*)INTERNAL_ERR, INTERNAL_ERR_LEN))

/**
 * Invoked to easily return with a bad argument error
 */
#define BAD_ARG_ERR() { \
    handle_client_err(handle->conn, (char*)&BAD_ARGS, BAD_ARGS_LEN); \
    return; \
}

/* Static method declarations */
static void handle_echo_cmd(hlld_conn_handler *handle, char **args, int *args_len, int arg_count);
static void handle_set_cmd(hlld_conn_handler *handle, char **args, int *args_len, int arg_count);
static void handle_set_multi_cmd(hlld_conn_handler *handle, char **args, int *args_len, int arg_count);
static void handle_drop_cmd(hlld_conn_handler *handle, char **args, int *args_len, int arg_count);
static void handle_close_cmd(hlld_conn_handler *handle, char **args, int *args_len, int arg_count);
static void handle_clear_cmd(hlld_conn_handler *handle, char **args, int *args_len, int arg_count);
static void handle_list_cmd(hlld_conn_handler *handle, char **args, int *args_len, int arg_count);
static void handle_detail_cmd(hlld_conn_handler *handle, char **args, int *args_len, int arg_count);
static void handle_flush_cmd(hlld_conn_handler *handle, char **args, int *args_len, int arg_count);
static void handle_size_cmd(hlld_conn_handler *handle, char **args, int *args_len, int arg_count);

static void handle_info_cmd(hlld_conn_handler *handle, int arg_count);

static inline void handle_set_cmd_resp(hlld_conn_handler *handle, int res);
static inline void handle_client_resp(hlld_conn_info *conn, char* resp_mesg, int resp_len);
static inline void handle_string_resp(hlld_conn_info *conn, char* resp_mesg, int resp_len);
static void handle_client_err(hlld_conn_info *conn, char* err_msg, int msg_len);

static conn_cmd_type determine_client_command(char *cmd);

// Simple struct to hold data for a callback
typedef struct {
    struct hlld_setmgr *mgr;
    char **output;
} set_cb_data;

/**
 * Invoked to initialize the conn handler layer.
 */
void init_conn_handler() {
    // Compile our regexes
    int res;
    res = regcomp(&VALID_SET_NAMES_RE, VALID_SET_NAMES_PATTERN, REG_EXTENDED|REG_NOSUB);
    assert(res == 0);
}

/**
 * Invoked by the networking layer when there is new
 * data to be handled. The connection handler should
 * consume all the input possible, and generate responses
 * to all requests.
 * @arg handle The connection related information
 * @return 0 on success.
 */
int handle_client_connect(hlld_conn_handler *handle) {
    // Look for the next command line
    int arg_count, free_arg;

    char *args[MAX_ARGS];
    int args_len[MAX_ARGS];

    int status;
    while (1) {
        status = extract_command(handle->conn, args, args_len, MAX_ARGS, &arg_count, &free_arg);
        if (status == EXTRACT_NO_DATA) {
          return 0;
        } else if (status < 0) {
          return -1;
        }

        // Determine the command type
        conn_cmd_type type;
        if (arg_count > 0) {
            type = determine_client_command(args[0]);
        } else {
            type = UNKNOWN;
        }

        // Handle an error or unknown response
        switch(type) {
            case ECHO:
                handle_echo_cmd(handle, args + 1, args_len + 1, arg_count - 1);
                break;
            case SET:
                handle_set_cmd(handle, args + 1, args_len + 1, arg_count - 1);
                break;
            case SET_MULTI:
                handle_set_multi_cmd(handle, args + 1, args_len + 1, arg_count - 1);
                break;
            case DROP:
                handle_drop_cmd(handle, args + 1, args_len + 1, arg_count - 1);
                break;
            case CLOSE:
                handle_close_cmd(handle, args + 1, args_len + 1, arg_count - 1);
                break;
            case CLEAR:
                handle_clear_cmd(handle, args + 1, args_len + 1, arg_count - 1);
                break;
            case LIST:
                handle_list_cmd(handle, args + 1, args_len + 1, arg_count - 1);
                break;
            case DETAIL:
                handle_detail_cmd(handle, args + 1, args_len + 1, arg_count - 1);
                break;
            case INFO:
                handle_info_cmd(handle, arg_count - 1);
                break;
            case FLUSH:
                handle_flush_cmd(handle, args + 1, args_len + 1, arg_count - 1);
                break;
            case SIZE:
                handle_size_cmd(handle, args + 1, args_len + 1, arg_count - 1);
                break;
            default:
                handle_client_err(handle->conn, (char*)&CMD_NOT_SUP, CMD_NOT_SUP_LEN);
                break;
        }

        // Make sure to free the command buffer if we need to
        if (free_arg >= 0) {
          free(args[free_arg]);
        }
    }
}

/**
 * Periodic update is used to update our checkpoint with
 * the set manager, so that vacuum progress can be made.
 */
void periodic_update(hlld_conn_handler *handle) {
    setmgr_client_checkpoint(handle->mgr);
}


static void handle_echo_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count) {
  if (args_count != 1 || args_len[0] < 1) {
    BAD_ARG_ERR();
  }

  char length_string[512];
  int length_length = snprintf(length_string, 512, "$%d\r\n", args_len[0]);
  if (length_length == -1) {
    INTERNAL_ERROR();
    return;
  }

  char *buffers[] = {length_string, args[0], (char *)"\r\n"};
  int sizes[] = {length_length, args_len[0], 2};
  send_client_response(handle->conn, (char**)&buffers, (int*)&sizes, 3);
}

/**
 * Internal method to handle a command that relies
 * on a set name and a single key, responses are handled using
 * handle_multi_response.
 */
static void handle_set_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count) {
  if (handle && args && args_len && args_count) {
    return;
  }
  /*
    #define CHECK_ARG_ERR() { \
        handle_client_err(handle->conn, (char*)&SET_KEY_NEEDED, SET_KEY_NEEDED_LEN); \
        return; \
    }
    // If we have no args, complain.
    if (!args) CHECK_ARG_ERR();

    // Scan past the set name
    char *key;
    int key_len;
    int err = buffer_after_terminator(args, args_len, ' ', &key, &key_len);
    if (err || key_len <= 1) CHECK_ARG_ERR();

    // @TODO Read timestamp from input
    time_t timestamp = time(NULL);

    // Setup the buffers
    char *key_buf[] = {key};

    // Call into the set manager
    int res = setmgr_set_keys(handle->mgr, args, (char**)&key_buf, 1, timestamp);

    // Generate the response
    handle_set_cmd_resp(handle, res);
  */
}

/**
 * Internal method to handle a command that returns the estimated size
 * of a set given the sets key and the time window 
 */
static void handle_size_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count) {
    int err;

    // If we have no args, complain.
    if (args_count != 3) BAD_ARG_ERR();
    if (args_len[0] < 1) BAD_ARG_ERR();
    if (args_len[1] < 1) BAD_ARG_ERR();
    if (args_len[2] < 1) BAD_ARG_ERR();

    // Interpret the timestamp
    uint64_t timestamp_64;
    time_t timestamp;
    err = value_to_int64(args[1], &timestamp_64);
    if (err || timestamp_64 <= 0) BAD_ARG_ERR();
    timestamp = (time_t) timestamp_64;

    // Fetch the time window
    uint64_t time_window;
    err = value_to_int64(args[2], &time_window);
    if (err || time_window <= 0) BAD_ARG_ERR();

    // Build up the estimate and return it
    uint64_t estimate;
    err = setmgr_set_size(handle->mgr, args[0], &estimate, timestamp,  time_window);
    if (err) {
      INTERNAL_ERROR();
      return;
    }

    char estimate_string[512];
    int estimate_length = snprintf(estimate_string, 512, ":%lld\r\n", (long long)estimate);
    if (estimate_length == -1) {
      INTERNAL_ERROR();
      return;
    }

    handle_client_resp(handle->conn, estimate_string, estimate_length);
}


/**
 * Internal method to handle a command that relies
 * on a set name and multiple keys, responses are handled using
 * handle_multi_response.
 */
static void handle_set_multi_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count) {
    #define CHECK_ARG_ERR() { \
        handle_client_err(handle->conn, (char*)&SET_KEY_NEEDED, SET_KEY_NEEDED_LEN); \
        return; \
    }

    int err;

    // Extract the set name
    if (args_count < 3) CHECK_ARG_ERR();
    if (args_len[0] < 1) CHECK_ARG_ERR();
    if (args_len[1] < 1) CHECK_ARG_ERR();

    // Interpret the timestamp
    uint64_t timestamp_64;
    time_t timestamp;
    err = value_to_int64(args[1], &timestamp_64);
    if (err || timestamp_64 <= 0) {
        handle_client_err(handle->conn, (char*)&BAD_ARGS, BAD_ARGS_LEN);
        return;
    }
    timestamp = (time_t) timestamp_64;


    // Scan all the keys
    int res = 0;
    int index = 0;
    char *key_buf[MULTI_OP_SIZE];

    for (int arg = 2; arg < args_count; arg++) {
        if (err || args_len[arg] < 1) CHECK_ARG_ERR();

        // Set the key
        key_buf[index++] = args[arg];

        // If we have filled the buffer, check now
        if (index == MULTI_OP_SIZE) {
            // Handle the keys now
            res = setmgr_set_keys(handle->mgr, args[0], (char**)&key_buf, index, timestamp);
            if (res) goto SEND_RESULT;

            // Reset the index
            index = 0;
        }
    }

    // Handle any remaining keys
    if (index) {
        res = setmgr_set_keys(handle->mgr, args[0], key_buf, index, timestamp);
    }

SEND_RESULT:
    // Generate the response
    handle_set_cmd_resp(handle, res);
}


/**
 * Internal method to handle a command that relies
 * on a set name and a single key, responses are handled using
 * handle_multi_response.
 */
static void handle_setop_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count,
        int(*setmgr_func)(struct hlld_setmgr *, char*)) {
  if (setmgr_func && handle && args && args_len && args_count) {
    return;
  }
  return;
  /*
    // If we have no args, complain.
    if (!args) {
        handle_client_err(handle->conn, (char*)&SET_NEEDED, SET_NEEDED_LEN);
        return;
    }

    // Scan past the set name
    char *key;
    int key_len;
    int after = buffer_after_terminator(args, args_len, ' ', &key, &key_len);
    if (after == 0) {
        handle_client_err(handle->conn, (char*)&UNEXPECTED_ARGS, UNEXPECTED_ARGS_LEN);
        return;
    }

    // Call into the set manager
    int res = setmgr_func(handle->mgr, args);
    handle_set_cmd_resp(handle, res);
  */
}

static void handle_drop_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count) {
    handle_setop_cmd(handle, args, args_len, args_count, setmgr_drop_set);
}

static void handle_close_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count) {
    handle_setop_cmd(handle, args, args_len, args_count, setmgr_unmap_set);
}

static void handle_clear_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count) {
    handle_setop_cmd(handle, args, args_len, args_count, setmgr_clear_set);
}

// Callback invoked by list command to create an output
// line for each set. We hold a set handle which we
// can use to get some info about it
static void list_set_cb(void *data, char *set_name, struct hlld_set *set) {
    set_cb_data *cb_data = (set_cb_data *)data;
    int res;

    // Use the last flush size, attempt to get the latest size.
    // We do this in-case a list is at the same time as a unmap/delete.
    uint64_t estimate = set->set_config.size;
    setmgr_set_size_total(cb_data->mgr, set_name, &estimate);

    res = asprintf(cb_data->output, "+%s %f %u %llu %llu\r\n",
            set_name,
            set->set_config.default_eps,
            set->set_config.default_precision,
            (long long unsigned)hset_byte_size(set),
            (long long unsigned)estimate);
    assert(res != -1);
}

static void handle_list_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count) {

    const char *prefix;
    if (args_count == 0) {
        prefix = "";
    } else if (args_count == 1) {
        prefix = args[0];
    } else {
        BAD_ARG_ERR();
    }

    (void)args_len;

    // List all the sets
    struct hlld_set_list_head *head;
    int res = setmgr_list_sets(handle->mgr, prefix, &head);
    if (res != 0) {
        INTERNAL_ERROR();
        return;
    }

    // Allocate buffers for the responses
    int num_out = (head->size+1);
    char** output_bufs = (char**)malloc(num_out * sizeof(char*));
    int* output_bufs_len = (int*)malloc(num_out * sizeof(int));

    char count_string[512];
    int count_length = snprintf(count_string, 512, "*%d\r\n", head->size);
    if (count_length < 1) {
      INTERNAL_ERROR();
      return;
    }

    // Setup the START/END lines
    output_bufs[0] = count_string;
    output_bufs_len[0] = count_length;

    // Generate the responses
    char *resp;
    struct hlld_set_list *node = head->head;
    set_cb_data cb_data = {handle->mgr, &resp};
    for (int i=0; i < head->size; i++) {
        res = setmgr_set_cb(handle->mgr, node->set_name, list_set_cb, &cb_data);
        if (res == 0) {
            output_bufs[i+1] = resp;
            output_bufs_len[i+1] = strlen(resp);
        } else { // Skip this output
            output_bufs[i+1] = NULL;
            output_bufs_len[i+1] = 0;
        }
        node = node->next;
    }

    // Write the response
    send_client_response(handle->conn, output_bufs, output_bufs_len, num_out);

    // Cleanup
    for (int i=1; i <= head->size; i++) if(output_bufs[i]) free(output_bufs[i]);
    free(output_bufs);
    free(output_bufs_len);
    setmgr_cleanup_list(head);
}


// Callback invoked by list command to create an output
// line for each set. We hold a set handle which we
// can use to get some info about it
static void detail_set_cb(void *data, char *set_name, struct hlld_set *set) {
    (void)set_name;
    set_cb_data *cb_data = (set_cb_data *)data;

    // Use the last flush size, attempt to get the latest size.
    // We do this in-case a list is at the same time as a unmap/delete.
    uint64_t size = set->set_config.size;
    setmgr_set_size_total(cb_data->mgr, set_name, &size);

    // Get some metrics
    set_counters *counters = hset_counters(set);
    uint64_t storage = hset_byte_size(set);
    uint64_t sets = counters->sets;

    // Generate a formatted string output
    int res;
    res = asprintf(cb_data->output, "in_memory:%d\n\
page_ins:%llu\n\
page_outs:%llu\n\
epsilon:%f\n\
precision:%u\n\
sets:%llu\n\
size:%llu\n\
storage:%llu\n",
    ((hset_is_proxied(set)) ? 0 : 1),
    (unsigned long long)counters->page_ins, (unsigned long long)counters->page_outs,
    set->set_config.default_eps,
    set->set_config.default_precision,
    (unsigned long long)sets,
    (unsigned long long)size,
    (unsigned long long)storage);
    assert(res != -1);
}

static void handle_detail_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count) {
    // If we have no args, complain.
    if (args_count != 1 || args_len[0] < 1) {
        handle_client_err(handle->conn, (char*)&SET_NEEDED, SET_NEEDED_LEN);
        return;
    }

    char *info;
    int info_len;
    set_cb_data cb_data = {handle->mgr, &info};
    int res = setmgr_set_cb(handle->mgr, args[0], detail_set_cb, &cb_data);
    if (res != 0) {
        INTERNAL_ERROR();
        return;
    }

    info_len = strlen(info);

    handle_string_resp(handle->conn, info, info_len);

    free(info);
}

static void handle_info_cmd(hlld_conn_handler *handle, int args_count) {
    if (args_count != 0) {
        BAD_ARG_ERR();
    }

    const char *string = "role:master\r\n";
    handle_string_resp(handle->conn, (char *)string, strlen(string));
}


static void handle_flush_cmd(hlld_conn_handler *handle, char **args, int *args_len, int args_count) {
  if (handle && args && args_len && args_count) {
    return;
  }
  return;
  /*
    // If we have a specfic set, use filt_cmd
    if (args) {
        handle_setop_cmd(handle, args, args_len, args_count, setmgr_flush_set);
        return;
    }

    // List all the sets
    struct hlld_set_list_head *head;
    int res = setmgr_list_sets(handle->mgr, NULL, &head);
    if (res != 0) {
        INTERNAL_ERROR();
        return;
    }

    // Flush all, ignore errors since
    // sets might get deleted in the process
    struct hlld_set_list *node = head->head;
    while (node) {
        setmgr_flush_set(handle->mgr, node->set_name);
        node = node->next;
    }

    // Respond
    handle_client_resp(handle->conn, (char*)DONE_RESP, DONE_RESP_LEN);

    // Cleanup
    setmgr_cleanup_list(head);
  */
}


/**
 * Sends a client response message back for a simple set command
 * Simple convenience wrapper around handle_client_resp.
 */
static inline void handle_set_cmd_resp(hlld_conn_handler *handle, int res) {
    switch (res) {
        case 0:
            handle_client_resp(handle->conn, (char*)DONE_RESP, DONE_RESP_LEN);
            break;
        case -2:
            handle_client_resp(handle->conn, (char*)SET_NOT_PROXIED, SET_NOT_PROXIED_LEN);
            break;
        default:
            INTERNAL_ERROR();
            break;
    }
}


/**
 * Sends a client response message back. Simple convenience wrapper
 * around send_client_resp.
 */
static inline void handle_client_resp(hlld_conn_info *conn, char* resp_mesg, int resp_len) {
    char *buffers[] = {resp_mesg};
    int sizes[] = {resp_len};
    send_client_response(conn, (char**)&buffers, (int*)&sizes, 1);
}

/**
 * Sends a string response message back. Simple convenience wrapper
 * around send_client_resp.
 */
static inline void handle_string_resp(hlld_conn_info *conn, char* resp_mesg, int resp_len) {
    char *start_resp;
    int res = asprintf(&start_resp, "$%d\r\n", resp_len);
    assert(res > 0);

    // Create output buffers
    char *output[] = {(char*)start_resp, resp_mesg, (char *) "\r\n"};
    int lens[] = {(int) strlen(start_resp), resp_len, 2};

    // Write out the bufs
    send_client_response(conn, (char**)&output, (int*)&lens, 3);
    free(start_resp);
}


/**
 * Sends a client error message back. Optimizes to use multiple
 * output buffers so we can collapse this into a single write without
 * needing to move our buffers around.
 */
static void handle_client_err(hlld_conn_info *conn, char* err_msg, int msg_len) {
    char *buffers[] = {(char*)&CLIENT_ERR, err_msg, (char*)&NEW_LINE};
    int sizes[] = {CLIENT_ERR_LEN, msg_len, NEW_LINE_LEN};
    send_client_response(conn, (char**)&buffers, (int*)&sizes, 3);
}


/**
 * Determines the client command.
 * @arg cmd_buf A command buffer
 * @arg buf_len The length of the buffer
 * @arg arg_buf Output. Sets the start address of the command arguments.
 * @arg arg_len Output. Sets the length of arg_buf.
 * @return The conn_cmd_type enum value.
 * UNKNOWN if it doesn't match anything supported, or a proper command.
 */
static conn_cmd_type determine_client_command(char *cmd) {
    // Search for the command
    conn_cmd_type type = UNKNOWN;
    #define CMD_MATCH(name) (strcasecmp(name, cmd) == 0)
    switch (*cmd) {
        case 'd': case 'D':
            if (CMD_MATCH("detail"))
                type = DETAIL;
        case 'e': case 'E':
            if (CMD_MATCH("echo"))
                type = ECHO;
            break;
        case 'i': case 'I':
            if (CMD_MATCH("info"))
                type = INFO;
        case 'l': case 'L':
            if (CMD_MATCH("list"))
                type = LIST;
        case 's': case 'S':
            if (CMD_MATCH("shadd"))
                type = SET_MULTI;
            else if (CMD_MATCH("shcard"))
                type = SIZE;
            break;
    }
    return type;
}
