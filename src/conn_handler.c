#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <regex.h>
#include <assert.h>
#include "hll.h"
#include "conn_handler.h"
#include "convert.h"
#include "handler_constants.c"

/**
 * Defines the number of keys we set/check in a single
 * iteration for our multi commands. We do not do all the
 * keys at one time to prevent a client from holding locks
 * for too long. This is especially critical for set
 * operations which serialize access.
 */
#define MULTI_OP_SIZE 32

/**
 * Invoked in any context with a hlld_conn_handler
 * to send out an INTERNAL_ERROR message to the client.
 */
#define INTERNAL_ERROR() (handle_client_resp(handle->conn, (char*)INTERNAL_ERR, INTERNAL_ERR_LEN))

/* Static method declarations */
static void handle_set_cmd(hlld_conn_handler *handle, char *args, int args_len);
static void handle_set_multi_cmd(hlld_conn_handler *handle, char *args, int args_len);
static void handle_create_cmd(hlld_conn_handler *handle, char *args, int args_len);
static void handle_drop_cmd(hlld_conn_handler *handle, char *args, int args_len);
static void handle_close_cmd(hlld_conn_handler *handle, char *args, int args_len);
static void handle_clear_cmd(hlld_conn_handler *handle, char *args, int args_len);
static void handle_list_cmd(hlld_conn_handler *handle, char *args, int args_len);
static void handle_info_cmd(hlld_conn_handler *handle, char *args, int args_len);
static void handle_flush_cmd(hlld_conn_handler *handle, char *args, int args_len);
static void handle_size_cmd(hlld_conn_handler *handle, char *args, int args_len);


static inline void handle_set_cmd_resp(hlld_conn_handler *handle, int res);
static inline void handle_client_resp(hlld_conn_info *conn, char* resp_mesg, int resp_len);
static void handle_client_err(hlld_conn_info *conn, char* err_msg, int msg_len);

static conn_cmd_type determine_client_command(char *cmd_buf, int buf_len, char **arg_buf, int *arg_len);

static int buffer_after_terminator(char *buf, int buf_len, char terminator, char **after_term, int *after_len);

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
    char *buf, *arg_buf;
    int buf_len, arg_buf_len, should_free;
    int status;
    while (1) {
        status = extract_to_terminator(handle->conn, '\n', &buf, &buf_len, &should_free);
        if (status == -1) break; // Return if no command is available

        // Determine the command type
        conn_cmd_type type = determine_client_command(buf, buf_len, &arg_buf, &arg_buf_len);

        // For now only SET/SIZE are supported.
        if (type != SET_MULTI && type != SIZE) {
          type = UNKNOWN;
        }

        // Handle an error or unknown response
        switch(type) {
            case SET:
                handle_set_cmd(handle, arg_buf, arg_buf_len);
                break;
            case SET_MULTI:
                handle_set_multi_cmd(handle, arg_buf, arg_buf_len);
                break;
            case CREATE:
                handle_create_cmd(handle, arg_buf, arg_buf_len);
                break;
            case DROP:
                handle_drop_cmd(handle, arg_buf, arg_buf_len);
                break;
            case CLOSE:
                handle_close_cmd(handle, arg_buf, arg_buf_len);
                break;
            case CLEAR:
                handle_clear_cmd(handle, arg_buf, arg_buf_len);
                break;
            case LIST:
                handle_list_cmd(handle, arg_buf, arg_buf_len);
                break;
            case INFO:
                handle_info_cmd(handle, arg_buf, arg_buf_len);
                break;
            case FLUSH:
                handle_flush_cmd(handle, arg_buf, arg_buf_len);
                break;
            case SIZE:
                handle_size_cmd(handle, arg_buf, arg_buf_len);
                break;
            default:
                handle_client_err(handle->conn, (char*)&CMD_NOT_SUP, CMD_NOT_SUP_LEN);
                break;
        }

        // Make sure to free the command buffer if we need to
        if (should_free) free(buf);
    }

    return 0;
}

/**
 * Periodic update is used to update our checkpoint with
 * the set manager, so that vacuum progress can be made.
 */
void periodic_update(hlld_conn_handler *handle) {
    setmgr_client_checkpoint(handle->mgr);
}


/**
 * Internal method to handle a command that relies
 * on a set name and a single key, responses are handled using
 * handle_multi_response.
 */
static void handle_set_cmd(hlld_conn_handler *handle, char *args, int args_len) {
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

    // Automatically create the set if it doesn't exist
    if (res == -1 ) {
        setmgr_create_set(handle->mgr, args, NULL);
        res = setmgr_set_keys(handle->mgr, args, (char**)&key_buf, 1, timestamp);
    }

    // Generate the response
    handle_set_cmd_resp(handle, res);
}

/**
 * Internal method to handle a command that returns the estimated size
 * of a set given the sets key and the time window 
 */
static void handle_size_cmd(hlld_conn_handler *handle, char *args, int args_len) {
    // If we have no args, complain.
    if (!args) {
        handle_client_err(handle->conn, (char*)&SET_NEEDED, SET_NEEDED_LEN);
        return;
    }

    char *key = args;

    char *remaining;
    int remaining_len;
    int err;

    //  Fetch the timestamp out
    uint64_t timestamp_64;
    time_t timestamp;

    err = buffer_after_terminator(args, args_len, ' ', &remaining, &remaining_len);
    if (err || remaining_len <= 1) {
        handle_client_err(handle->conn, (char*)&WINDOW_NEEDED, WINDOW_NEEDED_LEN);
        return;
    }

    err = value_to_int64(remaining, &timestamp_64);
    if (err || timestamp_64 <= 0) {
        handle_client_err(handle->conn, (char*)&BAD_ARGS, BAD_ARGS_LEN);
        return;
    }
    timestamp = (time_t) timestamp_64;

    // Fetch the time window
    uint64_t time_window;
    err = buffer_after_terminator(remaining, remaining_len, ' ', &remaining, &remaining_len);
    if (err || remaining_len <= 1) {
        handle_client_err(handle->conn, (char*)&WINDOW_NEEDED, WINDOW_NEEDED_LEN);
        return;
    }

    err = value_to_int64(remaining, &time_window);
    if (err || time_window <= 0) {
        handle_client_err(handle->conn, (char*)&BAD_ARGS, BAD_ARGS_LEN);
        return;
    }

    uint64_t estimate;
    err = setmgr_set_size(handle->mgr, key, &estimate, time_window, timestamp);
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
static void handle_set_multi_cmd(hlld_conn_handler *handle, char *args, int args_len) {
    #define CHECK_ARG_ERR() { \
        handle_client_err(handle->conn, (char*)&SET_KEY_NEEDED, SET_KEY_NEEDED_LEN); \
        return; \
    }
    // If we have no args, complain.
    if (!args) CHECK_ARG_ERR();

    // Setup the buffers
    char *key_buf[MULTI_OP_SIZE];
    char *key;
    int key_len;
    int err;

    // Scan for the timestamp
    err = buffer_after_terminator(args, args_len, ' ', &key, &key_len);
    if (err || key_len <= 1) CHECK_ARG_ERR();

    uint64_t timestamp_64;
    time_t timestamp;
    err = value_to_int64(key, &timestamp_64);
    if (err || timestamp_64 <= 0) {
        handle_client_err(handle->conn, (char*)&BAD_ARGS, BAD_ARGS_LEN);
        return;
    }

    timestamp = (time_t) timestamp_64;

    // Scan all the keys
    err = buffer_after_terminator(key, key_len, ' ', &key, &key_len);
    if (err || key_len <= 1) CHECK_ARG_ERR();

    // Parse any options
    char *curr_key = key;
    int res = 0;
    int index = 0;
    while (curr_key && *curr_key != '\0') {
        // Adds a zero terminator to the current key, scans forward
        buffer_after_terminator(key, key_len, ' ', &key, &key_len);

        // Set the key
        key_buf[index] = curr_key;

        // Advance to the next key
        curr_key = key;
        index++;

        // If we have filled the buffer, check now
        if (index == MULTI_OP_SIZE) {
            // Handle the keys now
            res = setmgr_set_keys(handle->mgr, args, (char**)&key_buf, index, timestamp);

            // Automatically create the set if it doesn't exist
            if (res == -1 ) {
                setmgr_create_set(handle->mgr, args, NULL);
                res = setmgr_set_keys(handle->mgr, args, (char**)&key_buf, index, timestamp);
            }

            if (res) goto SEND_RESULT;

            // Reset the index
            index = 0;
        }
    }

    // Handle any remaining keys
    if (index) {
        res = setmgr_set_keys(handle->mgr, args, key_buf, index, timestamp);
        if (res == -1 ) {
            setmgr_create_set(handle->mgr, args, NULL);
            res = setmgr_set_keys(handle->mgr, args, key_buf, index, timestamp);
        }
    }

SEND_RESULT:
    // Generate the response
    handle_set_cmd_resp(handle, res);
}

/**
 * Internal command used to handle set creation.
 */
static void handle_create_cmd(hlld_conn_handler *handle, char *args, int args_len) {
    // If we have no args, complain.
    if (!args) {
        handle_client_err(handle->conn, (char*)&SET_NEEDED, SET_NEEDED_LEN);
        return;
    }

    // Scan for options after the set name
    char *options;
    int options_len;
    int res = buffer_after_terminator(args, args_len, ' ', &options, &options_len);

    // Verify the set name is valid
    char *set_name = args;
    if (regexec(&VALID_SET_NAMES_RE, set_name, 0, NULL, 0) != 0) {
        handle_client_err(handle->conn, (char*)&BAD_SET_NAME, BAD_SET_NAME_LEN);
        return;
    }

    // Parse the options
    struct hlld_config *config = NULL;
    int err = 0;
    if (res == 0) {
        // Make a new config store, copy the current
        config = (struct hlld_config*)malloc(sizeof(hlld_config));
        memcpy(config, handle->config, sizeof(struct hlld_config));

        // Parse any options
        char *param = options;
        while (param) {
            // Adds a zero terminator to the current param, scans forward
            buffer_after_terminator(options, options_len, ' ', &options, &options_len);

            // Check for the custom params
            int match = 0;
            if (sscanf(param, "precision=%u", &config->default_precision)) {
                // Compute error given precision
                config->default_eps = hll_error_for_precision(config->default_precision);
                match = 1;
            }
            if (sscanf(param, "eps=%lf", &config->default_eps)) {
                // Compute precision given error
                config->default_precision = hll_precision_for_error(config->default_eps);

                // Compute error given precision. This is kinda strange but it is done
                // since its not possible to hit all epsilons perfectly, but we try to get
                // the eps provided to be the upper bound. This value is the actual eps.
                config->default_eps = hll_error_for_precision(config->default_precision);
                match = 1;
            }
            match |= sscanf(param, "in_memory=%d", &config->in_memory);

            // Check if there was no match
            if (!match) {
                err = 1;
                handle_client_err(handle->conn, (char*)&BAD_ARGS, BAD_ARGS_LEN);
                break;
            }

            // Advance to the next param
            param = options;
        }

        // Validate the params
        int invalid_config = 0;
        invalid_config |= sane_default_precision(config->default_precision);
        invalid_config |= sane_default_eps(config->default_eps);
        invalid_config |= sane_in_memory(config->in_memory);

        // Barf if the configs are bad
        if (invalid_config) {
            err = 1;
            handle_client_err(handle->conn, (char*)&BAD_ARGS, BAD_ARGS_LEN);
        }
    }

    // Clean up an leave on errors
    if (err) {
        if (config) free(config);
        return;
    }

    // Create a new set
    res = setmgr_create_set(handle->mgr, set_name, config);
    switch (res) {
        case 0:
            handle_client_resp(handle->conn, (char*)DONE_RESP, DONE_RESP_LEN);
            break;
        case -1:
            handle_client_resp(handle->conn, (char*)EXISTS_RESP, EXISTS_RESP_LEN);
            if (config) free(config);
            break;
        case -3:
            handle_client_resp(handle->conn, (char*)DELETE_IN_PROGRESS, DELETE_IN_PROGRESS_LEN);
            if (config) free(config);
            break;
        default:
            INTERNAL_ERROR();
            if (config) free(config);
            break;
    }
}


/**
 * Internal method to handle a command that relies
 * on a set name and a single key, responses are handled using
 * handle_multi_response.
 */
static void handle_setop_cmd(hlld_conn_handler *handle, char *args, int args_len,
        int(*setmgr_func)(struct hlld_setmgr *, char*)) {
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
}

static void handle_drop_cmd(hlld_conn_handler *handle, char *args, int args_len) {
    handle_setop_cmd(handle, args, args_len, setmgr_drop_set);
}

static void handle_close_cmd(hlld_conn_handler *handle, char *args, int args_len) {
    handle_setop_cmd(handle, args, args_len, setmgr_unmap_set);
}

static void handle_clear_cmd(hlld_conn_handler *handle, char *args, int args_len) {
    handle_setop_cmd(handle, args, args_len, setmgr_clear_set);
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

    res = asprintf(cb_data->output, "%s %f %u %llu %llu\n",
            set_name,
            set->set_config.default_eps,
            set->set_config.default_precision,
            (long long unsigned)hset_byte_size(set),
            (long long unsigned)estimate);
    assert(res != -1);
}

static void handle_list_cmd(hlld_conn_handler *handle, char *args, int args_len) {
    (void)args_len;

    // List all the sets
    struct hlld_set_list_head *head;
    int res = setmgr_list_sets(handle->mgr, args, &head);
    if (res != 0) {
        INTERNAL_ERROR();
        return;
    }

    // Allocate buffers for the responses
    int num_out = (head->size+2);
    char** output_bufs = (char**)malloc(num_out * sizeof(char*));
    int* output_bufs_len = (int*)malloc(num_out * sizeof(int));

    // Setup the START/END lines
    output_bufs[0] = (char*)&START_RESP;
    output_bufs_len[0] = START_RESP_LEN;
    output_bufs[head->size+1] = (char*)&END_RESP;
    output_bufs_len[head->size+1] = END_RESP_LEN;

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
static void info_set_cb(void *data, char *set_name, struct hlld_set *set) {
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
    res = asprintf(cb_data->output, "in_memory %d\n\
page_ins %llu\n\
page_outs %llu\n\
epsilon %f\n\
precision %u\n\
sets %llu\n\
size %llu\n\
storage %llu\n",
    ((hset_is_proxied(set)) ? 0 : 1),
    (unsigned long long)counters->page_ins, (unsigned long long)counters->page_outs,
    set->set_config.default_eps,
    set->set_config.default_precision,
    (unsigned long long)sets,
    (unsigned long long)size,
    (unsigned long long)storage);
    assert(res != -1);
}

static void handle_info_cmd(hlld_conn_handler *handle, char *args, int args_len) {
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

    // Create output buffers
    char *output[] = {(char*)&START_RESP, NULL, (char*)&END_RESP};
    int lens[] = {START_RESP_LEN, 0, END_RESP_LEN};

    // Invoke the callback to get the set stats
    set_cb_data cb_data = {handle->mgr, &output[1]};
    int res = setmgr_set_cb(handle->mgr, args, info_set_cb, &cb_data);

    // Check for no set
    if (res != 0) {
        switch (res) {
            case -1:
                handle_client_resp(handle->conn, (char*)SET_NOT_EXIST, SET_NOT_EXIST_LEN);
                break;
            default:
                INTERNAL_ERROR();
                break;
        }
        return;
    }

    // Adjust the buffer size
    lens[1] = strlen(output[1]);

    // Write out the bufs
    send_client_response(handle->conn, (char**)&output, (int*)&lens, 3);
    free(output[1]);
}


static void handle_flush_cmd(hlld_conn_handler *handle, char *args, int args_len) {
    // If we have a specfic set, use filt_cmd
    if (args) {
        handle_setop_cmd(handle, args, args_len, setmgr_flush_set);
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
        case -1:
            handle_client_resp(handle->conn, (char*)SET_NOT_EXIST, SET_NOT_EXIST_LEN);
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
static conn_cmd_type determine_client_command(char *cmd_buf, int buf_len, char **arg_buf, int *arg_len) {
    // Check if we are ending with \r, and remove it.
    if (cmd_buf[buf_len-2] == '\r') {
        cmd_buf[buf_len-2] = '\0';
        buf_len -= 1;
    }

    // Scan for a space. This will setup the arg_buf and arg_len
    // if we do find the terminator. It will also insert a null terminator
    // at the space, so we can compare the cmd_buf to the commands.
    buffer_after_terminator(cmd_buf, buf_len, ' ', arg_buf, arg_len);

    // Search for the command
    conn_cmd_type type = UNKNOWN;
    #define CMD_MATCH(name) (strcmp(name, cmd_buf) == 0)
    switch (*cmd_buf) {
        case 'b':
            if (CMD_MATCH("b") || CMD_MATCH("bulk"))
                type = SET_MULTI;
            break;

        case 'c':
            if (CMD_MATCH("create")) {
                type = CREATE;
            } else if (CMD_MATCH("close")) {
                type = CLOSE;
            } else if (CMD_MATCH("clear")) {
                type = CLEAR;
            }
            break;

        case 'd':
            if (CMD_MATCH("drop"))
                type = DROP;
            break;

        case 'f':
            if (CMD_MATCH("flush"))
                type = FLUSH;
            break;

        case 'i':
            if (CMD_MATCH("info"))
                type = INFO;

        case 'l':
            if (CMD_MATCH("list"))
                type = LIST;
            break;

        case 's':
            if (CMD_MATCH("s") || CMD_MATCH("set"))
                type = SET;
            else if (CMD_MATCH("size"))
                type = SIZE;
            break;
    }
    return type;
}


/**
 * Scans the input buffer of a given length up to a terminator.
 * Then sets the start of the buffer after the terminator including
 * the length of the after buffer.
 * @arg buf The input buffer
 * @arg buf_len The length of the input buffer
 * @arg terminator The terminator to scan to. Replaced with the null terminator.
 * @arg after_term Output. Set to the byte after the terminator.
 * @arg after_len Output. Set to the length of the output buffer.
 * @return 0 if terminator found. -1 otherwise.
 */
static int buffer_after_terminator(char *buf, int buf_len, char terminator, char **after_term, int *after_len) {
    // Scan for a space
    char *term_addr = (char*)memchr(buf, terminator, buf_len);
    if (!term_addr) {
        *after_term = NULL;
        return -1;
    }

    // Convert the space to a null-seperator
    *term_addr = '\0';

    // Provide the arg buffer, and arg_len
    *after_term = term_addr+1;
    *after_len = buf_len - (term_addr - buf + 1);
    return 0;
}

