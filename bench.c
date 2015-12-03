#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

static int NUM_THREADS = 1;
static long long NUM_KEYS = 100000000;
static char* HOST = "127.0.0.1";
static int PORT = 4553;
static char *SET_NAME = "foobar%d";

typedef struct {
    int conn_fd;
    char set_name[32];
    char cmd_buf[128];
} conn_info;

int connect_fd(conn_info *info) {
    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = PF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(PF_INET, HOST, &addr.sin_addr);

    info->conn_fd = socket(PF_INET, SOCK_STREAM, 0);
    return connect(info->conn_fd, (struct sockaddr*)&addr, sizeof(addr));
}

int timediff(struct timeval *t1, struct timeval *t2) {
    uint64_t micro1 = t1->tv_sec * 1000000 + t1->tv_usec;
    uint64_t micro2= t2->tv_sec * 1000000 + t2->tv_usec;
    return (micro2-micro1) / 1000;
}

void *thread_main(void *in) {
    printf("Thread started.");
    conn_info info;
    int sets = 0;

    // Generate a set name
    char *buf = (char*)&info.set_name;
    sprintf(buf, SET_NAME, rand());
    printf("Using set: %s\n", buf);

    struct timeval start_connect, start_create, start_set, start_check, end;
    char out_buf[16];
    long long num, sent, len;

    // Connect
    gettimeofday(&start_connect, NULL);
    int res = connect_fd(&info);
    if (res) {
        printf("Failed to connect!");
        return NULL;
    }
    gettimeofday(&end, NULL);
    printf("Connect: %d msec\n", timediff(&start_connect, &end));

    // Make set
    gettimeofday(&start_create, NULL);

    len = sprintf((char*)&info.cmd_buf, "create %s in_memory=0\n", buf);
    send(info.conn_fd, info.cmd_buf, len, 0);
    num = recv(info.conn_fd, (char*)&out_buf, 5, 0);
    if (strcmp(out_buf, "Done\n") != 0) {
        printf("Failed to create set!");
        return NULL;
   }

    gettimeofday(&end, NULL);
    printf("Create: %d msec\n", timediff(&start_create, &end));

    // Set
    gettimeofday(&start_set, NULL);
    for (long long i=0; i< NUM_KEYS; i++) {
        sprintf((char*)&info.cmd_buf, "set %s test%lld \n", buf, i);
        sent = send(info.conn_fd, (char*)&info.cmd_buf, strlen(info.cmd_buf), 0);
        if (sent == -1) {
            printf("Failed to send!");
            return NULL;
        }
    }

    for (long long i=0; i< NUM_KEYS; i++) {
        int remain = 5;
        while (remain) {
            num = recv(info.conn_fd, (char*)out_buf, remain, 0);
            if (num == -1) {
                printf("Failed to read! Iter: %lld. Res: %lld\n", i, num);
                return NULL;
            }
            remain -= num;
        }
        sets++;
    }

    // Get

    gettimeofday(&end, NULL);
    printf("Set: %d msec. Num: %d\n", timediff(&start_set, &end), sets);

    /*sprintf((char*)&info.cmd_buf, "drop %s\n", buf);
    sent = send(info.conn_fd, (char*)&info.cmd_buf, strlen(info.cmd_buf), 0);*/

    return NULL;
}

int main(int argc, char **argv) {
    // Read random seed
    int randfh = open("/dev/random", O_RDONLY);
    unsigned seed = 0;
    read(randfh, &seed, sizeof(seed));
    close(randfh);

    srand(seed);
    pthread_t t[NUM_THREADS];
    for (int i=0; i< NUM_THREADS; i++) {
        pthread_create(&t[i], NULL, thread_main, NULL);
    }
    for (int i=0; i< NUM_THREADS; i++) {
        pthread_join(t[i], NULL);
    }
    return 0;
}
