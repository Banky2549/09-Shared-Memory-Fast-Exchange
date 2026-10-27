#include "shared_memory_lab.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <unistd.h>

int create_segment(void) {
    return shmget(IPC_PRIVATE, sizeof(struct shared_block), 0600 | IPC_CREAT);
}

void cleanup_segment(int segment_id, struct shared_block *block) {
    if (shmdt(block) == -1) {
        perror("shmdt");
    }
    if (shmctl(segment_id, IPC_RMID, NULL) == -1) {
        perror("shmctl");
    }
}

/* volatile: force a real read from shared memory on every iteration */
static void wait_for_flag(const volatile int *flag) {
    while (!*flag) {
        usleep(1000);
    }
}

static void wait_for_ready(const struct shared_block *block) {
    wait_for_flag(&block->ready);
}

/* The fixed message[16] in the header cannot hold "shared memory is fast"
 * (21 bytes + NUL). Task A therefore uses its own larger segment layout.
 * `ready` stays at offset 0, matching struct shared_block. */
struct fast_block {
    int ready;
    char message[32];
};

/* Copies text into block->message, truncating to fit. Returns the length the
 * full text would need; using snprintf's return value also makes the
 * truncation explicit, so -Wformat-truncation stays quiet under -Werror. */
static size_t write_message(struct shared_block *block, const char *text) {
    int needed = snprintf(block->message, sizeof(block->message), "%s", text);
    return needed < 0 ? 0 : (size_t)needed;
}

int task_fast_exchange(void) {
    int segment_id;
    pid_t child_pid;
    int status;
    struct fast_block *block;

    segment_id = shmget(IPC_PRIVATE, sizeof(struct fast_block), 0600 | IPC_CREAT);
    if (segment_id == -1) {
        perror("shmget");
        return 1;
    }

    block = shmat(segment_id, NULL, 0);
    if (block == (void *)-1) {
        perror("shmat");
        return 1;
    }
    memset(block, 0, sizeof(*block));

    fflush(stdout);
    child_pid = fork();
    if (child_pid < 0) {
        perror("fork");
        cleanup_segment(segment_id, (struct shared_block *)block);
        return 1;
    }

    if (child_pid == 0) {
        wait_for_flag(&block->ready);
        printf("child fast read: %s\n", block->message);
        fflush(stdout);
        if (shmdt(block) == -1) {
            perror("shmdt");
            _exit(1);
        }
        _exit(0); /* must exit here; returning would re-run main in the child */
    }

    snprintf(block->message, sizeof(block->message), "%s", "shared memory is fast");
    block->ready = 1;

    if (waitpid(child_pid, &status, 0) == -1) {
        perror("waitpid");
        cleanup_segment(segment_id, (struct shared_block *)block);
        return 1;
    }

    printf("parent fast write: done\n");
    fflush(stdout);

    cleanup_segment(segment_id, (struct shared_block *)block);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}

int task_latest_value_limit(void) {
    int segment_id;
    pid_t child_pid;
    int status;
    struct shared_block *block;

    segment_id = create_segment();
    if (segment_id == -1) {
        perror("shmget");
        return 1;
    }

    block = shmat(segment_id, NULL, 0);
    if (block == (void *)-1) {
        perror("shmat");
        return 1;
    }
    memset(block, 0, sizeof(*block));

    fflush(stdout);
    child_pid = fork();
    if (child_pid < 0) {
        perror("fork");
        cleanup_segment(segment_id, block);
        return 1;
    }

    if (child_pid == 0) {
        wait_for_ready(block);
        printf("child latest view: %s\n", block->message);
        fflush(stdout);
        if (shmdt(block) == -1) {
            perror("shmdt");
            _exit(1);
        }
        _exit(0);
    }

    write_message(block, "first update");
    write_message(block, "latest-only");
    block->ready = 1;

    if (waitpid(child_pid, &status, 0) == -1) {
        perror("waitpid");
        cleanup_segment(segment_id, block);
        return 1;
    }

    if (strcmp(block->message, "latest-only") == 0) {
        printf("limit observed: no history in shared memory\n");
        fflush(stdout);
    }

    cleanup_segment(segment_id, block);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}

int task_capacity_limit(void) {
    int segment_id;
    pid_t child_pid;
    int status;
    struct shared_block *block;
    const char *source = "abcdefghijklmnopqrstuvwxyz";

    segment_id = create_segment();
    if (segment_id == -1) {
        perror("shmget");
        return 1;
    }

    block = shmat(segment_id, NULL, 0);
    if (block == (void *)-1) {
        perror("shmat");
        return 1;
    }
    memset(block, 0, sizeof(*block));

    fflush(stdout);
    child_pid = fork();
    if (child_pid < 0) {
        perror("fork");
        cleanup_segment(segment_id, block);
        return 1;
    }

    if (child_pid == 0) {
        wait_for_ready(block);
        printf("child capacity bytes: %zu\n", strlen(block->message));
        fflush(stdout);
        if (shmdt(block) == -1) {
            perror("shmdt");
            _exit(1);
        }
        _exit(0);
    }

    write_message(block, source);
    block->ready = 1;

    if (waitpid(child_pid, &status, 0) == -1) {
        perror("waitpid");
        cleanup_segment(segment_id, block);
        return 1;
    }

    printf("parent source bytes: %zu\n", strlen(source));
    fflush(stdout);

    cleanup_segment(segment_id, block);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}

int main(void) {
    if (task_fast_exchange() != 0) {
        return 1;
    }

    if (task_latest_value_limit() != 0) {
        return 1;
    }

    if (task_capacity_limit() != 0) {
        return 1;
    }

    printf("all tasks done\n");
    return 0;
}