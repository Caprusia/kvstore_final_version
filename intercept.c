#define _GNU_SOURCE
#include <stdlib.h>
#include <errno.h>
#include <dlfcn.h>
#include <sys/mman.h>
#include <pthread.h>
#include <string.h>

#include <stdio.h>
#include <unistd.h>

#define LINE_MAX 64
#define LOG_FILENAME "/tmp/kvstore.log"

#define error(fmt, ...) \
do { \
    fprintf(stderr, fmt "\n", ##__VA_ARGS__); \
    exit(1); \
} while (0)

#define assert(cond, fmt, ...) \
do { \
    if (!(cond)) \
        error("Assertion \"%s\" at %s:%d failed: " fmt, #cond, __FILE__, \
              __LINE__, ##__VA_ARGS__); \
} while (0)

int orig_pthread_mutex_lock(pthread_mutex_t *);
int orig_pthread_mutex_unlock(pthread_mutex_t *);
int orig_pthread_cond_wait(pthread_cond_t *restrict, pthread_mutex_t *restrict);
int orig_pthread_cond_signal(pthread_cond_t *);
int orig_pthread_cond_broadcast(pthread_cond_t *);



struct data_t {
    char* s; // NULL-terminated
    struct data_t *next;
    struct data_t *prev;
};

struct data_queue_t {
    struct data_t *front, *rear;
    pthread_mutex_t lock;
    pthread_cond_t is_not_empty;
    int size;
};

struct data_queue_t *data_queue;
pthread_t log_tid;
FILE *logfile;

void log_queue_init() {
    int ret;
    data_queue = (struct data_queue_t*)malloc(sizeof(struct data_queue_t));
    data_queue->front = NULL;
    data_queue->rear = NULL;
    ret = pthread_mutex_init(&data_queue->lock, NULL);
    assert(ret == 0, "Could not initialize log file mutex: %d\n", ret);
    ret = pthread_cond_init(&data_queue->is_not_empty, NULL);
    assert(ret == 0, "Could not initialize log file mutex: %d\n", ret);
    data_queue->size = 0;
}

void put(struct data_queue_t *data_queue, char* line) {
    struct data_t *data = malloc(sizeof(struct data_t));
    data->s = line;
    data->prev = NULL;
    data->next = NULL;
    orig_pthread_mutex_lock(&data_queue->lock);
    if (data_queue->rear == NULL) {
        assert(data_queue->size == 0, "Queue error. size:%d", data_queue->size);
        data_queue->rear = data;
        data_queue->front = data;
    } else {
        data_queue->rear->prev = data;
        data->next = data_queue->rear;
        data_queue->rear = data;
    }
    data_queue->size++;
    orig_pthread_cond_signal(&data_queue->is_not_empty);
    orig_pthread_mutex_unlock(&data_queue->lock);
}

struct data_t *get(struct data_queue_t *data_queue) {
    struct data_t *curr;

    orig_pthread_mutex_lock(&data_queue->lock);
    while(data_queue->size == 0) {
        int ret = orig_pthread_cond_wait(&data_queue->is_not_empty, &data_queue->lock);
        assert(ret==0, "conditional wait error: %d\n", ret);
    }

    curr = data_queue->front;
    assert(curr != NULL, "Queue front is null!\n");
    if (curr->prev == NULL) {// curr was the last one
        data_queue->front = NULL;
        data_queue->rear = NULL;
    } else {
        data_queue->front = curr->prev;
        curr->next = NULL;
    }

    data_queue->size--;
    orig_pthread_mutex_unlock(&data_queue->lock);
    return curr;
}

void *log_thread(void* arg) {
    arg = arg; // avoid warning
    struct data_t *data;
    while ((data = get(data_queue))) {
       assert(data, "Log thread error\n");
       char *towrite = data->s;
       size_t remaining = strlen(data->s);
       while (remaining!=0) {
           int ret = fwrite(towrite, 1, remaining, logfile);
           assert(ret > 0, "Could not write to log file\n");
           remaining -= ret;
           towrite += ret;
       }
       fflush(logfile);
       free(data->s);
       free(data);
    }

    return (void*)NULL;
}

void spawn_log_thread() {
    int ret;
    log_queue_init();

    logfile = fopen(LOG_FILENAME, "w+");
    ret = pthread_create(&log_tid, NULL, log_thread, NULL);
    assert(!ret, "Could not spawn log thread: %d\n", ret);
}

int orig_pthread_mutex_lock(pthread_mutex_t *mutex) {
    static int (*_orig_pthread_mutex_lock)(pthread_mutex_t *);
    int ret;
    if (!_orig_pthread_mutex_lock)
        _orig_pthread_mutex_lock = dlsym(RTLD_NEXT, "pthread_mutex_lock");
    ret = _orig_pthread_mutex_lock(mutex);
    return ret;
}

int orig_pthread_mutex_unlock(pthread_mutex_t *mutex) {
    static int (*_orig_pthread_mutex_unlock)(pthread_mutex_t *);
    int ret;
    if (!_orig_pthread_mutex_unlock)
        _orig_pthread_mutex_unlock = dlsym(RTLD_NEXT, "pthread_mutex_unlock");
    ret = _orig_pthread_mutex_unlock(mutex);
    return ret;
}

int pthread_mutex_lock(pthread_mutex_t *mutex) {
    char *buf = (char*)malloc(LINE_MAX *sizeof(char));
    sprintf(buf, "lock:%p\n", mutex);
    put(data_queue, buf);
    return orig_pthread_mutex_lock(mutex);
}

int pthread_mutex_trylock(pthread_mutex_t *mutex) {
    static int (*_orig_pthread_mutex_trylock)(pthread_mutex_t *);
    int ret;
    if (!_orig_pthread_mutex_trylock)
        _orig_pthread_mutex_trylock = dlsym(RTLD_NEXT, "pthread_mutex_trylock");
    ret = _orig_pthread_mutex_trylock(mutex);
    if (!ret) {
        char *buf = (char*)malloc(LINE_MAX *sizeof(char));
        sprintf(buf, "lock:%p\n", mutex);
        put(data_queue, buf);
    }
    return ret;
}

int pthread_mutex_unlock(pthread_mutex_t *mutex) {
    char *buf = (char*)malloc(LINE_MAX *sizeof(char));
    sprintf(buf, "unlock:%p\n", mutex);
    put(data_queue, buf);
    return orig_pthread_mutex_unlock(mutex);
}

int pthread_rwlock_rdlock(pthread_rwlock_t *rwlock) {
    static int (*_orig_pthread_rwlock_rdlock)(pthread_rwlock_t *);
    int ret;
    if (!_orig_pthread_rwlock_rdlock)
        _orig_pthread_rwlock_rdlock = dlsym(RTLD_NEXT, "pthread_rwlock_rdlock");
    ret =_orig_pthread_rwlock_rdlock(rwlock);
    if (!ret) {
        char *buf = (char*)malloc(LINE_MAX *sizeof(char));
        sprintf(buf, "rdlock:%p\n", rwlock);
        put(data_queue, buf);
    }
    return ret;
}

int pthread_rwlock_tryrdlock(pthread_rwlock_t *rwlock) {
    static int (*_orig_pthread_rwlock_tryrdlock)(pthread_rwlock_t *);
    int ret;
    if (!_orig_pthread_rwlock_tryrdlock)
        _orig_pthread_rwlock_tryrdlock = dlsym(RTLD_NEXT, "pthread_rwlock_tryrdlock");
    ret =_orig_pthread_rwlock_tryrdlock(rwlock);
    if (!ret) {
        char *buf = (char*)malloc(LINE_MAX *sizeof(char));
        sprintf(buf, "rdlock:%p\n", rwlock);
        put(data_queue, buf);
    }
    return ret;
}

int pthread_rwlock_wrlock(pthread_rwlock_t *rwlock) {
    static int (*_orig_pthread_rwlock_wrlock)(pthread_rwlock_t *);
    int ret;
    if (!_orig_pthread_rwlock_wrlock)
        _orig_pthread_rwlock_wrlock = dlsym(RTLD_NEXT, "pthread_rwlock_wrlock");
    ret =_orig_pthread_rwlock_wrlock(rwlock);
    if (!ret) {
        char *buf = (char*)malloc(LINE_MAX *sizeof(char));
        sprintf(buf, "wrlock:%p\n", rwlock);
        put(data_queue, buf);
    }
    return ret;
}

int pthread_rwlock_trywrlock(pthread_rwlock_t *rwlock) {
    static int (*_orig_pthread_rwlock_trywrlock)(pthread_rwlock_t *);
    int ret;
    if (!_orig_pthread_rwlock_trywrlock)
        _orig_pthread_rwlock_trywrlock = dlsym(RTLD_NEXT, "pthread_rwlock_trywrlock");
    ret =_orig_pthread_rwlock_trywrlock(rwlock);
    if (!ret) {
        char *buf = (char*)malloc(LINE_MAX *sizeof(char));
        sprintf(buf, "wrlock:%p\n", rwlock);
        put(data_queue, buf);
    }
    return ret;
}

int pthread_rwlock_unlock(pthread_rwlock_t *rwlock) {
    static int (*_orig_pthread_rwlock_unlock)(pthread_rwlock_t *);
    int ret;
    if (!_orig_pthread_rwlock_unlock)
        _orig_pthread_rwlock_unlock = dlsym(RTLD_NEXT, "pthread_rwlock_unlock");
    ret =_orig_pthread_rwlock_unlock(rwlock);
    char *buf = (char*)malloc(LINE_MAX *sizeof(char));
    sprintf(buf, "rwunlock:%p\n", rwlock);
    put(data_queue, buf);
    return ret;
}

int orig_pthread_cond_wait(pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex) {
    static int (*_orig_pthread_cond_wait)(pthread_cond_t *restrict, pthread_mutex_t *restrict);
    int ret;
    if (!_orig_pthread_cond_wait)
        _orig_pthread_cond_wait = dlsym(RTLD_NEXT, "pthread_cond_wait");
    ret =_orig_pthread_cond_wait(cond, mutex);
    return ret;
 }

int pthread_cond_wait(pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex) {
    char *buf = (char*)malloc(LINE_MAX *sizeof(char));
    sprintf(buf, "pthread_cond_wait:%p\n", mutex);
    put(data_queue, buf);
    return orig_pthread_cond_wait(cond, mutex);
 }

int orig_pthread_cond_signal(pthread_cond_t *cond) {
    static int (*_orig_pthread_cond_signal)(pthread_cond_t *restrict);
    int ret;
    if (!_orig_pthread_cond_signal)
        _orig_pthread_cond_signal = dlsym(RTLD_NEXT, "pthread_cond_signal");
    ret =_orig_pthread_cond_signal(cond);
    return ret;
}

int pthread_cond_signal(pthread_cond_t *cond) {
    char *buf = (char*)malloc(LINE_MAX *sizeof(char));
    sprintf(buf, "pthread_cond_signal:%p\n", cond);
    put(data_queue, buf);
    return orig_pthread_cond_signal(cond);
}

int orig_pthread_cond_broadcast(pthread_cond_t *cond) {
    static int (*_orig_pthread_cond_broadcast)(pthread_cond_t *restrict);
    int ret;
    if (!_orig_pthread_cond_broadcast)
        _orig_pthread_cond_broadcast = dlsym(RTLD_NEXT, "pthread_cond_broadcast");
    ret =_orig_pthread_cond_broadcast(cond);
    return ret;
}

int pthread_cond_broadcast(pthread_cond_t *cond) {
    char *buf = (char*)malloc(LINE_MAX *sizeof(char));
    sprintf(buf, "pthread_cond_broadcast:%p\n", cond);
    put(data_queue, buf);
    return orig_pthread_cond_broadcast(cond);
}
