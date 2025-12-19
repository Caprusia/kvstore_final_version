#ifndef KVSTORE_H
#define KVSTORE_H

#include "common.h"
#include "hash.h"
#include <pthread.h>



struct user_item {
    //locks per item
    pthread_rwlock_t item_lock;
};

struct user_ht {
   //locks the table itself
    pthread_rwlock_t ht_lock;
};

#endif