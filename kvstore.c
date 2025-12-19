#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdbool.h>
#include <errno.h>

#include "server_utils.h"
#include "common.h"
#include "request_dispatcher.h"
#include "hash.h"
#include "kvstore.h"

#define HT_CAPACITY 256

//discards payload when we can not put on a lock. This makes sure we keep the stream in sync
void discard_payload(int socket, size_t len, struct request *request) {
    char *buf = malloc(len);
    size_t to_read = len;
    while (to_read > 0) {
        size_t chunk = (to_read > sizeof(buf)) ? sizeof(buf) : to_read;
        if (read_payload(socket, request, chunk,buf) <= 0) return; 
        to_read -= chunk;
    }
    free(buf);
}

void initialize_ht() {
    ht = malloc(sizeof(hashtable_t));
    ht->capacity = HT_CAPACITY;
    ht->items = calloc(HT_CAPACITY, sizeof(hash_item_t*));
    ht->user = malloc(sizeof(struct user_ht));
    
    //also initialize the lock for the
    pthread_rwlock_init(&ht->user->ht_lock, NULL);
}

//create's new item for set_item
hash_item_t* initialize_item(char *key) {
    //allocate memory for struct and user. And initialize everything in struct
    hash_item_t *item = malloc(sizeof(hash_item_t));
    item->key = strdup(key);
    item->value = NULL;
    item->value_size = 0;
    item->next = NULL;
    item->prev = NULL;
    item->user = malloc(sizeof(struct user_item));
    pthread_rwlock_init(&item->user->item_lock, NULL);
    return item;
}

int set_request(int socket, struct request *request) {
    int bucket = hash(request->key) % HT_CAPACITY;
    hash_item_t *item = NULL;
    bool new_entry = false;

    //reads the lock
    pthread_rwlock_rdlock(&ht->user->ht_lock);
    
    hash_item_t *cursor = ht->items[bucket];
    //loop through bucket to find the item
    while (cursor != NULL) {
        if (strcmp(cursor->key, request->key) == 0) {
            item = cursor;
            break;
        }
        cursor = cursor->next;
    }

    if (item != NULL) {
        //found item
        if (pthread_rwlock_trywrlock(&item->user->item_lock) != 0) {
            //item in use. release the ht lock and abort action
            pthread_rwlock_unlock(&ht->user->ht_lock);
            discard_payload(socket, request->msg_len, request);
            send_response(socket, KEY_ERROR, 0, NULL);
            return -1;
        }
        //item gets locked. release ht lock
        pthread_rwlock_unlock(&ht->user->ht_lock);
    } else {
        //not found, so we have to make a new item. change read lock to write lock
        pthread_rwlock_unlock(&ht->user->ht_lock);
        pthread_rwlock_wrlock(&ht->user->ht_lock);
        
        //check if another thread did not insert item during lock changes
        cursor = ht->items[bucket];
        while (cursor != NULL) {
            if (strcmp(cursor->key, request->key) == 0) {
                item = cursor;
                break;
            }
            cursor = cursor->next;
        }

        if (item != NULL) {
            //we found item so it has been placed during lock change. creates race condition
            if (pthread_rwlock_trywrlock(&item->user->item_lock) != 0) {
                //could not lock item, abort
                pthread_rwlock_unlock(&ht->user->ht_lock);
                discard_payload(socket, request->msg_len, request);
                send_response(socket, KEY_ERROR, 0, NULL);
                return -1;
            }
            //won the race and sets lock
            pthread_rwlock_unlock(&ht->user->ht_lock);
        } else {
            //was not inserted during lock change. make item and put in bucket
            item = initialize_item(request->key);
            new_entry = true;
            pthread_rwlock_wrlock(&item->user->item_lock);

            //add to bucket
            item->next = ht->items[bucket];
            if (ht->items[bucket]) ht->items[bucket]->prev = item;
            ht->items[bucket] = item;
            
            //we do release the ht lock so other threads can use the table
            pthread_rwlock_unlock(&ht->user->ht_lock);
        }
    }


    
    char *buffer = malloc(request->msg_len);
    if (buffer == NULL && request->msg_len > 0) {
        if (new_entry) {
             pthread_rwlock_wrlock(&ht->user->ht_lock);
             if (item->next != NULL) {
                item->next->prev = item->prev;
             }
             if (item->prev != NULL) {
                item->prev->next = item->next;
             }
             if (ht->items[bucket] == item) {
                ht->items[bucket] = item->next;
             }
             pthread_rwlock_unlock(&ht->user->ht_lock);
             pthread_rwlock_unlock(&item->user->item_lock);
             free(item->key); 
             free(item->user); 
             free(item);
        } else {
             pthread_rwlock_unlock(&item->user->item_lock);
        }
        send_response(socket, STORE_ERROR, 0, NULL);
        return -1;
    }

    if (read_payload(socket, request, request->msg_len, buffer) < 0) {
        free(buffer);
        
        if (new_entry) {
             pthread_rwlock_wrlock(&ht->user->ht_lock);
             pthread_rwlock_wrlock(&ht->user->ht_lock);
             if (item->next != NULL) {
                item->next->prev = item->prev;
             }
             if (item->prev != NULL) {
                item->prev->next = item->next;
             }
             if (ht->items[bucket] == item) {
                ht->items[bucket] = item->next;
             }
             pthread_rwlock_unlock(&ht->user->ht_lock);

             pthread_rwlock_unlock(&item->user->item_lock);
             pthread_rwlock_destroy(&item->user->item_lock);
             free(item->key); free(item->user); free(item);
        } else {
             pthread_rwlock_unlock(&item->user->item_lock);
        }
        return -1;
    }

    if (check_payload(socket, request, request->msg_len) < 0) {
        free(buffer);
        // Clean up logic (omitted for brevity, same as above)
        if (new_entry) {
            pthread_rwlock_wrlock(&ht->user->ht_lock);
            if (item->next){
            item->next->prev = item->prev;
            }
            if (item->prev){
            item->prev->next = item->next;
            }
            if (ht->items[bucket] == item){
                ht->items[bucket] = item->next;
            }
            pthread_rwlock_unlock(&ht->user->ht_lock);
            
            pthread_rwlock_unlock(&item->user->item_lock);
            pthread_rwlock_destroy(&item->user->item_lock);
            free(item->key); 
            free(item->user); 
            free(item);
        } else {
             pthread_rwlock_unlock(&item->user->item_lock);
        }
        return -1;
    }

    // Update Value
    if (item->value) {
        free(item->value);
    }
    item->value = buffer;
    item->value_size = request->msg_len;

    pthread_rwlock_unlock(&item->user->item_lock);
    send_response(socket, OK, 0, NULL);
    return 0;
}

int get_request(int socket, struct request *request) {
    int bucket = hash(request->key) % HT_CAPACITY;
    hash_item_t *item = NULL;

    //put ht to readlock
    pthread_rwlock_rdlock(&ht->user->ht_lock);
    
    hash_item_t *cursor = ht->items[bucket];
    while (cursor != NULL) {
        if (strcmp(cursor->key, request->key) == 0) {
            item = cursor;
            break;
        }
        cursor = cursor->next;
    }

    if (item == NULL) {
        pthread_rwlock_unlock(&ht->user->ht_lock);
        send_response(socket, KEY_ERROR, 0, NULL);
        return -1;
    }


    if (pthread_rwlock_tryrdlock(&item->user->item_lock) != 0) {
        //couldn't lock item, abort
        pthread_rwlock_unlock(&ht->user->ht_lock);
        send_response(socket, KEY_ERROR, 0, NULL);
        return -1;
    }

    //unlock ht
    pthread_rwlock_unlock(&ht->user->ht_lock);
    send_response(socket, OK, item->value_size, item->value);
    pthread_rwlock_unlock(&item->user->item_lock);
    return 0;
}

int del_request(int socket, struct request *request) {
    int bucket = hash(request->key) % HT_CAPACITY;
    hash_item_t *item = NULL;

    //lock write on ht
    pthread_rwlock_wrlock(&ht->user->ht_lock);
    
    hash_item_t *cursor = ht->items[bucket];
    while (cursor != NULL) {
        if (strcmp(cursor->key, request->key) == 0) {
            item = cursor;
            break;
        }
        cursor = cursor->next;
    }

    if (item == NULL) {
        pthread_rwlock_unlock(&ht->user->ht_lock);
        send_response(socket, KEY_ERROR, 0, NULL);
        return -1;
    }

    if (pthread_rwlock_trywrlock(&item->user->item_lock) != 0) {
        //couldn't lock item
        pthread_rwlock_unlock(&ht->user->ht_lock);
        send_response(socket, KEY_ERROR, 0, NULL);
        return -1;
    }

    //take item out of the bucket
    if (item->prev){
        item->prev->next = item->next;
    }
    else{
        ht->items[bucket] = item->next;
    }
    if (item->next) item->next->prev = item->prev;

    pthread_rwlock_unlock(&ht->user->ht_lock);
    pthread_rwlock_unlock(&item->user->item_lock);
    pthread_rwlock_destroy(&item->user->item_lock);
    
    if (item->value){
        free(item->value);
    }
    free(item->key);
    free(item->user);
    free(item);

    send_response(socket, OK, 0, NULL);
    return 0;
}

void *main_job(void *arg) {
    struct conn_info *conn_info = (struct conn_info *)arg;
    struct request *request = allocate_request();
    request->connection_close = 0;
    int method;

    do {
        method = recv_request(conn_info->socket_fd, request);
        switch (method) {
            case SET: 
                set_request(conn_info->socket_fd, request); 
                break;
            case GET: 
                get_request(conn_info->socket_fd, request); 
                break;
            case DEL: 
                del_request(conn_info->socket_fd, request); 
                break;
            case RST: 
                send_response(conn_info->socket_fd, OK, 0, NULL); 
                break;
        }
        if (request->key) {
            free(request->key);
            request->key = NULL;
        }
    } while (!request->connection_close);

    close_connection(conn_info->socket_fd);
    free(request);
    free(conn_info);
    return NULL;
}

int main(int argc, char *argv[]) {
    int listen_sock = server_init(argc, argv);
    initialize_ht();

    for (;;) {
        struct conn_info *conn_info = calloc(1, sizeof(struct conn_info));
        if (accept_new_connection(listen_sock, conn_info) < 0) {
            free(conn_info);
            continue;
        }

        pthread_t tid;
        if (pthread_create(&tid, NULL, main_job, conn_info) != 0) {
            perror("pthread_create");
            close_connection(conn_info->socket_fd);
            free(conn_info);
            continue;
        }
        pthread_detach(tid);
    }
    return 0;
}