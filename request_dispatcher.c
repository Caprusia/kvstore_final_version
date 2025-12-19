#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <assert.h>
#include <string.h>

#include "hash.h"
#include "request_dispatcher.h"
#include "parser.h"
#include "server_utils.h"

int ping(int socket)
{
    return send_response(socket, OK, 0, NULL);
}

/*
 * Thread unsafe
 */
int dump(const char *filename, int socket)
{
    assert(ht != NULL);

    int fd;
    if ((fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666)) == -1) {
        char errbuf[1024];
        snprintf(errbuf, sizeof(errbuf), "Could not open %s for creating dump",
                 filename);
        error("%s\n", errbuf);
        send_response(socket, UNK_ERROR, strlen(errbuf), errbuf);
        return -1;
    }

    for (unsigned bucket = 0; bucket < ht->capacity; bucket++) {
        dprintf(fd, "B %d\n", bucket);
        hash_item_t *curr = ht->items[bucket];
        while (curr != NULL) {
            dprintf(fd, "K %s %zu\n", curr->key, curr->value_size);
            if (write(fd, curr->value, curr->value_size) < 0) {
                char errbuf[1024];
                snprintf(errbuf, sizeof(errbuf),
                         "Could not dump value of size %zu for key %s",
                         curr->value_size, curr->key);
                error("%s\n", errbuf);
                send_response(socket, UNK_ERROR, strlen(errbuf), errbuf);
                break;
            }
            if (write(fd, "\n", 1) < 0) {
                error("Could not write newline to dump\n");
                send_response(socket, UNK_ERROR, 0, NULL);
                break;
            }
            curr = curr->next;
        }
    }
    close(fd);
    return send_response(socket, OK, 0, NULL);
}

int setopt_request(int socket, struct request *request)
{
    if (!strcmp(request->key, "SNDBUF")) {
        char respbuf[256];
        int sndbuf = 0;
        socklen_t optlen = sizeof(sndbuf);

        if (setsockopt(socket, SOL_SOCKET, SO_SNDBUF, &sndbuf,
                   sizeof(sndbuf)) < 0) {
            perror("setsockopt SNDBUF");
            return send_response(socket, SETOPT_ERROR, 0, NULL);
        }
        if (getsockopt(socket, SOL_SOCKET, SO_SNDBUF, &sndbuf, &optlen)) {
            perror("getsockopt SNDBUF");
            return send_response(socket, SETOPT_ERROR, 0, NULL);
        }
        snprintf(respbuf, sizeof(respbuf), "%d", sndbuf);
        return send_response(socket, OK, strlen(respbuf), respbuf);
    } else {
        return send_response(socket, KEY_ERROR, 0, NULL);
    }
}

void request_dispatcher(int socket, struct request *request)
{
    pr_info("Method: %s\n", method_to_str(request->method));
    if (request->key) {
        pr_info("Key: %s [%zu]\n", request->key, request->key_len);
    }

    switch (request->method) {
    case PING:
        ping(socket);
        break;
    case DUMP:
        dump(DUMP_FILE, socket);
        break;
    case EXIT:
        send_response(socket, OK, 0, NULL);
        exit(0);
        break;
    case SETOPT:
        setopt_request(socket, request);
        break;
    case UNK:
        send_response(socket, PARSING_ERROR, 0, NULL);
        break;
    default:
        return;
    }
}
