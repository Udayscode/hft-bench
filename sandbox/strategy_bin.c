// strategy_bin.c - minimal matching engine stub
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT 8080
#define BUF_SIZE 256

int main() {
    int server_fd, client_fd;
    struct sockaddr_in addr;
    char buf[BUF_SIZE];

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 128);

    printf("[ENGINE] listening on port %d\n", PORT);
    fflush(stdout);

    while (1) {
        client_fd = accept(server_fd, NULL, NULL);
        int n = read(client_fd, buf, BUF_SIZE - 1);
        if (n > 0) {
            buf[n] = '\0';
            // ack back: order_id + accepted
            char resp[64];
            snprintf(resp, sizeof(resp), "{\"status\":\"ack\"}\n");
            write(client_fd, resp, strlen(resp));
        }
        close(client_fd);
    }
}