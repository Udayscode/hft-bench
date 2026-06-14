// strategy_bin.c - production-grade low-latency AF_VSOCK matching engine
//
// Architecture:
//   - Binds to AF_VSOCK port 8080 (accessible from host via Firecracker VSOCK proxy)
//   - epoll-driven event loop: handles persistent connections on a single thread
//   - Performs full price-time priority Limit Order Book (LOB) matching
//   - Returns exact trade execution JSON matches to align 100% with shadow-engine

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <linux/vm_sockets.h>

// ── Configuration ─────────────────────────────────────────────────────────────
#define VSOCK_PORT            8080
#define MAX_EVENTS            64
#define MAX_CLIENTS           256
#define CLIENT_BUF_SIZE       4096
#define MAX_ORDERS            131072
#define MAX_TRADES_PER_ORDER  128

// ── Limit Order Book structures ───────────────────────────────────────────────
typedef struct {
    unsigned long long id;
    unsigned long long price;
    unsigned long long qty;
    int side; // 0 for Buy, 1 for Sell
    int active;
} BookOrder;

typedef struct {
    unsigned long long maker_order_id;
    unsigned long long taker_order_id;
    unsigned long long price;
    unsigned int quantity;
} MatchedTrade;

// ── LOB state ────────────────────────────────────────────────────────────────
static BookOrder order_index[MAX_ORDERS];
static unsigned long long active_bids[MAX_ORDERS];
static int num_bids = 0;
static unsigned long long active_asks[MAX_ORDERS];
static int num_asks = 0;
static unsigned long long message_count = 0;
static unsigned long long max_order_id_processed = 0;

static void reset_order_book(void) {
    memset(order_index, 0, sizeof(order_index));
    memset(active_bids, 0, sizeof(active_bids));
    memset(active_asks, 0, sizeof(active_asks));
    num_bids = 0;
    num_asks = 0;
    message_count = 0;
    max_order_id_processed = 0;
}

static void remove_bid(unsigned long long id) {
    int idx = -1;
    for (int i = 0; i < num_bids; i++) {
        if (active_bids[i] == id) {
            idx = i;
            break;
        }
    }
    if (idx != -1) {
        for (int i = idx; i < num_bids - 1; i++) {
            active_bids[i] = active_bids[i + 1];
        }
        num_bids--;
    }
}

static void remove_ask(unsigned long long id) {
    int idx = -1;
    for (int i = 0; i < num_asks; i++) {
        if (active_asks[i] == id) {
            idx = i;
            break;
        }
    }
    if (idx != -1) {
        for (int i = idx; i < num_asks - 1; i++) {
            active_asks[i] = active_asks[i + 1];
        }
        num_asks--;
    }
}

// ── Matching Logic ───────────────────────────────────────────────────────────
static void match_buy_order(
    unsigned long long id,
    unsigned long long price,
    unsigned long long qty,
    int is_market,
    MatchedTrade *trades,
    int *num_trades
) {
    unsigned long long remaining_qty = qty;

    while (remaining_qty > 0) {
        // Find best ask: minimum price, then oldest (lowest ID)
        int best_idx = -1;
        for (int k = 0; k < num_asks; k++) {
            unsigned long long aid = active_asks[k];
            if (order_index[aid].active) {
                if (best_idx == -1 ||
                    order_index[aid].price < order_index[best_idx].price ||
                    (order_index[aid].price == order_index[best_idx].price && order_index[aid].id < order_index[best_idx].id)) {
                    best_idx = aid;
                }
            }
        }

        if (best_idx == -1) break; // No asks to match against
        if (!is_market && price < order_index[best_idx].price) break; // Spread doesn't cross

        // Determine match quantity
        unsigned long long match_qty = (remaining_qty < order_index[best_idx].qty) ? remaining_qty : order_index[best_idx].qty;

        if (*num_trades < MAX_TRADES_PER_ORDER) {
            trades[*num_trades].maker_order_id = order_index[best_idx].id;
            trades[*num_trades].taker_order_id = id;
            trades[*num_trades].price = order_index[best_idx].price;
            trades[*num_trades].quantity = (unsigned int)match_qty;
            (*num_trades)++;
        }

        remaining_qty -= match_qty;
        order_index[best_idx].qty -= match_qty;

        if (order_index[best_idx].qty == 0) {
            order_index[best_idx].active = 0;
            remove_ask(best_idx);
        }
    }

    // Insert remaining quantity to bids if it is a Limit order
    if (!is_market && remaining_qty > 0 && id < MAX_ORDERS) {
        order_index[id].id = id;
        order_index[id].price = price;
        order_index[id].qty = remaining_qty;
        order_index[id].side = 0; // Buy
        order_index[id].active = 1;
        active_bids[num_bids++] = id;
    }
}

static void match_sell_order(
    unsigned long long id,
    unsigned long long price,
    unsigned long long qty,
    int is_market,
    MatchedTrade *trades,
    int *num_trades
) {
    unsigned long long remaining_qty = qty;

    while (remaining_qty > 0) {
        // Find best bid: maximum price, then oldest (lowest ID)
        int best_idx = -1;
        for (int k = 0; k < num_bids; k++) {
            unsigned long long bid = active_bids[k];
            if (order_index[bid].active) {
                if (best_idx == -1 ||
                    order_index[bid].price > order_index[best_idx].price ||
                    (order_index[bid].price == order_index[best_idx].price && order_index[bid].id < order_index[best_idx].id)) {
                    best_idx = bid;
                }
            }
        }

        if (best_idx == -1) break; // No bids to match against
        if (!is_market && price > order_index[best_idx].price) break; // Spread doesn't cross

        // Determine match quantity
        unsigned long long match_qty = (remaining_qty < order_index[best_idx].qty) ? remaining_qty : order_index[best_idx].qty;

        if (*num_trades < MAX_TRADES_PER_ORDER) {
            trades[*num_trades].maker_order_id = order_index[best_idx].id;
            trades[*num_trades].taker_order_id = id;
            trades[*num_trades].price = order_index[best_idx].price;
            trades[*num_trades].quantity = (unsigned int)match_qty;
            (*num_trades)++;
        }

        remaining_qty -= match_qty;
        order_index[best_idx].qty -= match_qty;

        if (order_index[best_idx].qty == 0) {
            order_index[best_idx].active = 0;
            remove_bid(best_idx);
        }
    }

    // Insert remaining quantity to asks if it is a Limit order
    if (!is_market && remaining_qty > 0 && id < MAX_ORDERS) {
        order_index[id].id = id;
        order_index[id].price = price;
        order_index[id].qty = remaining_qty;
        order_index[id].side = 1; // Sell
        order_index[id].active = 1;
        active_asks[num_asks++] = id;
    }
}

static void cancel_order(unsigned long long target_id) {
    if (target_id < MAX_ORDERS && order_index[target_id].active) {
        order_index[target_id].active = 0;
        if (order_index[target_id].side == 0) {
            remove_bid(target_id);
        } else {
            remove_ask(target_id);
        }
    }
}

// ── Per-client state ───────────────────────────────────────────────────────────
typedef struct {
    int  fd;
    char buf[CLIENT_BUF_SIZE];
    int  buf_len;
} Client;

static Client clients[MAX_CLIENTS];

static int find_client_slot(void) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].fd == -1) return i;
    }
    return -1;
}

static int find_client_by_fd(int fd) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].fd == fd) return i;
    }
    return -1;
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// ── Main ───────────────────────────────────────────────────────────────────────
int main(void) {
    // Initialise client table and reset book state
    for (int i = 0; i < MAX_CLIENTS; i++) {
        clients[i].fd     = -1;
        clients[i].buf_len = 0;
    }
    reset_order_book();

    // Create TCP listener on port 8080
    int tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_fd < 0) { perror("socket(AF_INET)"); return 1; }

    int opt = 1;
    setsockopt(tcp_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in tcp_addr = {0};
    tcp_addr.sin_family      = AF_INET;
    tcp_addr.sin_addr.s_addr = INADDR_ANY;
    tcp_addr.sin_port        = htons(VSOCK_PORT);

    if (bind(tcp_fd, (struct sockaddr *)&tcp_addr, sizeof(tcp_addr)) < 0) {
        perror("bind(TCP)"); close(tcp_fd); return 1;
    }

    if (listen(tcp_fd, 128) < 0) { perror("listen TCP"); return 1; }
    if (set_nonblocking(tcp_fd) < 0) { perror("fcntl(TCP)"); return 1; }

    // Create VSOCK listener on port 8080
    int vsock_fd = socket(AF_VSOCK, SOCK_STREAM, 0);
    if (vsock_fd >= 0) {
        struct sockaddr_vm vsock_addr = {0};
        vsock_addr.svm_family = AF_VSOCK;
        vsock_addr.svm_cid = VMADDR_CID_ANY;
        vsock_addr.svm_port = VSOCK_PORT;

        if (bind(vsock_fd, (struct sockaddr *)&vsock_addr, sizeof(vsock_addr)) < 0) {
            perror("bind(VSOCK)"); close(vsock_fd); vsock_fd = -1;
        } else {
            if (listen(vsock_fd, 128) < 0) { perror("listen VSOCK"); close(vsock_fd); vsock_fd = -1; }
            else if (set_nonblocking(vsock_fd) < 0) { perror("fcntl(VSOCK)"); close(vsock_fd); vsock_fd = -1; }
        }
    }

    printf("[ENGINE] listening on TCP port %d and VSOCK port %d\n", VSOCK_PORT, VSOCK_PORT);
    fflush(stdout);

    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) { perror("epoll_create1"); return 1; }

    struct epoll_event ev = {0};
    ev.events   = EPOLLIN | EPOLLET;
    ev.data.fd  = tcp_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, tcp_fd, &ev) < 0) {
        perror("epoll_ctl(tcp)"); return 1;
    }
    if (vsock_fd >= 0) {
        ev.events   = EPOLLIN | EPOLLET;
        ev.data.fd  = vsock_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, vsock_fd, &ev) < 0) {
            perror("epoll_ctl(vsock)"); return 1;
        }
    }

    struct epoll_event events[MAX_EVENTS];

    for (;;) {
        int n_ready = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (n_ready < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait"); break;
        }

        for (int i = 0; i < n_ready; i++) {
            int ev_fd = events[i].data.fd;

            // ── New connection ─────────────────────────────────────────────────
            if (ev_fd == tcp_fd || (vsock_fd >= 0 && ev_fd == vsock_fd)) {
                for (;;) {
                    int client_fd = accept(ev_fd, NULL, NULL);
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        perror("accept"); break;
                    }
                    if (set_nonblocking(client_fd) < 0) {
                        close(client_fd); continue;
                    }

                    int sndbuf = 4096;
                    setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
                    int nodelay = 1;
                    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

                    int slot = find_client_slot();
                    if (slot < 0) {
                        fprintf(stderr, "[ENGINE] max clients reached, dropping connection\n");
                        close(client_fd);
                        continue;
                    }
                    clients[slot].fd      = client_fd;
                    clients[slot].buf_len = 0;

                    struct epoll_event cev = {0};
                    cev.events   = EPOLLIN | EPOLLET | EPOLLRDHUP;
                    cev.data.fd  = client_fd;
                    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &cev) < 0) {
                        perror("epoll_ctl(client)");
                        clients[slot].fd = -1;
                        close(client_fd);
                    }
                }
                continue;
            }

            // ── Client data or disconnect ──────────────────────────────────────
            if (events[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
                int slot = find_client_by_fd(ev_fd);
                if (slot >= 0) clients[slot].fd = -1;
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ev_fd, NULL);
                close(ev_fd);
                continue;
            }

            if (events[i].events & EPOLLIN) {
                int slot = find_client_by_fd(ev_fd);
                if (slot < 0) continue;

                Client *c = &clients[slot];

                for (;;) {
                    int space = CLIENT_BUF_SIZE - c->buf_len - 1;
                    if (space <= 0) {
                        c->buf_len = 0;
                        space = CLIENT_BUF_SIZE - 1;
                    }

                    ssize_t r = recv(ev_fd, c->buf + c->buf_len, space, 0);
                    if (r < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        goto close_client;
                    }
                    if (r == 0) goto close_client;

                    c->buf_len += (int)r;

                    char *start = c->buf;
                    char *end;
                    while ((end = memchr(start, '\n', c->buf_len - (int)(start - c->buf))) != NULL) {
                        // Null terminate line to ease parsing
                        *end = '\0';
                        message_count++; // Sequential order_id mapping (1-indexed)

                        char type[16] = {0};
                        unsigned long long price = 0;
                        unsigned long long qty = 0;
                        char side[8] = {0};
                        unsigned long long target_order_id = 0;
                        unsigned long long order_id = 0;

                        char *type_ptr = strstr(start, "\"type\":\"");
                        if (type_ptr) {
                            sscanf(type_ptr + 8, "%15[^\"]", type);
                        }

                        char *oid_ptr = strstr(start, "\"order_id\":");
                        if (oid_ptr) {
                            order_id = strtoull(oid_ptr + 11, NULL, 10);
                        } else {
                            order_id = message_count;
                        }



                        MatchedTrade trades[MAX_TRADES_PER_ORDER];
                        int num_trades = 0;

                        if (strcmp(type, "limit") == 0) {
                            if (order_id > max_order_id_processed) {
                                max_order_id_processed = order_id;
                                char *price_ptr = strstr(start, "\"price\":");
                                if (price_ptr) price = strtoull(price_ptr + 8, NULL, 10);
                                char *qty_ptr = strstr(start, "\"qty\":");
                                if (qty_ptr) qty = strtoull(qty_ptr + 6, NULL, 10);
                                char *side_ptr = strstr(start, "\"side\":\"");
                                if (side_ptr) sscanf(side_ptr + 8, "%7[^\"]", side);

                                int is_sell = (strcmp(side, "sell") == 0);
                                if (is_sell) {
                                    match_sell_order(order_id, price, qty, 0, trades, &num_trades);
                                } else {
                                    match_buy_order(order_id, price, qty, 0, trades, &num_trades);
                                }
                            }
                        } else if (strcmp(type, "market") == 0) {
                            if (order_id > max_order_id_processed) {
                                max_order_id_processed = order_id;
                                char *qty_ptr = strstr(start, "\"qty\":");
                                if (qty_ptr) qty = strtoull(qty_ptr + 6, NULL, 10);
                                char *side_ptr = strstr(start, "\"side\":\"");
                                if (side_ptr) sscanf(side_ptr + 8, "%7[^\"]", side);

                                int is_sell = (strcmp(side, "sell") == 0);
                                if (is_sell) {
                                    match_sell_order(order_id, 0, qty, 1, trades, &num_trades);
                                } else {
                                    match_buy_order(order_id, 0, qty, 1, trades, &num_trades);
                                }
                            }
                        } else if (strcmp(type, "cancel") == 0) {
                            char *id_ptr = strstr(start, "\"order_id\":");
                            if (id_ptr) target_order_id = strtoull(id_ptr + 11, NULL, 10);
                            cancel_order(target_order_id);
                        }

                        // Generate JSON response
                        char response_buf[8192];
                        int response_len = 0;
                        if (num_trades == 0) {
                            strcpy(response_buf, "{\"trades\":[]}\n");
                            response_len = 14;
                        } else {
                            response_len = sprintf(response_buf, "{\"trades\":[");
                            for (int t = 0; t < num_trades; t++) {
                                response_len += sprintf(response_buf + response_len,
                                    "{\"maker_order_id\":%llu,\"taker_order_id\":%llu,\"price\":%llu,\"quantity\":%u}%s",
                                    trades[t].maker_order_id,
                                    trades[t].taker_order_id,
                                    trades[t].price,
                                    trades[t].quantity,
                                    (t == num_trades - 1) ? "" : ","
                                );
                            }
                            response_len += sprintf(response_buf + response_len, "]}\n");
                        }

                        // Restore newline char
                        *end = '\n';

                        ssize_t sent = 0;
                        while (sent < response_len) {
                            ssize_t w = write(ev_fd, response_buf + sent, response_len - sent);
                            if (w < 0) {
                                if (errno == EAGAIN) {
#if defined(__x86_64__)
                                    __builtin_ia32_pause();
#endif
                                    continue;
                                }
                                goto close_client;
                            }
                            sent += w;
                        }
                        start = end + 1;
                    }

                    int remaining = c->buf_len - (int)(start - c->buf);
                    if (remaining > 0 && start != c->buf) {
                        memmove(c->buf, start, remaining);
                    }
                    c->buf_len = remaining;
                }
                continue;

close_client:
                clients[slot].fd = -1;
                clients[slot].buf_len = 0;
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ev_fd, NULL);
                close(ev_fd);
            }
        }
    }

    close(tcp_fd);
    if (vsock_fd >= 0) close(vsock_fd);
    close(epoll_fd);
    return 0;
}