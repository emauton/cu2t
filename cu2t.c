/* A bridge between UDP and TCP for the Carbon plaintext protocol:
 *   http://graphite.readthedocs.org/en/latest/feeding-carbon.html
 *
 * Designed as a simple process popping datagrams off a single UDP socket
 * and forwarding them to a single TCP backend. Should actually work for any
 * line protocol.
 *
 * The operator can run as many processes as they need to balance incoming
 * UDP traffic against TCP backends. Using SO_REUSEPORT on the UDP socket
 * means that the kernel manages the queueing and switching of datagrams into
 * the bridge processes. See https://lwn.net/Articles/542629/.
 *
 * Example:
 *     cu2t localhost 2003 backend 2003
 * forwards Carbon data from UDP localhost:2003 to TCP backend:2003.
 *
 * TODO(emauton): handle signals, print stats to stdout every 5s or so, maybe
 *                reconnect on failure (though allowing whatever it is that's
 *                supervising this to just restart is probably wiser).
 * */

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define BUF_SIZE 1024

void usage(void);
struct addrinfo *get_addr(char *host, char *port, int socktype);
int bind_udp_sock(char *host, char *port);
int connect_tcp_sock(char *host, char *port);
void send_or_die(int fd, char *buf, size_t len);

int main(int argc, char *argv[]) {
    int udp, tcp, n;
    char buf[BUF_SIZE];

    if (argc != 5)
        usage();

    udp = bind_udp_sock(argv[1], argv[2]);
    tcp = connect_tcp_sock(argv[3], argv[4]);

    while (1) {
        n = recv(udp, buf, BUF_SIZE, 0);

        if (n == -1) {
            perror("main recv()");
            continue;
        }

        send_or_die(tcp, buf, n);
    }

    return EXIT_SUCCESS;
}

void usage(void) {
    fprintf(stderr,
            "Usage: cu2t <udp_host> <udp_port> <tcp_host> <tcp_port>\n");
    exit(EXIT_FAILURE);
}

struct addrinfo *get_addr(char *host, char *port, int socktype) {
    struct addrinfo *ret;
    struct addrinfo hints;
    int err;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = socktype;

    err = getaddrinfo(host, port, &hints, &ret);
    if (err != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        exit(EXIT_FAILURE);
    }

    return ret;
}

int bind_udp_sock(char *host, char *port) {
    struct addrinfo *result, *rp;
    int fd, optval, err;

    result = get_addr(host, port, SOCK_DGRAM);

    /* Cf. getaddrinfo(3). */
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd == -1) {
            perror("bind_udp_sock socket()");
            continue;
        }

        optval = 1;
        err = setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));
        if (err != 0) {
            perror("bind_udp_sock setsockopt()");
            continue;
        }

        if (bind(fd, rp->ai_addr, rp->ai_addrlen) == 0)
            break;                  /* Success */

        perror("bind_udp_sock bind()");
        close(fd);
    }

    if (rp == NULL) {               /* No address succeeded */
        fprintf(stderr, "Could not bind %s:%s\n", host, port);
        exit(EXIT_FAILURE);
    }

    freeaddrinfo(result);
    return fd;
}

int connect_tcp_sock(char *host, char *port) {
    struct addrinfo *result, *rp;
    int fd;

    result = get_addr(host, port, SOCK_STREAM);

    /* Cf. getaddrinfo(3). */
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd == -1) {
            perror("connect_tcp_sock socket()");
            continue;
        }

        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0)
            break;                  /* Success */

        perror("connect_tcp_sock connect()");
        close(fd);
    }

    if (rp == NULL) {               /* No address succeeded */
        fprintf(stderr, "Could not connect %s:%s\n", host, port);
        exit(EXIT_FAILURE);
    }

    freeaddrinfo(result);
    return fd;
}

void send_or_die(int fd, char *buf, size_t len) {
    ssize_t nw;

    while (len != 0 && (nw = send(fd, buf, len, 0)) != 0) {
        if (nw == -1) {
            if (errno == EINTR)
                continue;
            else {
                fprintf(stderr, "writing to fd %d: %s\n", fd, strerror(errno));
                exit(EXIT_FAILURE);
            }
        }
        len -= nw;
        buf += nw;
    }
}

