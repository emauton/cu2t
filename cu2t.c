/* A bridge between UDP and TCP for the Carbon plaintext protocol:
 *   http://graphite.readthedocs.org/en/latest/feeding-carbon.html
 *
 * Designed as a simple process popping datagrams off a single UDP socket
 * and forwarding them to a single TCP backend. Should actually work for any
 * line protocol.
 *
 * The operator can run as many processes as they need to balance incoming
 * UDP traffic against TCP backends.
 *
 * Example:
 *     cu2t localhost 2003 backend 2003
 * forwards Carbon data from UDP localhost:2003 to TCP backend:2003.
 *
 * TODO(emauton): print stats to stdout every 5s or so
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


/* Bind a UDP listen socket to `addr'.
 *
 * Using SO_REUSEPORT on the UDP socket means that the kernel manages the
 * queueing and switching of datagrams into the bridge processes. See
 * https://lwn.net/Articles/542629/.
 *
 * See the `attach_socket' argument of `setup_socket()'. */
int udp_listener(struct addrinfo *addr) {
    int fd, optval, err;

    fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);

    if (fd == -1) {
        perror("udp_listener socket()");
        return -1;
    }

    optval = 1;
    err = setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));
    if (err != 0) {
        perror("udp_listener setsockopt()");
        return -1;
    }

    if (bind(fd, addr->ai_addr, addr->ai_addrlen) == 0) {
        return fd;
    } else {
        perror("udp_listener bind()");
        close(fd);
        return -1;
    }
}


/* Connect a TCP client socket to `addr'.
 *
 * See the `attach_socket' argument of `setup_socket()' */
int tcp_client(struct addrinfo *addr) {
    int fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);

    if (fd == -1) {
        perror("udp_listener socket()");
        return -1;
    }

    if (connect(fd, addr->ai_addr, addr->ai_addrlen) == 0) {
        return fd;
    } else {
        perror("tcp_listener connect()");
        close(fd);
        return -1;
    }
}


/* Resolve an address and attach a socket to it.
 *
 * `socktype' is SOCK_DGRAM or SOCK_STREAM.
 *
 * The `attach_socket' function takes an address, creates a socket with
 * whatever options, and connects (or binds) it to the resolved address given,
 * returning the socket on success or -1 on error.
 *
 * Addresses are tried in the order returned by getaddrinfo(). */
int setup_socket(char *host, char *port, int socktype,
                 int (*attach_socket)(struct addrinfo *addr)) {
    struct addrinfo *result, *p;
    struct addrinfo hints = {0};
    int fd, err;

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = socktype;

    err = getaddrinfo(host, port, &hints, &result);
    if (err != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        exit(EXIT_FAILURE);
    }

    for (p = result; p != NULL; p = p->ai_next) {
        fd = attach_socket(p);
        if (fd != -1)
            break;
    }

    if (p == NULL) {
        fprintf(stderr, "failed to attach %s:%s\n", host, port);
        exit(EXIT_FAILURE);
    }

    freeaddrinfo(result);
    return fd;
}


/* Send all data in `buf' to `fd', dying on error.
 *
 * Note MSG_NOSIGNAL: this avoids needing to set a SIG_IGN handler for SIGPIPE
 * in order to have EPIPE delivered correctly on socket disconnection. */
void send_or_die(int fd, char *buf, size_t len) {
    ssize_t nw;

    while (len != 0 && (nw = send(fd, buf, len, MSG_NOSIGNAL)) != 0) {
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


#define BUF_SIZE 1024

int main(int argc, char *argv[]) {
    int udp, tcp, n;
    char buf[BUF_SIZE];

    if (argc != 5) {
        fprintf(stderr,
                "Usage: cu2t <udp_host> <udp_port> <tcp_host> <tcp_port>\n");
        exit(EXIT_FAILURE);
    }

    udp = setup_socket(argv[1], argv[2], SOCK_DGRAM, udp_listener);
    tcp = setup_socket(argv[3], argv[4], SOCK_STREAM, tcp_client);

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
