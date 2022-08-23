#include "kite_client.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/wait.h>
#include <unistd.h>

#include "utils/elog.h"

int kite_connect(sockstream_t **ss, char *host) {
        struct addrinfo hints, *res;
	int sockfd = 0;

        char *port = strchr(host, ':');
        if (port == NULL) {
                elog(LOG, "kite: host should be in hostname:port format");
                return 1;
        }

        *port = 0;
        port++;


        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        if (0 != getaddrinfo(host, port, &hints, &res)) {
                elog(LOG, "kite: getaddrinfo error");
                return 1;
        }

        sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (sockfd < 0) {
                elog(LOG, "kite: cannot create socket");
                return 2;
        }

        if (-1 == connect(sockfd, res->ai_addr, res->ai_addrlen)) {
                elog(LOG, "kite: cannot open kite connection");
                return 3;
        }

        freeaddrinfo(res);

        *ss = sockstream_assign(sockfd);
        if (!*ss) {
                elog(LOG, "kite: out of memory");
                return 4;
        }
	return 0;
}

void kite_destroy(sockstream_t *ss) {
	sockstream_destroy(ss);
}

kite_result_t *kite_get_result(sockstream_t *ss, int ncol, char *json) {
	kite_result_t *res = 0;

        if (sockstream_send(ss, "KIT1", 0, 0)) {
                elog(ERROR, "%s", sockstream_errmsg(ss));
                return 0;
        }

        if (sockstream_send(ss, "JSON", strlen(json), json)) {
                elog(ERROR, "%s", sockstream_errmsg(ss));
                return 0;
        }

	res = palloc0(sizeof(kite_result_t));
	res->ss = ss;
	res->ncol = ncol;
	return 0;
}

int kite_result_get_next(kite_result_t *res, int ncol, Datum *datum) {

	return 0;
}

int kite_result_eos(kite_result_t) {

	return 0;
}

void kite_result_destroy(kite_result_t *res) {

}

