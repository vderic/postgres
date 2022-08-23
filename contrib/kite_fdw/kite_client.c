#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kite_client.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/wait.h>
#include <unistd.h>
#include "lz4.h"


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
	res->vec = palloc0(ncol * sizeof(xrg_vector_t *));
	return 0;
}

static int kite_result_fill_column(kite_result_t *res, xrg_vector_t **retv) {
	xrg_vector_t *v = 0;
	sockbuf_t sockbuf;

	*retv = 0;

	sockbuf_init(&sockbuf);
	
	if (sockstream_recv(res->ss, &sockbuf)) {
		elog(ERROR, "%s", sockstream_errmsg(res->ss));
		sockbuf_final(&sockbuf);
		return -1;
	}

	if (memcmp(sockbuf.msgty, "BYE_", 4) == 0) {
		sockbuf_final(&sockbuf);
		return 0;
	}

	if (memcmp(sockbuf.msgty, "ERR_", 4) == 0) {
		elog(ERROR, "%s", sockbuf.buf);
		sockbuf_final(&sockbuf);
		return -1;
	}

	if (memcmp(sockbuf.msgty, "VEC_", 4) != 0) {
		elog(ERROR, "VEC_ expected");
		sockbuf_final(&sockbuf);
		return -2;
	}

	if (sockbuf.msgsz == 0) {
		elog(ERROR, "sockstream_recv: VEC_ with size ZERO");
		sockbuf_final(&sockbuf);
		return -3;
	}

	v = (xrg_vector_t *) sockbuf.buf;
	if (xrg_vector_is_compressed(v)) {
		int ret = 0;
                int newsz = xrg_align(16, sizeof(v->header) + v->header.nbyte + v->header.nitem);

                char *buf = palloc(newsz);
                int zbyte = v->header.zbyte;
                int nbyte = v->header.nbyte;
                int nitem = v->header.nitem;

                memcpy(buf, v, sizeof(v->header));
                buf += sizeof(v->header);

                ret = LZ4_decompress_safe(XRG_VECTOR_DATA(v), buf, zbyte, nbyte);

                Assert(ret == nbyte);

                buf += nbyte;
                memcpy(buf, XRG_VECTOR_FLAG(v), nitem);

		*retv = (xrg_vector_t *) buf;
        } else {
                char *buf = palloc(sockbuf.msgsz);
                memcpy(buf, sockbuf.buf, sockbuf.msgsz);
		*retv = (xrg_vector_t *) buf;
        }

	sockbuf_final(&sockbuf);
	return (*retv)->header.nitem;
}


static bool kite_result_fill(kite_result_t *res, int ncol) {
	int i = 0, ret = 0;
	int32_t nrow = 0;
	sockbuf_t sockbuf;

	/* fill all columns */
	for (i = 0 ; i < ncol ; i++) {
		// fill each column
		ret = kite_result_fill_column(res, &res->vec[i]);
		if (ret <= 0) {
			// BYE or ERROR
			res->cursor = 0;
			res->nrow = 0;
			sockbuf_final(&sockbuf);
			return false;
		}
		Assert(nrow == res->vec[i]->header.nitem);
		nrow = res->vec[i]->header.nitem;
	}

	/* check (VEC_ and size == 0) for end of row group */
	sockbuf_init(&sockbuf);

	if (sockstream_recv(res->ss, &sockbuf)) {
		elog(ERROR, "sockstream_recv: not end of rowgroup. expected VEC_ with zero size");
		sockbuf_final(&sockbuf);
		return false;
	}

	Assert(memcmp(sockbuf.msgty, "VEC_") == 0 && sockbuf.msgsz == 0);

	sockbuf_final(&sockbuf);

	res->cursor = 0;
	res->nrow = nrow;

	return true;

}

static bool kite_result_has_more(kite_result_t *res) {

	return (res->cursor < res->nrow);
}


static void kite_result_reset(kite_result_t *res) {

	int i = 0 ;
	for (i = 0 ; i < res->ncol ; i++) {
		if (res->vec[i]) {
			pfree(res->vec[i]);
			res->vec[i] = 0;
		}
	}
}

static void kite_result_get_next_datum(int ncol, Datum *values, bool *isnulls) {

}

bool kite_result_get_next(kite_result_t *res, int ncol, Datum *values, bool *isnulls) {

	if (! kite_result_has_more(res)) {
		bool ok = false;

		kite_result_reset(res);
		ok = kite_result_fill(res, ncol);
		if (! ok) {
			return false;
		}
	}

	if (kite_result_has_more(res)) {
		kite_result_get_next_datum(ncol, values, isnulls);
		return true;
	}

	return true;
}

int kite_result_eos(kite_result_t) {

	return 0;
}

void kite_result_destroy(kite_result_t *res) {

}

