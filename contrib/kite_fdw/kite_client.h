#ifndef KITE_CLIENT_H
#define KITE_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "funcapi.h"
#include "access/tupdesc.h"
#include "sockstream.h"
#include "xrg.h"

typedef struct kite_result_t {
	int ncol;
	int nrow;
	xrg_iter_t *iter;
	xrg_vector_t **cols;
	int cols_size;

} kite_result_t;

sockstream_t *kite_connect(char *host);

void kite_destroy(sockstream_t *ss);

int kite_exec(sockstream_t *ss, char *json);

kite_result_t *kite_get_result(sockstream_t *ss);

int xrg_vector_fill_sockbuf(xrg_vector_t **c, sockbuf_t *sbuf);

bool kite_result_fill(sockstream_t *ss, kite_result_t *res);

xrg_iter_t *kite_result_next(kite_result_t *res);

void kite_result_destroy(kite_result_t *res);

int kite_result_get_nfields(kite_result_t *res);

int kite_result_get_nrow(kite_result_t *res);

#ifdef __cplusplus
}
#endif

#endif
