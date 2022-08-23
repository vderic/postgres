#ifndef KITE_CLIENT_H
#define KITE_CLIENT_H


#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "access/tupdesc.h"
#include "sockstream.h"
#include "xrg.h"

typedef struct kite_result_t {
	
	sockstream_t *ss;
	int ncol;
	xrg_vector_t *vec;
	int cursor;
	int nrow;

} kite_result_t;


sockstream_t *kite_connect(char *host, int port);

kite_result_t *kite_get_result(sockstream_t *ss, int ncol, char *json);

int kite_result_get_next(kite_result_t *res, int ncol, Datum *datum);

int kite_result_eos(kite_result_t);

void kite_result_destroy(kite_result_t *res);

#ifdef __cplusplus
}
#endif

#endif
