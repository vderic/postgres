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

typedef struct xrg_column_t {
	xrg_vector_t *v;
	Datum *datumv;
} xrg_column_t;

xrg_column_t *xrg_column_create(xrg_vector_t *v);

void xrg_column_final(xrg_column_t *col);

int xrg_column_fill(xrg_column_t **c, sockbuf_t *sbuf);

void xrg_column_decode(xrg_column_t *c, FmgrInfo *flinfo, Oid ioparams, int32_t typmod);

Datum xrg_column_get_value(xrg_column_t *c, int row);

bool xrg_column_get_isnull(xrg_column_t *c, int row);


typedef struct kite_result_t {
	
	int ncol;
	xrg_column_t **cols;
	int nrow;

} kite_result_t;


int kite_connect(sockstream_t **ss, char *host);

void kite_destroy(sockstream_t *ss);

int kite_exec(sockstream_t *ss, char *json);

kite_result_t *kite_get_result(sockstream_t *ss);

bool kite_result_has_more(kite_result_t *res);

void kite_result_reset(kite_result_t *res);

bool kite_result_fill(sockstream_t *ss, kite_result_t *res);

void kite_result_decode(kite_result_t *res, AttInMetadata *attinmeta, List *retrieved_attrs);

int kite_result_scan_next(kite_result_t *res, int ncol, Datum *datums, bool *isnulls);

void kite_result_destroy(kite_result_t *res);

int kite_result_get_nfield(kite_result_t *res);

int kite_result_get_nrow(kite_result_t *res);


#ifdef __cplusplus
}
#endif

#endif
