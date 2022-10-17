#ifndef AGG_H
#define AGG_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "xrg.h"
#include "hop/hashagg.h"
#include "hop/spill.h"
#include "kite_client.h"

typedef struct kite_target_t kite_target_t;
struct kite_target_t {
	Oid aggfn;
	int pgattr;
	List *attrs;
	bool gbykey;
	void *data;
};


typedef struct xrg_agg_t xrg_agg_t;
struct xrg_agg_t {
	int ncol;
	int batchid;
	hagg_t *hagg;
	hagg_iter_t agg_iter;

	List *groupby_attrs;
	List *aggfnoids;
	List *retrieved_attrs;

	xrg_attr_t *attr;
	bool reached_eof;

	int ntlist;
	kite_target_t *tlist;
};

xrg_agg_t *xrg_agg_init(List *retrieved_attrs, List *aggfnoids, List *groupby_attrs);
int xrg_agg_get_next(xrg_agg_t *agg, sockstream_t *ss, AttInMetadata *attinmeta, Datum *datums, bool *flag, int n);

void xrg_agg_destroy(xrg_agg_t *agg);

/* avg_trans_t */
typedef struct avg_trans_t avg_trans_t;
struct avg_trans_t {
	union {
		int64_t i64;
		__int128_t i128;
		double fp64;
	} sum;
	int64_t count;
};

#if 0
/* tuple data */
typedef union tupledata_t tupledata_t;
union tupledata_t {
	int8_t i8;
	int16_t i16;
	int32_t i32;
	int64_t i64;
	__int128_t i128;
	float fp32;
	float fp64;
	const char *p;
	struct {
		union {
			int64_t i64;
			__int128_t i128;
			double fp64;
		} sum;
		int64_t count;
	} avg;
};

int tupledata_primitive_init(int32_t aggfn, tupledata_t *pt, const void *p, xrg_attr_t *attr);

int tupledata_avg_init(int32_t aggfn, tupledata_t *pt, const void *p1, xrg_attr_t *attr1,
                const void *p2, xrg_attr_t *attr2);
#endif

int avg_trans_init(int32_t aggfn, avg_trans_t *pt, const void *p1, xrg_attr_t *attr1,
                const void *p2, xrg_attr_t *attr2);

void aggregate(int32_t aggfn, void *transdata, const void *data, xrg_attr_t *attr);

#endif


