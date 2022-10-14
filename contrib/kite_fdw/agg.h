#ifndef AGG_H
#define AGG_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "xrg.h"
#include "hop/hashagg.h"
#include "hop/spill.h"
#include "kite_client.h"

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

};

xrg_agg_t *xrg_agg_init(List *retrieved_attrs, List *aggfnoids, List *groupby_attrs);
int xrg_agg_get_next(xrg_agg_t *agg, sockstream_t *ss, Datum *datums, bool *flag, int n);


#endif


