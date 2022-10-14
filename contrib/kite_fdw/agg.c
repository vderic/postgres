#include "agg.h"

extern bool aggfnoid_is_avg(int aggfnoid);

static char *column_next(xrg_attr_t *attr, char *p) {
	if (attr->itemsz > 0) {
		p +=  attr->itemsz;
	} else {
		p += xrg_bytea_len(p) + 4;
	}

	return p;
}

static char *get_tuple(const void *rec, xrg_attr_t *attrs, int cno) {

	char *p = (char *) rec;
	for (int i = 0 ; i < cno ; i++, attrs++) {
		p = column_next(attrs, p);
	}

	return p;
}

static int get_ncol_from_aggfnoids(List *aggfnoids) {
	ListCell *lc;
	int i = 0;

	foreach (lc, aggfnoids) {
		int fn = lfirst_oid(lc);
		if (aggfnoid_is_avg(fn)) {
			i += 2;
		} else {
			i++;
		}
	}
	return i;
}


static int keyeq(void *context, const void *rec1, const void *rec2) {
	xrg_agg_t *agg = (xrg_agg_t *) context;
	ListCell *lc;

	if (! agg->groupby_attrs) {
		return 1;
	}

	foreach (lc, agg->groupby_attrs) {
		int gby = lfirst_int(lc);
		char *data1 = get_tuple(rec1, agg->attr, gby);
		int itemsz = agg->attr[gby].itemsz;
		char *data2 = get_tuple(rec2, agg->attr, gby);

		if (itemsz > 0) {
			if (memcmp(data1, data2, itemsz) != 0) {
				return 0;
			}
		} else {
			//bytea

			int itemsz1 = xrg_bytea_len(data1);
			int itemsz2 = xrg_bytea_len(data2);
			char* ptr1 = xrg_bytea_ptr(data1);
			char* ptr2 = xrg_bytea_ptr(data2);

			if (itemsz1 != itemsz2 || memcmp(ptr1, ptr2, itemsz1) != 0) {
				return 0;
			}
		}
	}

	return 1;
}

static void *transdata_create(Oid aggfn, xrg_attr_t *attr, char *p) {

	return 0;
}

static void *transdata2_create(Oid aggfn, xrg_attr_t *attr1, char *p1, xrg_attr_t *attr2, char *p2) {


	return 0;
}

static void *init(void *context, const void *rec) {

	xrg_agg_t *agg = (xrg_agg_t *) context;
	ListCell *lc;
	int sz = 0;
	char *p = (char *) rec;
	List *agglist = NIL;

	int i = 0;
	xrg_attr_t *attr = agg->attr;
	foreach (lc, agg->aggfnoids) {
		Oid fn = lfirst_oid(lc);
		if (fn > 0) {
			if (aggfnoid_is_avg(fn)) {
				char *p1 = p;
				xrg_attr_t *attr1 = attr++;
				char *p2 = column_next(attr1, p1);
				xrg_attr_t *attr2 = attr++;
				void *transdata = transdata2_create(fn, attr1, p1, attr2, p2);
				agglist = lappend(agglist, transdata);

				p = column_next(attr2, p2);
				i += 2;
			} else {
				void *transdata = transdata_create(fn, attr, p);
				agglist = lappend(agglist, transdata);

				p = column_next(attr, p);

				attr++;
				i++;
			}
		} else {
			p = column_next(attr, p);
			attr++;
			i++;
		}
	}

	return agglist;
}

static void *trans(void *context, const void *rec, void *data) {
	List *translist = (List *)data;

	return 0;
}

static void finalize(void *context, const void *rec, void *data) {
	List *translist = (List *)data;

}

static int get_serialize_size(xrg_iter_t *iter) {

	int sz = 0;
	for (int i = 0 ; i < iter->nitem ; i++) {
		if (iter->attr[i].itemsz >= 0) {
			sz += iter->attr[i].itemsz;
		} else {
			sz += xrg_bytea_len(iter->value[i]) + 4;
		}
	}

	return sz;
}

static int serialize(xrg_iter_t *iter, char **buf, int *buflen) {
	char *p = 0;
	int sz = get_serialize_size(iter);
	if (!*buf) {
		*buf = (char *) palloc(sz);
		*buflen = sz;
	}

	if (*buf && sz > *buflen) {
		int newsz = sz * 2;
		*buf = (char *) repalloc(*buf, newsz);
		*buflen = newsz;
	}

	p = *buf;

	for (int i = 0 ; i < iter->nitem ; i++) {
		if (iter->attr[i].itemsz >= 0) {
			memcpy(p, iter->value[i], iter->attr[i].itemsz);
			p += iter->attr[i].itemsz;
		} else {
			int len = xrg_bytea_len(iter->value[i]) + 4;
			memcpy(p, iter->value[i], len);
			p += len;
		}
	}

	return sz;
}

xrg_agg_t *xrg_agg_init(List *retrieved_attrs, List *aggfnoids, List *groupby_attrs) {

	xrg_agg_t *agg = (xrg_agg_t*) palloc(sizeof(xrg_agg_t));
	Assert(agg);

	agg->reached_eof = false;
	agg->attr = 0;
	agg->retrieved_attrs = retrieved_attrs;
	agg->aggfnoids = aggfnoids;
	agg->groupby_attrs = groupby_attrs;
	agg->batchid = 0;
	memset(&agg->agg_iter, 0, sizeof(hagg_iter_t));

	Assert(aggfnoids);
	agg->ncol = get_ncol_from_aggfnoids(aggfnoids);

	agg->hagg = hagg_start(agg, 100, ".", keyeq, init, trans);


	return 0;
}

void xrg_agg_destroy(xrg_agg_t *agg) {
	if (agg) {
		if (agg->hagg) {
			hagg_release(agg->hagg);
		}

	}
}



static int xrg_agg_process(xrg_agg_t *agg, kite_result_t *res) {
	char errmsg[1024];
	xrg_iter_t *iter = 0;
	char *buf = 0;
	int buflen = 0;
	if (res->ncol != agg->ncol) {
		elog(ERROR, "xrg_agg_process: number of columns returned from kite not match (%d != %d)", 
				agg->ncol, res->ncol);
		return 1;
	}

	while ((iter = kite_result_next(res)) != 0) {
		int len = 0;
		uint64_t hval = 0; // TODO: hash value of the groupby keys

		if (! agg->attr) {
			agg->attr = (xrg_attr_t *) palloc(sizeof(xrg_attr_t) * iter->nitem);
			agg->ncol = iter->nitem;
			memcpy(agg->attr, iter->attr, sizeof(xrg_attr_t) * iter->nitem);
		}

		len = serialize(iter, &buf, &buflen);
		hagg_feed(agg->hagg, hval, buf, len);

	}

	xrg_iter_release(iter);

	if (buf) pfree(buf);

	return 0;
}

int xrg_agg_get_next(xrg_agg_t *agg, sockstream_t *ss, Datum *datums, bool *flag, int n) {

	const void *rec = 0;
	void *data = 0;

	// get all data from socket

	if (! agg->reached_eof)  {
		kite_result_t *res = 0;
		while ((res = kite_get_result(ss)) != 0) {
			xrg_agg_process(agg, res);
	
			kite_result_destroy(res);
		}

		agg->reached_eof = true;
	}

	// obtain and process the batch
	if (! agg->agg_iter.tab) {
		int max = hagg_batch_max(agg->hagg);
		if (agg->batchid < max) {
			hagg_process_batch(agg->hagg, agg->batchid, &agg->agg_iter);
			agg->batchid++;
		} else {
			// no more rows
			return 1;
		}
	}

	hagg_next(&agg->agg_iter, &rec, &data);
	if (rec == 0) {
		// End of Batch and try process next batch
		int max = hagg_batch_max(agg->hagg);
		memset(&agg->agg_iter, 0, sizeof(hagg_iter_t));
		if (agg->batchid < max) {
			hagg_process_batch(agg->hagg, agg->batchid, &agg->agg_iter);
			agg->batchid++;
		} else {
			// no more rows
			return 1;
		}

		hagg_next(&agg->agg_iter, &rec, &data);
		if (rec == 0) {
			elog(ERROR, "hagg_next: no record found");
		}
	}
	
	finalize(agg, rec, data);


	return 0;
}


