#include "agg.h"

static int keyeq(void *context, const void *rec1, const void *rec2) {

	return 0;
}

static void *init(void *context, const void *rec) {


	return 0;
}

static void *trans(void *context, const void *rec, void *data) {

	return 0;
}

static void finalize(void *context, const void *rec, void *data) {

}

static int serialize(xrg_iter_t *iter, char *data, int len) {

	return 0;
}

static int get_serialize_size(xrg_iter_t *iter) {

	return 0;
}


xrg_agg_t *xrg_agg_init(List *retrieved_attrs, List *aggfnoids, List *groupby_attrs) {

	xrg_agg_t *agg = (xrg_agg_t*) palloc(sizeof(xrg_agg_t));
	Assert(agg);

	agg->attr = 0;
	agg->retrieved_attrs = retrieved_attrs;
	agg->aggfnoids = aggfnoids;
	agg->groupby_attrs = groupby_attrs;
	agg->batchid = 0;
	memset(&agg->agg_iter, 0, sizeof(hagg_iter_t));

	Assert(aggfnoids);
	agg->ncol = list_length(aggfnoids);

	agg->hagg = hagg_start(agg, 100, ".", keyeq, init, trans);


	return 0;
}


static int xrg_agg_process(xrg_agg_t *agg, kite_result_t *res) {
	char errmsg[1024];
	xrg_iter_t *iter = 0;
	char *buf = 0;
	int buflen = 0;

	if (res->ncol != agg->ncol) {
		elog(ERROR, "xrg_agg_process: number of columns returned from kite not match (%d != %d)", agg->ncol, res->ncol);
		return 1;
	}

	while ((iter = kite_result_next(res)) != 0) {
		int len = 0;
		uint64_t hval = 0; // TODO: hash value of the groupby keys

		if (! agg->attr) {
			agg->attr = (xrg_attr_t *) palloc(sizeof(xrg_attr_t) * iter->nitem);
			memcpy(agg->attr, iter->attr, sizeof(xrg_attr_t) * iter->nitem);
		}

		int sz = get_serialize_size(iter);

		if (!buf) {
			buf = (char *) palloc(sz);
			buflen = sz;
		}

		if (buf && sz > buflen) {
			int newsz = sz * 2;
			buf = (char *) repalloc(buf, newsz);
			buflen = newsz;
		}

		len = serialize(iter, buf, buflen);
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
	kite_result_t *res = 0;
	while ((res = kite_get_result(ss)) != 0) {
		xrg_agg_process(agg, res);

		kite_result_destroy(res);
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


