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
#include "dec.h"

#include "utils/timestamp.h"
#include "utils/numeric.h"

/*
bool kite_result_scan_next(kite_result_t *res,
                int ncol,
                Datum *values, bool *isnulls) {

        if (! kite_result_has_more(res)) {
                bool ok = false;

                kite_result_reset(res);
                ok = kite_result_fill(res, ncol);
                if (! ok) {
                        return false;
                }

                // decode
                kite_result_decode(res, attinmeta, retrieved_attrs);
        }

        if (kite_result_has_more(res)) {
                kite_result_scan_next_datum(res, ncol, values, isnulls);
                return true;
        }

        return true;
}
*/


/* decode */
static inline Datum decode_int16(char *data, int it) {
	int16_t *p = (int16_t*) data;
	return Int16GetDatum(p[it]);
}

static inline Datum decode_char(char *p, int32_t it) {
	return CharGetDatum(p[it]);
}

static inline Datum decode_int32(char *data, int32_t it) {
	int32_t *p = (int32_t *) data;
	return Int32GetDatum(p[it]);
}

static inline Datum decode_int64(char *data, int32_t it) {
	int64_t *p = (int64_t *) data;
	return Int64GetDatum(p[it]);
}

static inline Datum decode_int128(char *data, int32_t it) {
	__int128_t *p = (__int128_t *) data;
	return PointerGetDatum(&p[it]);
}

static inline Datum decode_float(char *data, int32_t it) {
	float *p = (float *) data;
	return Float4GetDatum(p[it]);
}

static inline Datum decode_double(char *data, int32_t it) {
	double *p = (double *) data;
	return Float8GetDatum(p[it]);
}

static inline Datum decode_date(int32_t d) {
        d -= (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
        return Int32GetDatum(d);
}
static inline Datum decode_time(int64_t t) {
        return Int64GetDatum(t);
}

static Datum decode_timestamp(int64_t ts) {
        Timestamp epoch_ts = SetEpochTimestamp();
        ts += epoch_ts;
        return Int64GetDatum(ts);
}


/* xrg_column_t */
xrg_column_t *xrg_column_create(xrg_vector_t *v) {
	int nitem = v->header.nitem;
	int16_t ltyp = v->header.ltyp;
	xrg_column_t *c = palloc(sizeof(xrg_column_t));
	c->v = v;
	
	if (ltyp == XRG_LTYP_NONE) {
		c->datumv = 0;
	} else {
		c->datumv = palloc(nitem * sizeof(Datum));
	}

	return c;
}

void xrg_column_final(xrg_column_t *c) {
	if (!c) {
		return;
	}
	if (c->datumv) {
		pfree(c->datumv);
	}
	if (c->v) {
		pfree(c->v);
	}
	pfree(c);
}

Datum xrg_column_get_value(xrg_column_t *c, int row) {
	
	int16_t ltyp = c->v->header.ltyp;
	int16_t ptyp = c->v->header.ptyp;
	char *data = XRG_VECTOR_DATA(c->v);

	if (ltyp == XRG_LTYP_NONE) {
                switch (ptyp) {
                case XRG_PTYP_INT8: {
                        return decode_char(data, row);
                } break;
                case XRG_PTYP_INT16: {
                        return decode_int16(data, row);
                } break;
                case XRG_PTYP_INT32: {
                        return decode_int32(data, row);
                } break;
                case XRG_PTYP_INT64: {
                        return decode_int64(data, row);
                } break;
                case XRG_PTYP_INT128: {
                        return decode_int128(data, row);
                } break;
                case XRG_PTYP_FP32: {
                        return decode_float(data, row);
                } break;
                case XRG_PTYP_FP64: {
                        return decode_double(data, row);
                } break;
                default: { return c->datumv[row]; }
                }
	}
       
	return c->datumv[row];
}

bool xrg_column_get_isnull(xrg_column_t *c, int row) {
	char *flag = XRG_VECTOR_FLAG(c->v);
	return flag[row];
}


int xrg_column_fill(xrg_column_t **c, sockstream_t *ss) {
	xrg_vector_t *v = 0;
	sockbuf_t sockbuf;

	*c = 0;
	sockbuf_init(&sockbuf);
	
	if (sockstream_recv(ss, &sockbuf)) {
		elog(ERROR, "%s", sockstream_errmsg(ss));
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

		*c = xrg_column_create((xrg_vector_t *) buf);
        } else {
                char *buf = palloc(sockbuf.msgsz);
                memcpy(buf, sockbuf.buf, sockbuf.msgsz);
		*c = xrg_column_create((xrg_vector_t *) buf);
        }

	sockbuf_final(&sockbuf);
	return (*c)->v->header.nitem;
}

void xrg_column_decode(xrg_column_t *c, FmgrInfo *flinfo, Oid ioparams, int32_t typmod) {

	xrg_vector_t *v = c->v;
	Datum *datumv = c->datumv;
	int16_t ltyp = c->v->header.ltyp;
	int16_t ptyp = c->v->header.ptyp;
	int nitem = c->v->header.nitem;

	switch (ltyp) {
	case XRG_LTYP_NONE:
		// primitive type. no decode here
		return;
	case XRG_LTYP_DATE: 
	{
		int32_t *p = (int32_t *) XRG_VECTOR_DATA(v);
		char *flag = XRG_VECTOR_FLAG(v);
		int i = 0;
		for (i = 0 ; i < nitem; i++) {
			datumv[i] = (flag[i] & XRG_FLAG_NULL) ? 0 : decode_date(p[i]);
		}
	}
	return;
	case XRG_LTYP_TIME:
	{
		int64_t *p = (int64_t *) XRG_VECTOR_DATA(v);
		char *flag = XRG_VECTOR_FLAG(v);
		int i = 0;
		for (i = 0 ; i < nitem; i++) {
			datumv[i] = (flag[i] & XRG_FLAG_NULL) ? 0 : decode_time(p[i]);
		}
	}
	return;
	case XRG_LTYP_TIMESTAMP:
	{
		int64_t *p = (int64_t *) XRG_VECTOR_DATA(v);
		char *flag = XRG_VECTOR_FLAG(v);
		int i = 0;
		for (i = 0 ; i < nitem; i++) {
			datumv[i] = (flag[i] & XRG_FLAG_NULL) ? 0 : decode_timestamp(p[i]);
		}
	}
	return;
	case XRG_LTYP_INTERVAL:
	{
		__int128_t *p = (__int128_t *) XRG_VECTOR_DATA(v);
		char *flag = XRG_VECTOR_FLAG(v);
		int i = 0;
		for (i = 0 ; i < nitem; i++) {
			datumv[i] = (flag[i] & XRG_FLAG_NULL) ? 0 : PointerGetDatum(&p[i]);
		}
	}
	return;
	case XRG_LTYP_DECIMAL:
	case XRG_LTYP_STRING:
	break;
	default:
	{
		elog(ERROR, "invalid xrg logical type %d", ltyp);
		return;

	}
	}

	if (ltyp == XRG_LTYP_DECIMAL && ptyp == XRG_PTYP_INT64) {
		int64_t *p = (int64_t *) XRG_VECTOR_DATA(v);
		char *flag = XRG_VECTOR_FLAG(v);
		int scale = v->header.scale;
		int precision = v->header.precision;
		int i = 0;

		for (i = 0 ; i < nitem ; i++) {
			if (flag[i] & XRG_FLAG_NULL) {
				datumv[i] = 0;
			} else {
				char s[MAX_DEC128_PRECISION+2];
				int64_t v = p[i];
				dec64_to_string(v, scale, s);
				/*
                                FmgrInfo flinfo;
                                memset(&flinfo, 0, sizeof(FmgrInfo));
                                flinfo.fn_addr = numeric_in;
                                flinfo.fn_nargs = 3;
                                flinfo.fn_strict = true;
				*/
                                datumv[i] = InputFunctionCall(flinfo, s, ioparams, typmod);
			}
		}
		return;
	}

	if (ltyp == XRG_LTYP_DECIMAL && ptyp == XRG_PTYP_INT128) {
		__int128_t *p = (__int128_t *) XRG_VECTOR_DATA(v);
		char *flag = XRG_VECTOR_FLAG(v);
		int scale = v->header.scale;
		int precision = v->header.precision;
		int i = 0;

		for (i = 0 ; i < nitem ; i++) {
			if (flag[i] & XRG_FLAG_NULL) {
				datumv[i] = 0;
			} else {
				char s[MAX_DEC128_PRECISION+2];
				__int128_t v = p[i];
				dec128_to_string(v, scale, s);
				/*
                                FmgrInfo flinfo;
                                memset(&flinfo, 0, sizeof(FmgrInfo));
                                flinfo.fn_addr = numeric_in;
                                flinfo.fn_nargs = 3;
                                flinfo.fn_strict = true;
				*/
                                datumv[i] = InputFunctionCall(flinfo, s, ioparams, typmod);
			}
		}
		return;

	}

        if (ltyp == XRG_LTYP_STRING && ptyp == XRG_PTYP_BYTEA) {
                char *nextptr = XRG_VECTOR_DATA(v);
                char *flag = XRG_VECTOR_FLAG(v);
                for (int i = 0; i < nitem; i++) {
                        int sz = xrg_bytea_len(nextptr);
                        if (flag[i] & XRG_FLAG_NULL) {
                                datumv[i] = 0;
                        } else {
                                SET_VARSIZE(nextptr, sz + VARHDRSZ);
                                datumv[i] = PointerGetDatum(nextptr);
                        }
                        nextptr += sz + 4;
                }
                return;
        }
}


/* kite client */
int kite_connect(sockstream_t **ss, char *host) {
        struct addrinfo hints, *res;
	int sockfd = 0;
	char *hoststr = pstrdup(host);

        char *port = strchr(hoststr, ':');
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
	res->cols = palloc0(ncol * sizeof(xrg_column_t *));
	return res;
}

bool kite_result_fill(kite_result_t *res, int ncol) {
	int i = 0, ret = 0;
	int32_t nrow = 0;
	sockbuf_t sockbuf;

	/* fill all columns */
	for (i = 0 ; i < ncol ; i++) {
		// fill each column
		ret = xrg_column_fill(&res->cols[i], res->ss);
		if (ret <= 0) {
			// BYE or ERROR
			res->cursor = 0;
			res->nrow = 0;
			sockbuf_final(&sockbuf);
			return false;
		}
		Assert(nrow == res->cols[i]->v->header.nitem);
		nrow = res->cols[i]->v->header.nitem;
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

bool kite_result_has_more(kite_result_t *res) {

	return (res->cursor < res->nrow);
}


void kite_result_reset(kite_result_t *res) {

	int i = 0 ;
	for (i = 0 ; i < res->ncol ; i++) {
		if (res->cols[i]) {
			xrg_column_final(res->cols[i]);
			res->cols[i] = 0;
		}
	}
}

void kite_result_decode(kite_result_t *res, AttInMetadata *attinmeta, List *retrieved_attrs) {
	int j = 0;
	ListCell *lc;

	foreach (lc, retrieved_attrs) {
		int i = lfirst_int(lc);
		xrg_column_t *c = res->cols[j];
		xrg_column_decode(c, &attinmeta->attinfuncs[i-1], attinmeta->attioparams[i-1], attinmeta->atttypmods[i-1]);

		j++;
	}
}

int kite_result_scan_next(kite_result_t *res, int ncol, Datum *datums, bool *isnulls) {
	int i = 0; 

	if (res->cursor >= res->nrow) {
		return -1;
	}

	for (i = 0 ; i < ncol ; i++) {
		xrg_column_t *c = res->cols[i];
		datums[i] = xrg_column_get_value(c, res->cursor);
		isnulls[i] = xrg_column_get_isnull(c, res->cursor);
	}

	res->cursor++;
	return 0;
}


int kite_result_eos(kite_result_t) {

	return 0;
}

void kite_result_destroy(kite_result_t *res) {

}

