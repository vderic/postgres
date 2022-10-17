
#include "decode.h"
#include "decimal.h"
#include "utils/timestamp.h"
#include "utils/numeric.h"
#include "utils/fmgrprotos.h"

static inline Datum decode_int16(char *data) {
	int16_t *p = (int16_t *)data;
	return Int16GetDatum(*p);
}

static inline Datum decode_char(char *p) {
	return CharGetDatum(*p);
}

static inline Datum decode_int32(char *data) {
	int32_t *p = (int32_t *)data;
	return Int32GetDatum(*p);
}

static inline Datum decode_int64(char *data) {
	int64_t *p = (int64_t *)data;
	return Int64GetDatum(*p);
}

static inline Datum decode_int128(char *data) {
	__int128_t *p = (__int128_t *)data;
	return PointerGetDatum(p);
}

static inline Datum decode_float(char *data) {
	float *p = (float *)data;
	return Float4GetDatum(*p);
}

static inline Datum decode_double(char *data) {
	double *p = (double *)data;
	return Float8GetDatum(*p);
}

static inline Datum decode_date(char *data) {
	int32_t d = *((int32_t *)data);
	d -= (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
	return Int32GetDatum(d);
}
static inline Datum decode_time(char *data) {
	int64_t t = *((int64_t *)data);
	return Int64GetDatum(t);
}

static Datum decode_timestamp(char *data) {
	int64_t ts = *((int64_t *)data);
	Timestamp epoch_ts = SetEpochTimestamp();
	ts += epoch_ts;
	return Int64GetDatum(ts);
}

/* decode functions */
int var_decode(char *data, char flag, xrg_attr_t *attr, int atttypmod, Datum *pg_datum, bool *pg_isnull) {
	int ltyp = attr->ltyp;
	int ptyp = attr->ptyp;
	int precision = attr->precision;
	int scale = attr->scale;

	// data in iter->value[idx] and iter->flag[idx] and iter->attrs[idx].ptyp
	*pg_isnull = (flag & XRG_FLAG_NULL);

	switch (ltyp) {
	case XRG_LTYP_NONE:
		// primitive type. no decode here
		switch (ptyp) {
		case XRG_PTYP_INT8: {
			*pg_datum = decode_char(data);
		} break;
		case XRG_PTYP_INT16: {
			*pg_datum = decode_int16(data);
		} break;
		case XRG_PTYP_INT32: {
			*pg_datum = decode_int32(data);
		} break;
		case XRG_PTYP_INT64: {
			*pg_datum = decode_int64(data);
		} break;
		case XRG_PTYP_INT128: {
			*pg_datum = decode_int128(data);
		} break;
		case XRG_PTYP_FP32: {
			*pg_datum = decode_float(data);
		} break;
		case XRG_PTYP_FP64: {
			*pg_datum = decode_double(data);
		} break;
		default: {
			elog(ERROR, "decode_var: invalid physcial type %d with NONE logical type", ptyp);
			return -1;
		}
		}
		return 0;
	case XRG_LTYP_DATE: {
		*pg_datum = (flag & XRG_FLAG_NULL) ? 0 : decode_date(data);
	}
		return 0;
	case XRG_LTYP_TIME: {
		*pg_datum = (flag & XRG_FLAG_NULL) ? 0 : decode_time(data);
	}
		return 0;
	case XRG_LTYP_TIMESTAMP: {
		*pg_datum = (flag & XRG_FLAG_NULL) ? 0 : decode_timestamp(data);
	}
		return 0;
	case XRG_LTYP_INTERVAL: {
		*pg_datum = (flag & XRG_FLAG_NULL) ? 0 : PointerGetDatum((__int128_t *)data);
	}
		return 0;
	case XRG_LTYP_DECIMAL:
	case XRG_LTYP_STRING:
		break;
	default: {
		elog(ERROR, "invalid xrg logical type %d", ltyp);
		return -1;
	}
	}

	if (ltyp == XRG_LTYP_DECIMAL && ptyp == XRG_PTYP_INT64) {
		FmgrInfo flinfo;
		int64_t v = *((int64_t *)data);
		char dst[MAX_DEC128_STRLEN];
		decimal64_to_string(v, precision, scale, dst, sizeof(dst));
		memset(&flinfo, 0, sizeof(FmgrInfo));
		flinfo.fn_addr = numeric_in;
		flinfo.fn_nargs = 3;
		flinfo.fn_strict = true;
		*pg_datum = InputFunctionCall(&flinfo, dst, 0, atttypmod);
		return 0;
	}

	if (ltyp == XRG_LTYP_DECIMAL && ptyp == XRG_PTYP_INT128) {
		FmgrInfo flinfo;
		__int128_t v = *((__int128_t *)data);
		char dst[MAX_DEC128_STRLEN];
		decimal128_to_string(v, precision, scale, dst, sizeof(dst));
		memset(&flinfo, 0, sizeof(FmgrInfo));
		flinfo.fn_addr = numeric_in;
		flinfo.fn_nargs = 3;
		flinfo.fn_strict = true;
		*pg_datum = InputFunctionCall(&flinfo, dst, 0, atttypmod);
		return 0;
	}

	if (ltyp == XRG_LTYP_STRING && ptyp == XRG_PTYP_BYTEA) {
		int sz = xrg_bytea_len(data);
		if (flag & XRG_FLAG_NULL) {
			*pg_datum = 0;
		} else {
			SET_VARSIZE(data, sz + VARHDRSZ);
			*pg_datum = PointerGetDatum(data);
		}
		return 0;
	}

	return 0;
}
