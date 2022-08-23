#include "json.h"
#include "access/tupdesc.h"
#include "catalog/pg_attribute.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "catalog/pg_type_d.h"

const char *xrg_typ_str(int16_t ptyp, int16_t ltyp) {
        switch (ptyp) {
        case XRG_PTYP_INT8:
                return "int8";
        case XRG_PTYP_INT16:
                return "int16";
        case XRG_PTYP_INT32:
                switch (ltyp) {
                case XRG_LTYP_DATE:
                        return "date";
                default:
                        return "int32";
                }
                break;
        case XRG_PTYP_INT64:
                switch (ltyp) {
                case XRG_LTYP_DECIMAL:
                        return "decimal";
                case XRG_LTYP_TIME:
                        return "time";
                case XRG_LTYP_TIMESTAMP:
                        return "timestamp";
                default:
                        return "int64";
                }

                break;
        case XRG_PTYP_INT128:
                switch (ltyp) {
                case XRG_LTYP_INTERVAL:
                        return "interval";
                case XRG_LTYP_DECIMAL:
                        return "decimal";
                default:
                        return "int128";
                }

                break;
        case XRG_PTYP_FP32:
                return "fp32";
        case XRG_PTYP_FP64:
                return "fp64";
        case XRG_PTYP_BYTEA:
                switch (ltyp) {
                case XRG_LTYP_STRING:
                        return "string";
                default:
                        return "bytea";
                }
                break;
        default:
                break;
        }

        return "";
}

void pg_typ_to_xrg_typ(Oid t, int32_t typmod, int16_t &ptyp, int16_t &ltyp, int16_t &precision, int16_t &scale, bool &is_array) 
{
        is_array = false;
        switch (t) {
        case BOOLOID: {
                ptyp = XRG_PTYP_INT8;
                ltyp = XRG_LTYP_NONE;
        }
                return;
        case 1000: // array of BOOLOID
        {
                ptyp = XRG_PTYP_INT8;
                ltyp = XRG_LTYP_NONE;
                is_array = true;
        }
                return;
        case INT2OID: {
                ptyp = XRG_PTYP_INT16;
                ltyp = XRG_LTYP_NONE;
        }
                return;
        case INT2ARRAYOID: {
                ptyp = XRG_PTYP_INT16;
                ltyp = XRG_LTYP_NONE;
                is_array = true;
        }
                return;
        case INT4OID: {
                ptyp = XRG_PTYP_INT32;
                ltyp = XRG_LTYP_NONE;
        }
                return;
        case INT4ARRAYOID: {
                ptyp = XRG_PTYP_INT32;
                ltyp = XRG_LTYP_NONE;
                is_array = true;
        }
                return;
        case INT8OID: {
                ptyp = XRG_PTYP_INT64;
                ltyp = XRG_LTYP_NONE;
        }
                return;
        case INT8ARRAYOID: {
                ptyp = XRG_PTYP_INT64;
                ltyp = XRG_LTYP_NONE;
                is_array = true;
        }
                return;
        case DATEOID: {
                ptyp = XRG_PTYP_INT32;
                ltyp = XRG_LTYP_DATE;
        }
                return;
        case 1182: // array of date
        {
                ptyp = XRG_PTYP_INT32;
                ltyp = XRG_LTYP_DATE;
                is_array = true;
        }
                return;
        case TIMEOID: {
                ptyp = XRG_PTYP_INT64;
                ltyp = XRG_LTYP_TIME;
        }
                return;
        case 1183: // array of time
        {
                ptyp = XRG_PTYP_INT64;
                ltyp = XRG_LTYP_TIME;
                is_array = true;
        }
                return;
        case TIMESTAMPOID: {
                ptyp = XRG_PTYP_INT64;
                ltyp = XRG_LTYP_TIMESTAMP;
        }
                return;
        case 1115: // array of timestamp
        {
                ptyp = XRG_PTYP_INT64;
                ltyp = XRG_LTYP_TIMESTAMP;
                is_array = true;
        }
                return;
        case TIMESTAMPTZOID: {
                ptyp = XRG_PTYP_INT64;
                ltyp = XRG_LTYP_TIMESTAMP;
        }
                return;
        case 1185: // array of timestamptz
        {
                ptyp = XRG_PTYP_INT64;
                ltyp = XRG_LTYP_TIMESTAMP;
                is_array = true;
        }
                return;
        case FLOAT4OID: {
                ptyp = XRG_PTYP_FP32;
                ltyp = XRG_LTYP_NONE;
        }
                return;
        case FLOAT4ARRAYOID: {
                ptyp = XRG_PTYP_FP32;
                ltyp = XRG_LTYP_NONE;
                is_array = true;
        }
                return;
        case FLOAT8OID: {
                ptyp = XRG_PTYP_FP64;
                ltyp = XRG_LTYP_NONE;
        }
                return;
        case FLOAT8ARRAYOID: {
                ptyp = XRG_PTYP_FP64;
                ltyp = XRG_LTYP_NONE;
                is_array = true;
        }
                return;
        case CASHOID: {
                ptyp = XRG_PTYP_INT128;
                ltyp = XRG_LTYP_DECIMAL;
        }
                return;
        // case UUIDOD:
        // case 2951: // Array of UUID
        case INTERVALOID: {
                ptyp = XRG_PTYP_INT128;
                ltyp = XRG_LTYP_INTERVAL;
        }
                return;
        case 1187: // array of interval
        {
                ptyp = XRG_PTYP_INT128;
                ltyp = XRG_LTYP_INTERVAL;
                is_array = true;
        }
                return;
        case NUMERICOID: {
                // Const will have typmod == -1. Set it to INT64 decimal first.
                ptyp = XRG_PTYP_INT64;
                if (typmod >= (int32)VARHDRSZ) {
                        int32_t tmp = typmod - VARHDRSZ;
                        precision = (tmp >> 16) & 0xFFFF;
                        scale = tmp & 0xFFFF;

                        if (precision <= 18) {
                                ptyp = XRG_PTYP_INT64;
                        } else {
                                ptyp = XRG_PTYP_INT128;
                        }
                }
                ltyp = XRG_LTYP_DECIMAL;
        }
                return;
        case 1231: // array of NUMERIC
        {
                ptyp = XRG_PTYP_INT64;
                ltyp = XRG_LTYP_DECIMAL;
                is_array = true;
        }
                return;
        case BYTEAOID: {
                ptyp = XRG_PTYP_BYTEA;
                ltyp = XRG_LTYP_NONE;
        }
                return;
        case BPCHAROID:
        case TEXTOID:
        case VARCHAROID: {
                ptyp = XRG_PTYP_BYTEA;
                ltyp = XRG_LTYP_STRING;
        }
                return;
        case TEXTARRAYOID:
        case 1014: // array of bpchar
        case 1015: // arrayof varchar
        {
                ptyp = XRG_PTYP_BYTEA;
                ltyp = XRG_LTYP_STRING;
                is_array = true;
        }
                return;
        default: {
                ptyp = XRG_PTYP_BYTEA;
                ltyp = XRG_LTYP_STRING;
        }
                return;
        }
}


/* json helper */
static void kite_json_schema_from_pg(rapidjson::Document &doc, TupleDesc tupdesc) {
	auto &alloc = doc.GetAllocator();
	
	int i;

	rapidjson::Value array(rapidjson::kArrayType);

	for (i = 1 ; i <= tupdesc->natts ; i++) {
		int16_t ptyp, ltyp, precision, scale;
		bool is_array = false;
		ptyp = ltyp = precision = scale = 0;

		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		elog(LOG, "SCAN col[%d]: name = %s, typid = %d, typmod = %d", i, NameStr(attr->attname), attr->atttypid, attr->atttypmod);

		pg_typ_to_xrg_typ(attr->atttypid, attr->atttypmod, ptyp, ltyp, precision, scale, is_array);

                elog(LOG, "schema: typ=%d, typmod=%d, ptyp =%d, ltyp=%d, precision =%d, scale=%d, is_array = %d",
                                attr->atttypid, attr->atttypmod, ptyp, ltyp, precision, scale, is_array);

		rapidjson::Value obj(rapidjson::kObjectType);

		// name, type, precision, scale
		rapidjson::Value value(rapidjson::kObjectType);

		// name
		char *colname = NameStr(attr->attname);
		value.SetString(colname, static_cast<rapidjson::SizeType>(strlen(colname)), alloc);
		obj.AddMember("name", value, alloc);


		// type
		const char *type = xrg_typ_str(ptyp, ltyp);
		value.SetString(type, static_cast<rapidjson::SizeType>(strlen(type)), alloc);
		obj.AddMember("type", value, alloc);

		if (strcmp(type, "decimal") == 0) {
			value.SetInt(precision);
			obj.AddMember("precision", value, alloc);
			value.SetInt(scale);
			obj.AddMember("scale", value, alloc);
		}

		array.PushBack(obj, alloc);
	}

	doc.AddMember("schema", array, alloc);

}

static void kite_json_add_sql(rapidjson::Document &doc, char *sql) {
	auto &alloc = doc.GetAllocator();

	rapidjson::Value obj(rapidjson::kObjectType);
	obj.SetString(sql, static_cast<rapidjson::SizeType>(strlen(sql)), alloc);
	doc.AddMember("sql", obj, alloc);
}


static void kite_json_add_fragment(rapidjson::Document &doc, int curr, int nfrag) {
	auto &alloc = doc.GetAllocator();

	rapidjson::Value arr(rapidjson::kArrayType);
	rapidjson::Value fno, fcnt;
	fno.SetInt(curr);
	fcnt.SetInt(nfrag);
	arr.PushBack(fno, alloc);
	arr.PushBack(fcnt, alloc);
	doc.AddMember("fragment", arr, alloc);
}

char *kite_build_json(char *sql, TupleDesc tupdesc, int curfrag, int nfrag) {
	rapidjson::Document doc;
	doc.SetObject();

	kite_json_schema_from_pg(doc, tupdesc);
	kite_json_add_sql(doc, sql);
	kite_json_add_fragment(doc, curfrag, nfrag);

	rapidjson::StringBuffer buffer;
	rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
	doc.Accept(writer);
	const char *json = buffer.GetString();
	return strdup(json);
}

