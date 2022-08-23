#include "json.h"
#include "access/tupdesc.h"
#include "catalog/pg_attribute.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"

/* json helper */
static void kite_json_schema_from_pg(rapidjson::Document &doc, TupleDesc tupdesc) {
	auto &alloc = doc.GetAllocator();
	
	int i;

	rapidjson::Value array(rapidjson::kArrayType);

	for (i = 1 ; i <= tupdesc->natts ; i++) {

		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		elog(LOG, "SCAN col[%d]: name = %s, typid = %d, typmod = %d", i, NameStr(attr->attname), attr->atttypid, attr->atttypmod);

		rapidjson::Value obj(rapidjson::kObjectType);

		// name, type, precision, scale
		rapidjson::Value value(rapidjson::kObjectType);

		// name
		char *colname = NameStr(attr->attname);
		value.SetString(colname, static_cast<rapidjson::SizeType>(strlen(colname)), alloc);
		obj.AddMember("name", value, alloc);


		// type
		char *type = "type";
		value.SetString(type, static_cast<rapidjson::SizeType>(strlen(type)), alloc);
		obj.AddMember("type", value, alloc);

		if (strcmp(type, "decimal") == 0) {
			int precision = 0;
			int scale = 0;
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

