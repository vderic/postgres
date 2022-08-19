#include "dec.h"
#include "arrow/util/decimal.h"

char *dec128_to_string(__int128_t d128, int scale) {
	arrow::Decimal128 dec((uint8_t*) &d128);
	std::string s = dec.ToString(scale);
	return strdup(s.c_str());
}

char *dec64_to_string(int64_t d64, int scale) {
	arrow::Decimal128 dec(0, d64);
	std::string s = dec.ToString(scale);
	return strdup(s.c_str());
}
