#include "dec.h"
#include "arrow/util/decimal.h"

void dec128_to_string(__int128_t d128, int scale, char *ret) {
	arrow::Decimal128 dec((uint8_t*) &d128);
	std::string s = dec.ToString(scale);
	strcpy(ret, s.c_str());
}

void dec64_to_string(int64_t d64, int scale, char *ret) {
	arrow::Decimal128 dec(0, d64);
	std::string s = dec.ToString(scale);
	strcpy(ret, s.c_str());
}
