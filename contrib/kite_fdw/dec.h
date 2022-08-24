#ifndef UTIL_H
#define UTIL_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_DEC128_PRECISION 38

/* decimal */
void dec128_to_string(__int128_t i128, int scale, char *ret);

void dec64_to_string(int64_t d64, int scale, char *ret);

#ifdef __cplusplus
}
#endif

#endif
