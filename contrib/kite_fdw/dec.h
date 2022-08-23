#ifndef UTIL_H
#define UTIL_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* decimal */
char *dec128_to_string(__int128_t i128, int scale);

char *dec64_to_string(int64_t d64, int scale);

#ifdef __cplusplus
}
#endif

#endif
