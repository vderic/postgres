#ifndef JSON_H
#define JSON_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "access/tupdesc.h"
#include "sockstream.h"
#include "xrg.h"

/* json helper */
char *kite_build_json(char *sql, TupleDesc tupdesc, int curfrag, int nfrag);


#ifdef __cplusplus
}
#endif

#endif
