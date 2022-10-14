#ifndef _DECODE_H_
#define _DECODE_H_

#include "postgres.h"
#include "xrg.h"

/* decode functions */
int var_decode(xrg_iter_t *iter, int idx, int atttypmod, Datum *pg_datum, bool *pg_isnull);

#endif
