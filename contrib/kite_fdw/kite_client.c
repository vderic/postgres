#include "kite_client.h"

sockstream_t *kite_connect(char *host, int port) {

	return 0;
}

kite_result_t *kite_get_result(sockstream_t *conn, int ncol, char *json) {


	return 0;
}

int kite_result_get_next(kite_result_t *res, int ncol, Datum *datum) {

	return 0;
}

int kite_result_eos(kite_result_t) {

	return 0;
}

void kite_result_destroy(kite_result_t *res) {

}

