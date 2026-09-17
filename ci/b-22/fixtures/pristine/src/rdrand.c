/* A fixture, not librdkafka. The include block below is verbatim from librdkafka 2.13.0's
 * src/rdrand.c; everything else in that file is 400 lines the patch does not touch. */

#include "rd.h"
#include "rdrand.h"
#include "rdtime.h"
#include "tinycthread.h"
#include "rdmurmur2.h"
#ifndef _WIN32
/* getentropy() can be present in one of these two */
#include <unistd.h>
#include <sys/random.h>
#endif

#ifdef HAVE_OSSL_SECURE_RAND_BYTES
#include <openssl/rand.h>
#endif

/* Initial seed with time+thread id */
unsigned int rd_seed(void);
