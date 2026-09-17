/* A fixture: the source as it would look on the day upstream guards the include itself. The patch
 * in ci/librdkafka/patches/ then has nothing left to do, and saying so is the point of B-22. */

#include "rd.h"
#include "rdrand.h"
#include "rdtime.h"
#include "tinycthread.h"
#include "rdmurmur2.h"
#ifndef _WIN32
/* getentropy() can be present in one of these two */
#include <unistd.h>
#if defined(HAVE_GETENTROPY)
#include <sys/random.h>
#endif
#endif

#ifdef HAVE_OSSL_SECURE_RAND_BYTES
#include <openssl/rand.h>
#endif

/* Initial seed with time+thread id */
unsigned int rd_seed(void);
