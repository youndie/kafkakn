/* A fixture: the source as it would look on the day upstream rearranges the include block for its
 * own reasons. The problem the patch solves may or may not still be there - what is certain is that
 * the patch cannot be applied as written, and that is a different action from deleting it. */

#include "rd.h"
#include "rdrand.h"
#include "rdtime.h"
#include "tinycthread.h"
#include "rdmurmur2.h"
#ifndef _WIN32
#include <sys/random.h> /* getentropy() */
#endif

#ifdef HAVE_OSSL_SECURE_RAND_BYTES
#include <openssl/rand.h>
#endif

/* Initial seed with time+thread id */
unsigned int rd_seed(void);
