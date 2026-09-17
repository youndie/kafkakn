package io.github.youndie.kafkakn

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The suite proving itself before it has anything to prove.
 *
 * `commonTest` is where every producer assertion will live, and the whole argument of this project
 * is that those assertions run on **both** arms against one broker (research §1.1). That claim is
 * worth checking now, while there is no implementation to confuse it with: if this file does not
 * execute twice, nothing written later does either, and the differential oracle is a diagram rather
 * than a mechanism.
 *
 * It is verified by reading the two result files, not by watching a build print BUILD SUCCESSFUL.
 */
class HarnessTest {

    @Test
    fun the_common_suite_runs_on_this_target() {
        assertEquals(4, 2 + 2)
    }
}
