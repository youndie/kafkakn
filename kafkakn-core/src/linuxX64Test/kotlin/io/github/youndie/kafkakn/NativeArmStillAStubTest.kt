package io.github.youndie.kafkakn

import kotlin.test.Test
import kotlin.test.assertSame

/**
 * The guard on a temporary exclusion.
 *
 * `ProduceTest` lives in `commonTest` because that is where it belongs, and the native arm is
 * excluded from it in `build.gradle.kts` because the native producer does not exist yet
 * ([B-07](../../../../../../../docs/backlog/B-07-native-actual.md)). An exclusion with only a
 * comment behind it is how a suite quietly stops testing an arm.
 *
 * So this asserts the exclusion is still **warranted**. The moment B-07 makes the native factory
 * return something real, this test fails — and the only way to make it pass again is to delete it
 * together with the filter it guards.
 */
class NativeArmStillAStubTest {

    @Test
    fun the_native_producer_is_still_the_stub_so_the_exclusion_is_warranted() {
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to "127.0.0.1:9092"))
        assertSame(
            UnimplementedProducer,
            producer,
            "the native arm is implemented - delete this test AND the ProduceTest filter in build.gradle.kts",
        )
    }
}
