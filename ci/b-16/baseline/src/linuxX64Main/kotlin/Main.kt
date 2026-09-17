import kotlinx.coroutines.runBlocking

/**
 * `ci/downstream` with the Kafka taken out: the same `runBlocking`, the same shape, no producer.
 *
 * It has to do *something* — an empty `main` invites the linker to conclude the binary needs nothing
 * — and it has to do the same something the downstream build does, or the difference between the two `ldd`
 * sets has more than one possible cause.
 */
fun main() = runBlocking {
    println("kafkakn ldd baseline")
}
