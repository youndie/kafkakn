import io.github.youndie.kafkakn.ProducerConfig
import io.github.youndie.kafkakn.ProducerRecord
import io.github.youndie.kafkakn.kafkaProducer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * A steady stream of records with **unique values**, so that a duplicate on the topic is a record
 * that was written twice rather than two records that happened to be equal.
 *
 * The request timeout is short on purpose and it is per arm (`faultConfig`): the fixture freezes the
 * broker for longer than it, so requests already appended but not yet acknowledged time out on the
 * client and are retried. Without idempotence, the retry is appended a second time. That is the whole
 * mechanism under test, and a timeout the pause does not exceed would test nothing.
 *
 *     driver <bootstrap> <topic> <stamp> <seconds> <idempotence: default|true|false>
 */
fun main(args: Array<String>) =
    runBlocking {
        val (bootstrap, topic, stamp) = args
        val seconds = args[3].toInt()
        val idempotence = args[4]

        val producer =
            kafkaProducer(
                ProducerConfig(
                    buildMap {
                        put("bootstrap.servers", bootstrap)
                        put("acks", "all")
                        // Long enough that every retry after a pause still has time to land: a record
                        // that times out locally is a refusal, not a duplicate, and would hide one.
                        put("delivery.timeout.ms", "120000")
                        putAll(faultConfig())
                        // `default` leaves the key out altogether - the row this item is about.
                        if (idempotence != "default") put("enable.idempotence", idempotence)
                    },
                ),
            )

        val until = TimeSource.Monotonic.markNow() + seconds.seconds
        // No shared counter: worker `w` owns the indices w, w+50, w+100, ... so every value is unique
        // by construction, and each worker counts its own. There is no atomic integer in common code
        // on this Kotlin, and a lock would serialise exactly the concurrency the fault needs.
        val counts =
            withContext(Dispatchers.Default) {
                (0 until CONCURRENCY)
                    .map { worker ->
                        async {
                            var sent = 0
                            var refused = 0
                            var index = worker
                            while (until.hasNotPassedNow()) {
                                try {
                                    producer.send(ProducerRecord(topic, "$stamp:$index".encodeToByteArray()))
                                    sent++
                                } catch (cancelled: CancellationException) {
                                    throw cancelled
                                } catch (failure: Exception) {
                                    // Counted, never hidden: a refused send is the contract's legitimate
                                    // outcome and it is not a duplicate - but a run full of them would
                                    // be a run that measured the timeout instead of the retry.
                                    refused++
                                }
                                index += CONCURRENCY
                            }
                            sent to refused
                        }
                    }.awaitAll()
            }
        producer.close()
        println(
            "DRIVER handed-in=${counts.sumOf { it.first + it.second }} " +
                "refused=${counts.sumOf { it.second }} idempotence=$idempotence",
        )
    }

private const val CONCURRENCY = 50

/** The client-side request timeout, which the two clients spell differently. */
expect fun faultConfig(): Map<String, String>
