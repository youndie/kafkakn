import io.github.youndie.kafkakn.ProducerConfig
import io.github.youndie.kafkakn.ProducerRecord
import io.github.youndie.kafkakn.kafkaProducer

/**
 * Compiled, not run.
 *
 * Resolution alone would be satisfied by an empty artefact; this names the `expect` factory and the
 * two types a caller needs, so the metadata module has to carry a usable common surface and each
 * platform has to supply its `actual`. A record is built rather than sent - a broker is B-13's
 * business.
 */
@Suppress("unused")
suspend fun probe(bootstrap: String) {
    val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap))
    producer.send(ProducerRecord("probe", "hello".encodeToByteArray()))
    producer.close()
}
