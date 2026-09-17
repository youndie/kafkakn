import io.github.youndie.kafkakn.ProducerConfig
import io.github.youndie.kafkakn.ProducerRecord
import io.github.youndie.kafkakn.RecordHeader
import io.github.youndie.kafkakn.kafkaProducer
import kotlinx.coroutines.runBlocking

/**
 * The smallest thing that is honestly a downstream build: it produces, with headers, and prints where each
 * record landed.
 *
 * It exists to fail. Nothing here is a product, and nothing found by running it is fixed here — a
 * finding goes back into kafkakn's backlog, which is the whole point of having a caller that is not
 * the library's own suite.
 */
fun main(args: Array<String>) = runBlocking {
    val bootstrap = args.getOrElse(0) { "127.0.0.1:9092" }
    val topic = args.getOrElse(1) { "kafkakn" }
    val stamp = args.getOrElse(2) { "downstream" }
    val count = args.getOrElse(3) { "50" }.toInt()

    val producer = kafkaProducer(
        ProducerConfig(
            "bootstrap.servers" to bootstrap,
            "acks" to "all",
        ),
    )
    try {
        var lastPartition = -1
        repeat(count) { index ->
            val metadata = producer.send(
                ProducerRecord(
                    topic = topic,
                    value = "$stamp:$index".encodeToByteArray(),
                    key = "k$index".encodeToByteArray(),
                    headers = listOf(RecordHeader("from", stamp.encodeToByteArray())),
                ),
            )
            lastPartition = metadata.partition
        }
        producer.flush()
        println("sent $count records as $stamp, last partition $lastPartition")
    } finally {
        producer.close()
    }
}
