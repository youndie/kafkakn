package io.github.youndie.kafkakn

/**
 * The JVM arm — and therefore the oracle. It will delegate to `org.apache.kafka:kafka-clients`,
 * which is the reference implementation; every line of our own logic here is a line the oracle no
 * longer vouches for.
 */
public actual fun kafkaProducer(config: ProducerConfig): KafkaProducer = UnimplementedProducer
