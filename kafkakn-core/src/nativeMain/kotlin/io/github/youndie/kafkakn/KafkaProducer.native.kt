package io.github.youndie.kafkakn

/**
 * The native arm. It will wrap librdkafka through cinterop, and is correct when it agrees with the
 * JVM arm against the same broker.
 */
public actual fun kafkaProducer(config: ProducerConfig): KafkaProducer = UnimplementedProducer
