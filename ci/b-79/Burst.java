// B-79: the fresh-topic burst on the Kafka distribution's own client, with kafkakn nowhere on the classpath. It says
// whether the stuck first batch is the Java client's or kafkakn's JVM arm's.
//
// Each round creates a topic of two partitions, then at once sends 2 x 1 000 records of 1 000 bytes, alternating the
// partitions, from <threads> threads, with the client's defaults (idempotence on) and linger.ms=5, as the kafkakn test
// does. With <warm> = 1, one record per partition is sent and acknowledged first.
//
//   java -cp <kafka-clients.jar>:<slf4j-api.jar> Burst.java <bootstrap> <rounds> <threads> <warm>
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class Burst {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        int rounds = Integer.parseInt(args[1]);
        int threads = Integer.parseInt(args[2]);
        boolean warm = args[3].equals("1");
        for (int round = 0; round < rounds; round++) {
            String topic = "kafkakn-burst-java-" + UUID.randomUUID().toString().substring(0, 8);
            try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
                admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get();
            }
            Properties properties = new Properties();
            properties.put("bootstrap.servers", bootstrap);
            properties.put("linger.ms", "5");
            long started = System.nanoTime();
            int landed = 0;
            int failed = 0;
            String first = null;
            try (KafkaProducer<byte[], byte[]> producer =
                    new KafkaProducer<>(properties, new ByteArraySerializer(), new ByteArraySerializer())) {
                if (warm) {
                    for (int partition = 0; partition < 2; partition++) {
                        producer.send(new ProducerRecord<>(topic, partition, null, "warm".getBytes())).get();
                    }
                }
                ExecutorService pool = Executors.newFixedThreadPool(threads);
                List<Future<Future<RecordMetadata>>> sent = new ArrayList<>();
                for (int index = 0; index < 1000; index++) {
                    for (int partition = 0; partition < 2; partition++) {
                        byte[] value = new byte[1000];
                        int p = partition;
                        sent.add(pool.submit(() -> producer.send(new ProducerRecord<>(topic, p, null, value))));
                    }
                }
                for (Future<Future<RecordMetadata>> outer : sent) {
                    try {
                        outer.get().get(150, TimeUnit.SECONDS);
                        landed++;
                    } catch (Exception e) {
                        failed++;
                        if (first == null) first = String.valueOf(e.getCause() != null ? e.getCause() : e);
                    }
                }
                pool.shutdown();
            }
            long ms = (System.nanoTime() - started) / 1_000_000;
            String said = first == null ? "null" : first.substring(0, Math.min(first.length(), 140));
            System.out.printf("threads=%d warm=%s landed=%d failed=%d ms=%d topic=%s first=%s%n",
                threads, warm, landed, failed, ms, topic, said);
        }
    }
}
